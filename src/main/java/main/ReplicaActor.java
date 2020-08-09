package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import main.Messages.*;
import scala.concurrent.duration.Duration;

/**
 *
 * @author alex
 */
public class ReplicaActor extends AbstractActor {
    public final int replicaID;
    private int value;
    private final UpdateList unstable_updates, delivered_updates;
    private final ElectionManager election_manager;
    
    private final TimeoutMap<Integer> update_req_timers;
    private final TimeoutMap<UpdateID> writeok_timers;

    private int epoch, seqNo;
    public final Map<Integer, ActorRef> nodes_by_id;
    public final List<Integer> crashed_nodes;
    private final TimeoutList heartbeat_timer;
    
    // Only used if replica is coordinator
    private final Map<UpdateID, Integer> updateAcks;
    
    
    public ReplicaActor(int ID, int value) {
        this.replicaID = ID;
        this.value = value;
        this.election_manager = new ElectionManager(this, this::onNewCoordinator);
        
        this.nodes_by_id = new HashMap<>();
        this.crashed_nodes = new ArrayList<>();
        this.heartbeat_timer = new TimeoutList(this::onHeartbeatTimeout, QTOB.HEARTBEAT_TIMEOUT_MS);
        this.unstable_updates = new UpdateList();
        this.delivered_updates = new UpdateList();
        this.update_req_timers = new TimeoutMap<>(this::onUpdateRequestTimeout, QTOB.NWK_TIMEOUT_MS);
        this.writeok_timers = new TimeoutMap<>(this::onWriteOkTimeout, QTOB.NWK_TIMEOUT_MS);

        this.epoch = -1;  // on the first election this will change to 0
        this.seqNo = 0;
        
        this.updateAcks = new HashMap<>();
    }

    static public Props props(int ID, int value) {
        return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }
    
    public void setStateToCrashed() {
        log("Setting state to CRASHED");
        getContext().become(crashed());
        heartbeat_timer.cancelAll();
        update_req_timers.cancelAll();
        writeok_timers.cancelAll();
        election_manager.election_ack_timers.cancelAll();
        election_manager.election_timers.cancelAll();
        crashed_nodes.add(replicaID);
    }
    
    public void onCoordinatorCrash() {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_COORD_CRASH)) {
            setStateToCrashed();
            return;
        }
        
        crashed_nodes.add(election_manager.coordinatorID);
        election_manager.beginElection();
    }
    
    public void onNewCoordinator(UpdateList updates) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_NEW_COORD)) {
            setStateToCrashed();
            return;
        }
        
        if (QTOB.VERBOSE) log("Coordinator => " + election_manager.coordinatorID);
	
        for (Update u : updates)
            if (!delivered_updates.contains(u))
                applyWrite(u);
        
        epoch++;
        if (election_manager.coordinatorID == replicaID) {
            seqNo = 0;
            scheduleNextHeartbeatReminder();
        }
    }
    
    public void log(String msg) {
        if (election_manager.coordinatorID != null && election_manager.coordinatorID == replicaID)
            System.out.println("[R" + replicaID + "*] " + msg);
        else
            System.out.println("[R" + replicaID + "] " + msg);
    }
    
    private void scheduleNextHeartbeatReminder() {
        getContext().system().scheduler().scheduleOnce(
            Duration.create(QTOB.HEARTBEAT_DELAY_MS, TimeUnit.MILLISECONDS),
            getSelf(),                            // To whom
            new HeartbeatReminder(),              // Msg to send
            getContext().system().dispatcher(),   // System dispatcher
            getSelf()                             // Sender
        );
    }
    
    public int getNextIDInRing() {
        if (nodes_by_id.isEmpty()) return replicaID;
        
        int id = replicaID;
        do id = ++id % nodes_by_id.size();
        while (crashed_nodes.contains(id));
        
        return id;
    }
    
    public ActorRef getNextActorInRing() {
        return nodes_by_id.get(getNextIDInRing());
    }
    
    public Update getMostRecentUpdate() {
        return delivered_updates.getMostRecent();
    }
    
    public void sendWithNwkDelay(ActorRef to, Object msg) {
        QTOB.simulateNwkDelay();
        to.tell(msg, getSelf());
    }
    
    private void onReadRequest(ReadRequest req) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_READ_REQ)) {
            setStateToCrashed();
            return;
        }
        
        sendWithNwkDelay(req.client, new ReadResponse(this.value));
        
        if (election_manager.coordinatorID == null)
            election_manager.beginElection();
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_WRITE_REQ)) {
            setStateToCrashed();
            return;
        }
        
        if (election_manager.electing || election_manager.coordinatorID == null) {
            election_manager.beginElection();
            if (QTOB.VERBOSE) log("Election in progress, dropping write request");
            return;
        }
        
        if (election_manager.coordinatorID == this.replicaID) {
            propagateUpdate(req.new_value);
        } else {
            // Forward to coordinator
            sendWithNwkDelay(
                nodes_by_id.get(election_manager.coordinatorID),
                req
            );
            this.update_req_timers.addTimer(req.new_value);
        }
    }
    
    private void propagateUpdate(int value) {
        this.seqNo++;
        UpdateID u_id = new UpdateID(epoch, seqNo);  // be careful with ++
        Update u = new Update(u_id, value);

        this.updateAcks.put(u_id, 1);// Count myself

        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_UPDATE_MSG_SND)) {
            setStateToCrashed();
            return;
        }
        
        for (int id : nodes_by_id.keySet())
            if (id != replicaID && !crashed_nodes.contains(id))
                sendWithNwkDelay(nodes_by_id.get(id), new UpdateMsg(u));
    }
    
    private void onUpdateMsg(UpdateMsg msg) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_UPDATE_MSG_RCV)) {
            setStateToCrashed();
            return;
        }
        
        if (update_req_timers.containsKey(msg.u.value)) {
            update_req_timers.cancelTimer(msg.u.value);
            writeok_timers.addTimer(msg.u.id);
        }
        
        
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_UPDATE_ACK_SND)) {
            setStateToCrashed();
            return;
        }
        this.unstable_updates.add(msg.u);
        sendWithNwkDelay(getSender(), new UpdateAck(msg.u));
    }
    
    private void onUpdateAck(UpdateAck msg) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_UPDATE_ACK_RCV)) {
            setStateToCrashed();
            return;
        }
        
        if (election_manager.coordinatorID == null)
            return;
        
        if (election_manager.coordinatorID != this.replicaID) {
            System.err.println("!!! Received UpdateAck even if not coordinator");
            return;
        }
        
        // Wait for Q acks and propagate writeok to everyone
        int curr_acks = incrementUpdateAcks(msg.u.id);
        int Q = Math.floorDiv(nodes_by_id.size(), 2) + 1;
        
        if (curr_acks == Q) {
            for (int id : nodes_by_id.keySet())
                if (!crashed_nodes.contains(id))
                    sendWithNwkDelay(nodes_by_id.get(id), new WriteOk(msg.u));
        }
    }
    
    private int incrementUpdateAcks(UpdateID id) {
        int n_acks = this.updateAcks.get(id) + 1;
        this.updateAcks.replace(id, n_acks);
        return n_acks;
    }
    
    private void onWriteOk(WriteOk msg) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_WRITE_OK)) {
            setStateToCrashed();
            return;
        }
        
        if (writeok_timers.containsKey(msg.u.id))
            writeok_timers.cancelTimer(msg.u.id);
        
        applyWrite(msg.u);
    }
    
    private void applyWrite(Update u) {
        this.unstable_updates.remove(u.id);
        this.delivered_updates.add(u);
        this.value = u.value;
        this.epoch = u.id.epoch; // TODO: not really, check
        this.seqNo = u.id.seqNo;
        
        System.out.println("Replica " + replicaID + " update " + epoch + ":" + seqNo + " " + this.value);    
    }
    
    private void onWriteOkTimeout(UpdateID node) {
        if (QTOB.VERBOSE) log("Wait for WriteOk timed out");
        onCoordinatorCrash();
    }

    private void onCrashMsg(CrashMsg msg) {
        setStateToCrashed();
    }
    
    private void onHeartbeatReminder(HeartbeatReminder msg) {
        if (election_manager.electing || election_manager.coordinatorID == null || election_manager.coordinatorID != this.replicaID)
            return;
        
        for (int id : this.nodes_by_id.keySet())
            if (id != replicaID && !crashed_nodes.contains(id))
                sendWithNwkDelay(nodes_by_id.get(id), new Heartbeat());
        
        scheduleNextHeartbeatReminder();
    }
    
    private void onUpdateRequestTimeout(Integer node) {
        if (QTOB.VERBOSE) log("Update request timed out");
        onCoordinatorCrash();
    }
    
    private void onHeartbeat(Heartbeat msg) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_HEARTBEAT)) {
            setStateToCrashed();
            return;
        }
        
        try { heartbeat_timer.cancelFirstTimer(); }
        catch (Exception e) { } // First heartbeat
        heartbeat_timer.addTimer();
    }
    
    private void onHeartbeatTimeout() {
        if (QTOB.VERBOSE) log("Heartbeat timeout");
        onCoordinatorCrash();
    }
    
    private void onInitializeGroup(InitializeGroup msg) {
        if (CrashHandler.getInstance().shouldCrash(replicaID, CrashHandler.Situation.ON_INIT)) {
            setStateToCrashed();
            return;
        }
        
        for (int i = 0; i < msg.group.size(); i++)
            this.nodes_by_id.put(i, msg.group.get(i));
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeGroup.class, this::onInitializeGroup)
            .match(Election.class, election_manager::onElection)
            .match(ElectionAck.class, election_manager::onElectionAck)
            .match(Synchronize.class, election_manager::onSynchronize)
            .match(ReadRequest.class, this::onReadRequest)
            .match(WriteRequest.class, this::onWriteRequest)
            .match(UpdateMsg.class, this::onUpdateMsg)
            .match(UpdateAck.class, this::onUpdateAck)
            .match(WriteOk.class, this::onWriteOk)
            .match(CrashMsg.class, this::onCrashMsg)
            .match(HeartbeatReminder.class, this::onHeartbeatReminder)
            .match(Heartbeat.class, this::onHeartbeat)
            .matchAny(msg -> {System.err.println("Replica " + replicaID + " unhandled " + msg.getClass().getSimpleName());})
            .build();
    }
    
    final AbstractActor.Receive crashed() {
        return receiveBuilder()
            .matchAny(msg -> {
                if (QTOB.VERBOSE)
                    ; // log("Crashed but got: " + msg.getClass().getSimpleName());
            })
            .build();
    }
}

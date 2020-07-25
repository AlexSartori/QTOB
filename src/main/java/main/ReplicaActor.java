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
    private final Map<UpdateID, Integer> updateHistory;
    private final ElectionManager election_manager;
    
    private final TimeoutMap<Integer> update_req_timers;
    private final TimeoutMap<UpdateID> writeok_timers;

    private int epoch, seqNo;
    private final Map<Integer, ActorRef> nodes_by_id;
    private final List<Integer> crashed_nodes;
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
        this.updateHistory = new HashMap<>(); // to be changed, maybe
        this.update_req_timers = new TimeoutMap<>(this::onUpdateRequestTimeout, QTOB.NWK_TIMEOUT_MS);
        this.writeok_timers = new TimeoutMap<>(this::onWriteOkTimeout, QTOB.NWK_TIMEOUT_MS);

        this.epoch = -1;  // on the first view change this will change to 0
        this.seqNo = 0;
        
        this.updateAcks = new HashMap<>();
    }

    static public Props props(int ID, int value) {
        return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }
    
    private void setStateToCrashed() {
        getContext().become(crashed());
        heartbeat_timer.cancelAll();
        update_req_timers.cancelAll();
        writeok_timers.cancelAll();
    }
    
    /* public void onCrashedNode(ActorRef node) {
        if (nodes_by_id.get(election_manager.coordinatorID) == node)
            election_manager.coordinatorID = null;
        
        List<ActorRef> new_peers = new ArrayList<>(this.alive_peers);
        new_peers.remove(node);
        // createAndPropagateView(new_peers);
    } */
    
    public void onNewCoordinator() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " coordinator => " + election_manager.coordinatorID);
	
        epoch++;
        if (election_manager.coordinatorID == replicaID) {
            seqNo = 0;
            scheduleNextHeartbeatReminder();
        }
    }
    
    public int getNextIDInRing() {
        int id = replicaID;
        
        do id = ++id % nodes_by_id.size();
        while (crashed_nodes.contains(id));
        
        return id;
    }
    
    public ActorRef getNextActorInRing() {
        return nodes_by_id.get(getNextIDInRing());
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
    
    private void sendWithNwkDelay(ActorRef to, Object msg) {
        QTOB.simulateNwkDelay();
        to.tell(msg, getSelf());
    }
    
    private void onReadRequest(ReadRequest req) {
        // System.out.println("Replica " + replicaID + " read request");
        sendWithNwkDelay(req.client, new ReadResponse(this.value));
        
        if (election_manager.coordinatorID == null)
            election_manager.beginElection();
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (election_manager.coordinatorID == null) {
            // TODO: enqueue requests during elections
            election_manager.beginElection();
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

        this.updateAcks.put(u_id, 0);

        for (ActorRef a : this.nodes_by_id.values())
            if (a != getSelf())
                sendWithNwkDelay(a, new UpdateMsg(u));
    }
    
    private void onUpdateMsg(UpdateMsg msg) {
        if (update_req_timers.containsKey(msg.u.value)) {
            update_req_timers.cancelTimer(msg.u.value);
            writeok_timers.addTimer(msg.u.id);
        }
        
        sendWithNwkDelay(getSender(), new UpdateAck(msg.u));
    }
    
    private void onUpdateAck(UpdateAck msg) {
        if (election_manager.coordinatorID != this.replicaID) {
            System.err.println("!!! Received UpdateAck even if not coordinator");
            return;
        }
        
        // Wait for Q acks and propagate writeok to everyone
        int curr_acks = incrementUpdateAcks(msg.u.id);
        int Q = Math.floorDiv(nodes_by_id.size() - crashed_nodes.size(), 2) + 1;
        
        if (curr_acks == Q) {
            for (ActorRef r : nodes_by_id.values())
                sendWithNwkDelay(r, new WriteOk(msg.u));
        }
    }
    
    private int incrementUpdateAcks(UpdateID id) {
        int n_acks = this.updateAcks.get(id) + 1;
        this.updateAcks.replace(id, n_acks);
        return n_acks;
    }
    
    private void onWriteOk(WriteOk msg) {
        if (writeok_timers.containsKey(msg.u.id))
            writeok_timers.cancelTimer(msg.u.id);
        
        applyWrite(msg.u);
        System.out.println("Replica " + replicaID + " update " + epoch + ":" + seqNo +" " + this.value);    
    }
    
    private void applyWrite(Update u) {
        this.updateHistory.put(u.id, u.value);
        this.value = u.value;
        this.epoch = u.id.epoch; // TODO: not really, check
        this.seqNo = u.id.seqNo;
    }
    
    private void onWriteOkTimeout(UpdateID node) {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " wait for WriteOk timed out");
        // onCrashedNode(nodes_by_id.get(election_manager.coordinatorID));
    }

    private void onCrashMsg(CrashMsg msg) {
        System.out.println("Replica " + replicaID + " setting state to CRASHED");
        setStateToCrashed();
    }
    
    private void onHeartbeatReminder(HeartbeatReminder msg) {
        if (election_manager.electing || election_manager.coordinatorID == null || election_manager.coordinatorID != this.replicaID)
            return;
        
        for (ActorRef a : this.nodes_by_id.values())
            if (a != getSelf()) {
                // if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " sending HB to " + a.path());
                QTOB.simulateNwkDelay();
                sendWithNwkDelay(a, new Heartbeat());
            }
        
        scheduleNextHeartbeatReminder();
    }
    
    private void onUpdateRequestTimeout(Integer node) {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " update request timed out");
        // onCrashedNode(nodes_by_id.get(election_manager.coordinatorID));
    }
    
    private void onHeartbeat(Heartbeat msg) {
        // if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " HB received");
        try { heartbeat_timer.cancelFirstTimer(); }
        catch (Exception e) { } // First heartbeat
        heartbeat_timer.addTimer();
    }
    
    private void onHeartbeatTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " Heartbeat timeout");
        // onCrashedNode(nodes_by_id.get(election_manager.coordinatorID));
    }
    
    private void onInitializeGroup(InitializeGroup msg) {
        for (int i = 0; i < msg.group.size(); i++)
            this.nodes_by_id.put(i, msg.group.get(i));
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeGroup.class, this::onInitializeGroup)
            .match(Election.class, election_manager::onElection)
            .match(ElectionAck.class, election_manager::onElectionAck)
            .match(Coordinator.class, election_manager::onCoordinator)
            .match(ReadRequest.class, this::onReadRequest)
            .match(WriteRequest.class, this::onWriteRequest)
            .match(UpdateMsg.class, this::onUpdateMsg)
            .match(UpdateAck.class, this::onUpdateAck)
            .match(WriteOk.class, this::onWriteOk)
            .match(CrashMsg.class, this::onCrashMsg)
            .match(HeartbeatReminder.class, this::onHeartbeatReminder)
            .match(Heartbeat.class, this::onHeartbeat)
            .matchAny(msg -> {System.out.println("Replica " + replicaID + " unhandled " + msg.getClass());})
            .build();
    }
    
    final AbstractActor.Receive crashed() {
        return receiveBuilder()
            .matchAny(msg -> {
                if (QTOB.VERBOSE)
                    System.out.println("Replica " + replicaID + " crashed but " + msg.getClass());
            })
            .build();
    }
}

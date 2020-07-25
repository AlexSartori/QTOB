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
    private boolean view_change;
    
    public final int replicaID;
    private int value;
    private final Map<UpdateID, Integer> updateHistory;
    private final ElectionManager election_manager;
    
    // View & Epoch management
    private final Map<Integer, View> views;
    private final Map<Integer, HashMap<ActorRef, Boolean>> flushes_received;
    private final TimeoutMap<Integer> update_req_timers;
    private final TimeoutMap<UpdateID> writeok_timers;

    private int epoch, seqNo;
    private final Map<Integer, ActorRef> nodes_by_id;
    private List<ActorRef> alive_peers;
    private final TimeoutList heartbeat_timer;
    private final TimeoutMap<ActorRef> heartbeat_ack_timers;
    
    // Only used if replica is coordinator
    private final Map<UpdateID, Integer> updateAcks;
    
    
    public ReplicaActor(int ID, int value) {
        this.view_change = false;
        this.replicaID = ID;
        this.value = value;
        this.election_manager = new ElectionManager(this, this::onNewCoordinator);
        
        this.nodes_by_id = new HashMap<>();
        this.alive_peers = new ArrayList<>();
        this.heartbeat_timer = new TimeoutList(this::onHeartbeatTimeout, QTOB.HEARTBEAT_TIMEOUT_MS);
        this.heartbeat_ack_timers = new TimeoutMap<>(this::onHeartbeatAckTimeout, QTOB.NWK_TIMEOUT_MS);
        this.updateHistory = new HashMap<>(); // to be changed, maybe
        this.views = new HashMap<>();
        this.flushes_received = new HashMap<>();
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
        heartbeat_ack_timers.cancelAll();
        update_req_timers.cancelAll();
        writeok_timers.cancelAll();
    }
    
    public void onCrashedNode(ActorRef node) {
        if (nodes_by_id.get(election_manager.coordinatorID) == node)
            election_manager.coordinatorID = null;
        
        List<ActorRef> new_peers = new ArrayList<>(this.alive_peers);
        new_peers.remove(node);
        createAndPropagateView(new_peers);
    }
    
    public void onNewCoordinator() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " coordinator => " + election_manager.coordinatorID);
	
        if (election_manager.coordinatorID == replicaID) {
            seqNo = 0;
            scheduleNextHeartbeatReminder();
        }
    }
    
    public int getNextIDInRing() {
        int id = replicaID;
        
        do id = ++id % nodes_by_id.size();
        while (!alive_peers.contains(nodes_by_id.get(id)));
        
        return id;
    }
    
    public ActorRef getNextActorInRing() {
        if (alive_peers.isEmpty())
            return getSelf();
        int idx = alive_peers.indexOf(getSelf());
        idx = (idx+1) % alive_peers.size();
        return this.alive_peers.get(idx);
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
    
    public void createAndPropagateView(List<ActorRef> new_group) {
        if(view_change) return;
        this.view_change = true;
        this.epoch++;
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " creating new View #" + epoch);
        
        View v = new View(this.epoch, new_group);
        addNewView(v);
        // TODO (?) Send all unstable messages
        flushViewToAll(v);
        
        for (ActorRef a : new_group) {
            if (a != getSelf()) {
                QTOB.simulateNwkDelay();
                a.tell(new ViewChange(v), getSelf()); 
            }
        }
    }
    
    private void addNewView(View v) {
        this.views.put(v.viewID, v);
        if (!flushes_received.containsKey(v.viewID))
            flushes_received.put(v.viewID, new HashMap<>());
    }
    
    private void onViewChange(ViewChange msg) {
        if (this.views.containsKey(msg.view.viewID))
            return;
        
        this.view_change = true; // Pause sending new multicasts
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " received ViewChange #" + msg.view.viewID);
        
        if (nodes_by_id.isEmpty())
            for (int i = 0; i < msg.view.peers.size(); i++)
                nodes_by_id.put(i, msg.view.peers.get(i));
        
        addNewView(msg.view);
        // TODO (?) Send all unstable messages
        flushViewToAll(msg.view);
    }
    
    private void flushViewToAll(View v) {
        for (ActorRef r : v.peers)
            if (r != getSelf()) {
                QTOB.simulateNwkDelay();
                r.tell(new Flush(v.viewID), getSelf());   
            }
    }
    
    private void onFlush(Flush msg) {
        addFlushAck(msg.id, getSender());
        
        if (isFlushComplete(msg.id))
            installView(msg.id);
    }
    
    private void addFlushAck(int id, ActorRef sender) {
        if (!flushes_received.containsKey(id))
            flushes_received.put(id, new HashMap<>());
        
        HashMap<ActorRef, Boolean> this_view = flushes_received.get(id);
        this_view.put(sender, true);
    }
    
    private boolean isFlushComplete(int id) {
        if (!views.containsKey(id)) return false;
        int n_peers = views.get(id).peers.size();
        int n_flushes = flushes_received.get(id).size();
        return n_flushes == (n_peers - 1);
    }
    
    private void installView(int id) {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " installing View #" + id);
        
        this.alive_peers = this.views.get(id).peers;
        this.epoch = id;
        this.view_change = false;
        
        if (election_manager.coordinatorID != null) {
            ActorRef coord = nodes_by_id.get(election_manager.coordinatorID);
            if (!alive_peers.contains(coord))
                election_manager.coordinatorID = null;
        }
    }
    
    private void onReadRequest(ReadRequest req) {
        System.out.println("Replica " + replicaID + " read request");
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
        
        if (election_manager.coordinatorID == null)
            election_manager.beginElection();
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (election_manager.coordinatorID == null) {
            // TODO: enqueue requests during elections
            election_manager.beginElection();
            return;
        }
        
        if (this.view_change) {
            // TODO: enqueue requests during view changes
            return;
        }
        
        if (election_manager.coordinatorID == this.replicaID) {
            propagateUpdate(req.new_value);
        } else {
            // Forward to coordinator
            this.nodes_by_id.get(election_manager.coordinatorID).tell(
                req,
                getSelf()
            );
            this.update_req_timers.addTimer(req.new_value);
        }
    }
    
    private void propagateUpdate(int value) {
        this.seqNo++;
        UpdateID u_id = new UpdateID(epoch, seqNo);  // be careful with ++
        Update u = new Update(u_id, value);

        this.updateAcks.put(u_id, 0);

        for (ActorRef a : this.alive_peers)
            if (a != getSelf())
                a.tell(new UpdateMsg(u), getSelf());
    }
    
    private void onUpdateMsg(UpdateMsg msg) {
        if (update_req_timers.containsKey(msg.u.value)) {
            update_req_timers.cancelTimer(msg.u.value);
            writeok_timers.addTimer(msg.u.id);
        }
        
        getSender().tell(
            new UpdateAck(msg.u),
            getSelf()
        );
    }
    
    private void onUpdateAck(UpdateAck msg) {
        if (election_manager.coordinatorID != this.replicaID) {
            System.err.println("!!! Received UpdateAck even if not coordinator");
            return;
        }
        
        // Wait for Q acks and propagate writeok to everyone
        int curr_acks = incrementUpdateAcks(msg.u.id);
        int Q = Math.floorDiv(alive_peers.size(), 2) + 1;
        
        if (curr_acks == Q) {
            for (ActorRef r : alive_peers)
                r.tell(
                    new WriteOk(msg.u),
                    getSelf()
                );
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
        System.out.println("Replica <" + replicaID + "> update <" + epoch + ">:<" + seqNo +"> <" + this.value + ">");    
    }
    
    private void applyWrite(Update u) {
        this.updateHistory.put(u.id, u.value);
        this.value = u.value;
        this.epoch = u.id.epoch; // TODO: not really, check
        this.seqNo = u.id.seqNo;
    }
    
    private void onWriteOkTimeout(UpdateID node) {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " wait for WriteOk timed out");
        onCrashedNode(nodes_by_id.get(election_manager.coordinatorID));
    }

    private void onCrashMsg(CrashMsg msg) {
        System.out.println("Replica " + replicaID + " setting state to CRASHED");
        setStateToCrashed();
    }
    
    private void onHeartbeatReminder(HeartbeatReminder msg) {
        if (election_manager.electing || election_manager.coordinatorID != this.replicaID)
            return;
        
        for (ActorRef a : this.alive_peers)
            if (a != getSelf()) {
                // if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " sending HB to " + a.path());
                QTOB.simulateNwkDelay();
                a.tell(new Heartbeat(), getSelf());
                heartbeat_ack_timers.addTimer(a);
            }
        
        scheduleNextHeartbeatReminder();
    }
    
    private void onUpdateRequestTimeout(Integer node) {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " update request timed out");
        onCrashedNode(nodes_by_id.get(election_manager.coordinatorID));
    }
    
    private void onHeartbeat(Heartbeat msg) {
        // if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " HB received");
        try { heartbeat_timer.cancelFirstTimer(); }
        catch (Exception e) { } // First heartbeat
        heartbeat_timer.addTimer();
        
        getSender().tell(new HeartbeatAck(), getSelf());
    }
    
    private void onHeartbeatTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " Heartbeat timeout");
        onCrashedNode(nodes_by_id.get(election_manager.coordinatorID));
    }
    
    private void onHeartbeatAck(HeartbeatAck msg) {
        try { heartbeat_ack_timers.cancelTimer(getSender()); }
        catch (Exception e) { /* Timers were canceled */ }
    }
    
    private void onHeartbeatAckTimeout(ActorRef node) {
        if (view_change || !alive_peers.contains(node)) return; // Old ack
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " HeartbeatAck timeout for " + node);
        onCrashedNode(node);
    }
    
    private void onInitializeGroup(InitializeGroup msg) {
        for (int i = 0; i < msg.group.size(); i++)
            this.nodes_by_id.put(i, msg.group.get(i));
        createAndPropagateView(msg.group);
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeGroup.class, this::onInitializeGroup)
            .match(ViewChange.class, this::onViewChange)
            .match(Flush.class, this::onFlush)
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
            .match(HeartbeatAck.class, this::onHeartbeatAck)
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

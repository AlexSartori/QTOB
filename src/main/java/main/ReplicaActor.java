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
    
    // Only used if replica is coordinator
    private final Map<UpdateID, Integer> updateAcks;
    
    
    public ReplicaActor(int ID, int value) {
        this.view_change = false;
        this.replicaID = ID;
        this.value = value;
        this.election_manager = new ElectionManager(this, this::onNewCoordinator);
        
        this.nodes_by_id = new HashMap<>();
        this.alive_peers = new ArrayList<>();
        this.updateHistory = new HashMap<>(); // to be changed, maybe
        this.views = new HashMap<>();
        this.flushes_received = new HashMap<>();
        this.update_req_timers = new TimeoutMap<>(this::onUpdateRequestTimeout, QTOB.NWK_TIMEOUT_MS);
        this.writeok_timers = new TimeoutMap<>(this::onWriteOkTimeout, QTOB.NWK_TIMEOUT_MS);

        
        this.epoch = -1;  // on the first election this will change to 0
        this.seqNo = 0;
        
        this.updateAcks = new HashMap<>();
    }

    static public Props props(int ID, int value) {
        return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }
    
    private void setStateToCrashed() {
        getContext().become(crashed());
    }
    
    public void onCrashedNode(ActorRef node) {
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
        ViewChange msg = new ViewChange(v);
        
        addNewView(v);
        // TODO (?) Send all unstable messages
        flushViewToAll(msg.view);
        
        for (ActorRef a : new_group) {
            if (a != getSelf()) {
                QTOB.simulateNwkDelay();
                a.tell(msg, getSelf()); 
            }
        }
    }
    
    private void addNewView(View v) {
        this.views.put(v.viewID, v);
        if (!flushes_received.containsKey(v.viewID))
            flushes_received.put(v.viewID, new HashMap<>());
    }
    
    private void onViewChange(ViewChange msg) {
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
        Flush msg = new Flush(v.viewID);
        
        for (ActorRef r : v.peers)
            if (r != getSelf()) {
                QTOB.simulateNwkDelay();
                r.tell(msg, getSelf());   
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
        
        if (election_manager.coordinatorID == null)
            election_manager.beginElection();
    }
    
    private void onReadRequest(ReadRequest req) {
        System.out.println("Replica " + replicaID + " read request");
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (election_manager.coordinatorID == null) {
            // TODO: enqueue requests during elections
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
        UpdateID u_id = new UpdateID(epoch, seqNo++);  // be careful with ++
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
    }
    
    private void applyWrite(Update u) {
        this.updateHistory.put(u.id, u.value);
        this.value = u.value;
    }

    private void onCrashMsg(CrashMsg msg) {
        System.out.println("Replica " + replicaID + " setting state to CRASHED");
        setStateToCrashed();
    }
    
    private void onHeartbeatReminder(HeartbeatReminder msg) {
        if (election_manager.coordinatorID == null || election_manager.coordinatorID != this.replicaID)
            return;
        
        for (ActorRef a : this.alive_peers)
            if (a != getSelf())
                a.tell(new Heartbeat(), getSelf());
        
        scheduleNextHeartbeatReminder();
    }
    
    private void onUpdateRequestTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " update request timed out");
        
        // Declare coordinator is dead
        List<ActorRef> new_peers = new ArrayList<>(this.alive_peers);
        new_peers.remove(this.nodes_by_id.get(election_manager.coordinatorID));
        election_manager.coordinatorID = null;
        createAndPropagateView(new_peers);
    }
    
    private void onWriteOkTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " wait for WriteOk timed out");
        onCrashedNode(nodes_by_id.get(election_manager.coordinatorID));
    }
    
    private void onHeartbeat(Heartbeat msg) {
        // TODO: cancel coordinator crash timeout (to be implemented)
        // System.out.println("Replica " + replicaID + " received heartbeat");
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

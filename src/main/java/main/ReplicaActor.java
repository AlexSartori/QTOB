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
    private enum State {
        VIEW_CHANGE,
        ELECTING,
        BROADCAST,
        CRASHED
    };
    private State state;
    
    public final int replicaID;
    private int value;
    private final Map<UpdateID, Integer> updateHistory;
    private final ElectionManager election_manager;
    
    // View & Epoch management
    private final Map<Integer, View> views;
    private final Map<Integer, Integer> flushes_received;
    private final TimeoutMap<Integer> update_req_timers;
    private final TimeoutMap<UpdateID> writeok_timers;

    private int epoch, seqNo;
    private final Map<Integer, ActorRef> nodes_by_id;
    private List<ActorRef> alive_peers;
    
    // Only used if replica is coordinator
    private final Map<UpdateID, Integer> updateAcks;
    
    
    public ReplicaActor(int ID, int value) {
        this.state = State.VIEW_CHANGE;
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
        this.state = State.CRASHED;
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
        this.state = State.VIEW_CHANGE;
        this.epoch++;
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " creating new View #" + epoch);
        
        View v = new View(this.epoch, new_group);
        addNewView(v);
        
        ViewChange msg = new ViewChange(v);
        for (ActorRef a : new_group) {
            QTOB.simulateNwkDelay();
            a.tell(msg, getSelf()); 
        }
    }
    
    private void addNewView(View v) {
        this.views.put(v.viewID, v);
        if (!flushes_received.containsKey(v.viewID))
            flushes_received.put(v.viewID, 0);
    }
    
    private void onViewChange(ViewChange msg) {
        this.state = State.VIEW_CHANGE; // Pause sending new multicasts
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " received ViewChange #" + msg.view.viewID);
        
        for (int i = 0; i < msg.view.peers.size(); i++)
            if (!this.nodes_by_id.containsKey(i))
            this.nodes_by_id.put(i, msg.view.peers.get(i));
        
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
        incrementFlushAks(msg.id);
        
        if (isFlushComplete(msg.id)) {
            installView(msg.id);
        }
    }
    
    private int incrementFlushAks(int id) {
        if (!flushes_received.containsKey(id))
            flushes_received.put(id, 0);
        
        int n_flushes = flushes_received.get(id) + 1;
        flushes_received.replace(id, n_flushes);
        return n_flushes;
    }
    
    private boolean isFlushComplete(int id) {
        if (!views.containsKey(id)) return false;
        int n_peers = views.get(id).peers.size();
        int n_flushes = flushes_received.get(id);
        return n_flushes == n_peers - 1;
    }
    
    private void installView(int id) {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " installing View #" + id);
        this.alive_peers = this.views.get(id).peers;
        this.epoch = id;
        this.state = State.BROADCAST;
    }
    
    private void onReadRequest(ReadRequest req) {
        System.out.println("Replica " + replicaID + " read request");
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
        
        if (election_manager.coordinatorID == null && state != State.ELECTING)
            election_manager.beginElection();
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (election_manager.coordinatorID == null) {
            election_manager.beginElection();
        }
        if (election_manager.coordinatorID == null) {
            // TODO: enqueue requests during elections
            return;
        }
        
        if (this.state == State.VIEW_CHANGE) {
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
        
        List<ActorRef> new_peers = new ArrayList<>(this.alive_peers);
        new_peers.remove(this.nodes_by_id.get(election_manager.coordinatorID));
        election_manager.coordinatorID = null;
        createAndPropagateView(new_peers);
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
            .match(ReadRequest.class, this::onReadRequest)
            .match(WriteRequest.class, this::onWriteRequest)
            .match(UpdateMsg.class, this::onUpdateMsg)
            .match(UpdateAck.class, this::onUpdateAck)
            .match(WriteOk.class, this::onWriteOk)
            .match(Election.class, election_manager::onElection)
            .match(ElectionAck.class, election_manager::onElectionAck)
            .match(Coordinator.class, election_manager::onCoordinator)
            .match(CrashMsg.class, this::onCrashMsg)
            .match(HeartbeatReminder.class, this::onHeartbeatReminder)
            .match(Heartbeat.class, this::onHeartbeat)
            .build();
    }
    
    final AbstractActor.Receive crashed() {
        return receiveBuilder()
            .matchAny(msg -> {})
            .build();
    }
}

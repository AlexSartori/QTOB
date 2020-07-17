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
    
    private final int replicaID;
    private int value;
    private final Map<UpdateID, Integer> updateHistory;
    
    // View & Epoch management
    private final Map<Integer, View> views;
    private final Map<Integer, Integer> flushes_received;
    private final TimeoutMap<Integer> update_req_timers;
    private final TimeoutMap<UpdateID> writeok_timers;

    private Integer coordinatorID;
    private int epoch, seqNo;
    private final Map<Integer, ActorRef> nodes_by_id;
    private List<ActorRef> alive_peers;
    
    // Only used if replica is coordinator
    private final Map<UpdateID, Integer> updateAcks;
    private final TimeoutList election_ack_timers;
    
    
    public ReplicaActor(int ID, int value) {
        this.state = State.VIEW_CHANGE;
        this.replicaID = ID;
        this.nodes_by_id = new HashMap<>();
        this.alive_peers = new ArrayList<>();
        this.value = value;
        this.updateHistory = new HashMap<>(); // to be changed, maybe
        this.views = new HashMap<>();
        this.flushes_received = new HashMap<>();
        this.update_req_timers = new TimeoutMap<>(this::onUpdateRequestTimeout, QTOB.NWK_TIMEOUT_MS);
        this.writeok_timers = new TimeoutMap<>(this::onWriteOkTimeout, QTOB.NWK_TIMEOUT_MS);

        
        this.coordinatorID = null;
        this.epoch = -1;  // on the first election this will change to 0
        this.seqNo = 0;
        
        this.updateAcks = new HashMap<>();
        this.election_ack_timers = new TimeoutList(this::onElectionAckTimeout, QTOB.NWK_TIMEOUT_MS);
    }

    static public Props props(int ID, int value) {
        return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }
    
    private void setStateToCrashed() {
        this.state = State.CRASHED;
        getContext().become(crashed());
    }
    
    private void beginElection() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " beginElection()");
        
        // Ring-based Algorithm
        this.state = State.ELECTING;
        
        Election msg = createElectionMsg();
        
        int next = getNextIDInRing();
        QTOB.simulateNwkDelay();
        this.alive_peers.get(next).tell(msg, getSelf());
        this.election_ack_timers.addTimer();
    }
    
    private Election createElectionMsg() {
        ArrayList<Integer> ids = new ArrayList<>();
        ids.add(replicaID);
        return new Election(ids);
    }
    
    private Election expandElectionMsg(Election msg) {
        ArrayList<Integer> ids = new ArrayList<>(msg.IDs);
        ids.add(replicaID);
        return new Election(ids);
    }
    
    private void onElection(Election msg) {
        if (this.state == State.VIEW_CHANGE)
            return; // Not ready, don't know the group yet
        if (this.state != State.ELECTING)
            this.state = State.ELECTING;
        
        Boolean end_election = msg.IDs.contains(this.replicaID);
        ActorRef next = this.alive_peers.get(getNextIDInRing());
        
        if (end_election) { // Change to coordinator message type
            next.tell(
                new Coordinator(new ArrayList<>(msg.IDs)),
                getSelf()
            );
        } else {
            // Add my ID and recirculate
            Election el_msg = expandElectionMsg(msg);
            next.tell(el_msg, getSelf());
            this.election_ack_timers.addTimer();
	
        }
        // Send back an ElectionAck to the ELECTION sender
        getSender().tell(
            new ElectionAck(this.replicaID),
            getSelf()
        );
    }
    
    private void onCoordinator(Coordinator msg) {
        if (this.state != State.ELECTING)
            return; // End recirculation
        
	// Election based on ID
        this.coordinatorID = findMaxID(msg.IDs);
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " coordinator = " + coordinatorID);
	
        if (this.coordinatorID == this.replicaID) {
            //this.epoch++; // Non dovrebbe incrementare al cambio di View?
            this.seqNo = 0;
            scheduleNextHeartbeatReminder();
        }
	
        this.state = State.BROADCAST;
        
        int next = getNextIDInRing();
        this.alive_peers.get(next).tell(
            new Coordinator(new ArrayList<>(msg.IDs)),
            getSelf()
        );
    }
    
    private int findMaxID(List<Integer> ids) {
        int max = -1;
        for (int id : ids)
            if (id > max)
                max = id;
        return max;
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
    
    private void onElectionAck(ElectionAck msg) {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " ElectionAck from " + msg.from);
        this.election_ack_timers.cancelFirstTimer();
    }
    
    private void onElectionAckTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " ElectionAck timeout");
        
        // Create and propagate new View
        int crashed = getNextIDInRing();
        List<ActorRef> new_peers = new ArrayList<>(this.alive_peers);
        new_peers.remove(crashed);
        createAndPropagateView(new_peers);
    }
    
    private int getNextIDInRing() {
        int idx = alive_peers.indexOf(getSelf());
        return (idx+1) % alive_peers.size();
    }
    
    private void createAndPropagateView(List<ActorRef> new_group) {
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
        
        if (coordinatorID == null && state != State.ELECTING)
            beginElection();
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (coordinatorID == null) {
            beginElection();
        }
        if (this.state == State.ELECTING) {
            // TODO: enqueue requests during elections
            return;
        }
        
        if (this.state == State.VIEW_CHANGE) {
            // TODO: enqueue requests during view changes
            return;
        }
        
        if (this.coordinatorID == this.replicaID) {
            propagateUpdate(req.new_value);
        } else {
            // Forward to coordinator
            this.nodes_by_id.get(this.coordinatorID).tell(
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
        if (this.coordinatorID != this.replicaID) {
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
        if (this.coordinatorID == null || this.coordinatorID != this.replicaID)
            return;
        
        for (ActorRef a : this.alive_peers)
            a.tell(new Heartbeat(), getSelf());
        
        scheduleNextHeartbeatReminder();
    }
    
    private void onUpdateRequestTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " update request timed out");
        
        // Declare coordinator is dead
        List<ActorRef> new_peers = new ArrayList<>(this.alive_peers);
        new_peers.remove(this.nodes_by_id.get(coordinatorID));
        this.coordinatorID = null;
        createAndPropagateView(new_peers);
    }
    
    private void onWriteOkTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + replicaID + " wait for WriteOk timed out");
        
        List<ActorRef> new_peers = new ArrayList<>(this.alive_peers);
        new_peers.remove(this.nodes_by_id.get(coordinatorID));
        this.coordinatorID = null;
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
            .match(Election.class, this::onElection)
            .match(ElectionAck.class, this::onElectionAck)
            .match(Coordinator.class, this::onCoordinator)
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

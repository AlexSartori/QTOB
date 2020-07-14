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
    private final List<View> views;
    private final Map<Integer, Integer> flushes_received;

    private Integer coordinatorID;
    private int epoch, seqNo;
    private List<ActorRef> peers;
    
    // Only used if replica is coordinator
    private final Map<UpdateID, Integer> updateAcks;
    private final TimeoutManager election_ack_timers;
    
    
    public ReplicaActor(int ID, int value) {
        this.state = State.VIEW_CHANGE;
        this.replicaID = ID;
        this.peers = new ArrayList<>();
        this.value = value;
        this.updateHistory = new HashMap<>(); // to be changed, maybe
        this.views = new ArrayList<>();
        this.flushes_received = new HashMap<>();
        
        this.coordinatorID = null;
        this.epoch = -1;  // on the first election this will change to 0
        this.seqNo = 0;
        
        this.updateAcks = new HashMap<>();
        this.election_ack_timers = new TimeoutManager(this::onElectionAckTimeout);
    }

    static public Props props(int ID, int value) {
        return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }
    
    private void setStateToCrashed() {
        this.state = State.CRASHED;
        getContext().become(crashed());
    }
    
    private void beginElection() {
        // Ring-based Algorithm
        this.state = State.ELECTING;
        
        Election msg = createElectionMsg();
        
        int next = getNextIDInRing();
        QTOB.simulateNwkDelay();
        this.peers.get(next).tell(msg, getSelf());
        this.election_ack_timers.addTimer(QTOB.NWK_TIMEOUT_MS);
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
        
        Boolean end_election = msg.IDs.contains(this.replicaID);
        ActorRef next = this.peers.get(getNextIDInRing());
        
        if (end_election) { // Change to coordinator message type
            next.tell(
                new Coordinator(new ArrayList<>(msg.IDs)),
                getSelf()
            );
        } else {
            // Add my ID and recirculate
            Election el_msg = expandElectionMsg(msg);
            next.tell(el_msg, getSelf());
            this.election_ack_timers.addTimer(QTOB.NWK_TIMEOUT_MS);
	
            // Send back an ElectionAck to the ELECTION sender
            getSender().tell(
                new ElectionAck(this.replicaID),
                getSelf()
            );
        }
    }
    
    private void onCoordinator(Coordinator msg) {
        if (this.state != State.ELECTING)
            return; // End recirculation
        
        this.election_ack_timers.cancelFirstTimer();
        
	// Election based on ID
        this.coordinatorID = findMaxID(msg.IDs);
	
        if (this.coordinatorID == this.replicaID) {
            this.epoch++; // Non dovrebbe incrementare al cambio di View?
            this.seqNo = 0;
            scheduleNextHeartbeatReminder();
        }
	
        this.state = State.BROADCAST;
        
        int next = getNextIDInRing();
        this.peers.get(next).tell(
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
        this.election_ack_timers.cancelFirstTimer();
    }
    
    private void onElectionAckTimeout() {
        System.out.println("Election Ack timeout");
        
        // Create and propagate new View
        int crashed = getNextIDInRing();
        List<ActorRef> new_peers = new ArrayList<>(this.peers);
        new_peers.remove(crashed);
        createAndPropagateView(new_peers);
    }
    
    private int getNextIDInRing() {
        int idx = peers.indexOf(getSelf());
        return (idx+1) % peers.size();
    }
    
    private void createAndPropagateView(List<ActorRef> new_group) {
        this.state = State.VIEW_CHANGE;
        this.epoch++;
        
        View v = new View(this.epoch, new_group);
        addNewView(v);
        
        ViewChange msg = new ViewChange(v);
        for (ActorRef a : new_group)
           a.tell(msg, getSelf()); 
    }
    
    private void addNewView(View v) {
        this.views.add(v);
        flushes_received.put(v.viewID, 0);
    }
    
    private void onViewChange(ViewChange msg) {
        this.state = State.VIEW_CHANGE; // Pause sending new multicasts
        
        addNewView(msg.view);
        // TODO (?) Send all unstable messages
        flushViewToAll(msg.view);
    }
    
    private void flushViewToAll(View v) {
        Flush msg = new Flush(v.viewID);
        
        for (ActorRef r : v.peers)
            if (r != getSelf())
                r.tell(msg, getSelf());   
    }
    
    private void onFlush(Flush msg) {
        incrementFlushAks(msg.id);
        
        if (isFlushComplete(msg.id)) {
            installView(msg.id);
            beginElection(); // TODO: always?
        }
    }
    
    private int incrementFlushAks(int id) {
        // TODO: handle flushes for unknown (yet unseen) views
        int n_flushes = flushes_received.get(id) + 1;
        flushes_received.replace(id, n_flushes);
        return n_flushes;
    }
    
    private boolean isFlushComplete(int id) {
        int n_peers = views.get(id).peers.size();
        int n_flushes = flushes_received.get(id);
        return n_flushes == n_peers - 1;
    }
    
    private void installView(int id) {
        this.peers = this.views.get(id).peers;
    }
    
    private void onReadRequest(ReadRequest req) {
        System.out.println("Replica " + replicaID + " read request");
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
    }
    
    private void onWriteRequest(WriteRequest req) {
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
            this.peers.get(this.coordinatorID).tell(  // ID may not correspond to index
                req,
                getSelf()
            );
        }
    }
    
    private void propagateUpdate(int value) {
        UpdateID u_id = new UpdateID(epoch, seqNo++);  // be careful with ++
        Update u = new Update(u_id, value);

        this.updateAcks.put(u_id, 0);

        for (ActorRef a : this.peers)
            if (a != getSelf())
                a.tell(new UpdateMsg(u), getSelf());
    }
    
    private void onUpdateMsg(UpdateMsg msg) {
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
        int Q = Math.floorDiv(peers.size(), 2) + 1;
        
        if (curr_acks == Q) {
            for (ActorRef r : peers)
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
        
        for (ActorRef a : this.peers)
            a.tell(new Heartbeat(), getSelf());
        
        scheduleNextHeartbeatReminder();
    }
    
    private void onHeartbeat(Heartbeat msg) {
        // TODO: cancel coordinator crash timeout (to be implemented)
        // System.out.println("Replica " + replicaID + " received heartbeat");
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeGroup.class, (InitializeGroup msg) -> createAndPropagateView(msg.group))
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

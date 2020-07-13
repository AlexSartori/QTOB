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
    private final List<ViewChange> views;
    private final Map<Integer, Integer> flushes_received;

    private Integer coordinatorID;
    private int epoch, seqNo;
    private List<ActorRef> peers;
    
    // Only used if replica is coordinator
    private final Map<UpdateID, Integer> updateAcks;
    
    
    public ReplicaActor(int ID, int value) {
        this.state = State.ELECTING;
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
    }

    static public Props props(int ID, int value) {
        return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }
    
    private void setStateToCrashed() {
        this.state = State.CRASHED;
    }
    
    private void beginElection() {
        if (this.state == State.CRASHED)
            return;
        
        // Ring-based Algorithm
        this.state = State.ELECTING;
        
        Election msg = createElectionMsg();
        
        int next = getNextIDInRing();
        this.peers.get(next).tell(msg, getSelf());
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
        if (this.state == State.CRASHED)
            return;
        
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
	
            // Send back an ElectionAck to the ELECTION sender
            getSender().tell(
                new ElectionAck(this.replicaID),
                getSelf()
            );
        }
    }
    
    private void onCoordinator(Coordinator msg) {
        if (this.state == State.CRASHED)
            return;
        if (this.state != State.ELECTING)
            return; // End recirculation
        
	// Election based on ID
        this.coordinatorID = findMaxID(msg.IDs);
	
        if (this.coordinatorID == this.replicaID) {
            this.epoch++;
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
        return; // TODO timeout node and create viewchange
    }
    
    private int getNextIDInRing() {
        int idx = peers.indexOf(getSelf());
        return (idx+1) % peers.size();
    }
    
    private void onViewChange(ViewChange v) {
        if (this.state == State.CRASHED)
            return;
        
        this.state = State.VIEW_CHANGE; // Pause sending new multicasts
       
        addNewView(v);
        // TODO (?) Send all unstable messages
        flushViewToAll(v);
    }
    
    private void addNewView(ViewChange v) {
        this.views.add(v);
        flushes_received.put(v.viewID, 0);
    }
    
    private void flushViewToAll(ViewChange v) {
        Flush msg = new Flush(v.viewID);
        
        for (ActorRef r : v.peers)
            if (r != getSelf())
                r.tell(msg, getSelf());   
    }
    
    private void onFlush(Flush msg) {
        if (this.state == State.CRASHED)
            return;
        
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
        if (this.state == State.CRASHED)
            return;
        
        System.out.println("Replica " + replicaID + " read request");
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (this.state == State.CRASHED) {
            return;
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
        if (this.state == State.CRASHED)
            return;
        
        getSender().tell(
            new UpdateAck(msg.u),
            getSelf()
        );
    }
    
    private void onUpdateAck(UpdateAck msg) {
        if (this.state == State.CRASHED)
            return;
        
        if (this.coordinatorID != this.replicaID) {
            System.err.println("!!! Received Ack even if not coordinator");
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
        if (this.state == State.CRASHED)
            return;
        
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
}

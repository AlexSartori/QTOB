package main;

import akka.actor.ActorRef;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alex
 */
public class ElectionManager {
    ReplicaActor parent;
    public Integer coordinatorID;
    private final TimeoutList election_ack_timers;
    Runnable new_coord_callback;
    public boolean electing;
    
    public ElectionManager(ReplicaActor parent, Runnable new_coord_callback) {
        this.parent = parent;
        this.new_coord_callback = new_coord_callback;
        this.coordinatorID = null;
        this.election_ack_timers = new TimeoutList(this::onElectionAckTimeout, QTOB.NWK_TIMEOUT_MS);
        this.electing = false;
    }
    
    public void beginElection() {
        if (electing) return;
        electing = true;
        if (QTOB.VERBOSE) System.out.println("Replica " + parent.replicaID + " beginElection()");
        
        // Ring-based Algorithm
        Messages.Election msg = createElectionMsg();
        
        ActorRef next = parent.getNextActorInRing();
        QTOB.simulateNwkDelay();
        next.tell(msg, parent.getSelf());
        this.election_ack_timers.addTimer();
    }
    
    private Messages.Election createElectionMsg() {
        ArrayList<Integer> ids = new ArrayList<>();
        ids.add(parent.replicaID);
        return new Messages.Election(ids);
    }
    
    private Messages.Election expandElectionMsg(Messages.Election msg) {
        ArrayList<Integer> ids = new ArrayList<>(msg.IDs);
        ids.add(parent.replicaID);
        return new Messages.Election(ids);
    }
    
    private int findMaxID(List<Integer> ids) {
        int max = -1;
        for (int id : ids)
            if (id > max)
                max = id;
        return max;
    }
    
    public void onElection(Messages.Election msg) {
        electing = true;
        coordinatorID = null;
        
        Boolean end_election = msg.IDs.contains(parent.replicaID);
        ActorRef next = parent.getNextActorInRing();
        
        if (end_election) { // Change to coordinator message type
            next.tell(
                new Messages.Coordinator(new ArrayList<>(msg.IDs)),
                parent.getSelf()
            );
        } else {
            // Add my ID and recirculate
            Messages.Election el_msg = expandElectionMsg(msg);
            next.tell(el_msg, parent.getSelf());
            this.election_ack_timers.addTimer();
        }
        
        // Send back an ElectionAck to the ELECTION sender
        parent.getSender().tell(
            new Messages.ElectionAck(parent.replicaID),
            parent.getSelf()
        );
    }
    
    public void onElectionAck(Messages.ElectionAck msg) {
        if (QTOB.VERBOSE) System.out.println("Replica " + parent.replicaID + " ElectionAck from " + msg.from);
        this.election_ack_timers.cancelFirstTimer();
    }
    
    private void onElectionAckTimeout() {
        if (QTOB.VERBOSE) System.out.println("Replica " + parent.replicaID + " ElectionAck timeout");
        electing = false;
        // parent.onCrashedNode(parent.getNextActorInRing());
    }
        
    public void onCoordinator(Messages.Coordinator msg) {
        if (coordinatorID != null)
            return; // End recirculation
        
	// Election based on ID
        coordinatorID = findMaxID(msg.IDs);
        electing = false;
        new_coord_callback.run();
        
        parent.getNextActorInRing().tell(
            new Messages.Coordinator(new ArrayList<>(msg.IDs)),
            parent.getSelf()
        );
    }
}

package main;

import akka.actor.ActorRef;
import java.util.ArrayList;
import java.util.List;
import main.Messages.*;

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
        if (QTOB.VERBOSE) parent.log("beginElection()");
        electing = true;
        coordinatorID = null;
        
        Election msg = createElectionMsg();
        parent.sendWithNwkDelay(parent.getNextActorInRing(), msg);
        this.election_ack_timers.addTimer();
    }
    
    private Election createElectionMsg() {
        ArrayList<Integer> ids = new ArrayList<>();
        ids.add(parent.replicaID);
        return new Election(ids, parent.getMostRecentUpdate(), parent.replicaID);
    }
    
    private Election expandElectionMsg(Election msg) {
        ArrayList<Integer> ids = new ArrayList<>(msg.IDs);
        ids.add(parent.replicaID);
        
        UpdateID latest = msg.most_recent_update;
        UpdateID my_latest = parent.getMostRecentUpdate();
        int update_owner = msg.most_recent_update_owner;
        if (my_latest != null && my_latest.happensAfter(latest)) {
            if (QTOB.VERBOSE) parent.log("Adding most recent update to election");
            latest = parent.getMostRecentUpdate();
            update_owner = parent.replicaID;
        }
        
        return new Election(ids, latest, update_owner);
    }
    
    private int findMaxID(List<Integer> ids) {
        int max = -1;
        for (int id : ids)
            if (id > max)
                max = id;
        return max;
    }
    
    public void onElection(Election msg) {
        electing = true;
        coordinatorID = null;
        
        Boolean end_election = msg.IDs.contains(parent.replicaID);
        ActorRef next = parent.getNextActorInRing();
        
        if (end_election) { // Change to coordinator message type
            parent.sendWithNwkDelay(
                next,
                new Coordinator(new ArrayList<>(msg.IDs))
            );
        } else {
            // Add my ID and recirculate
            parent.sendWithNwkDelay(next, expandElectionMsg(msg));
            this.election_ack_timers.addTimer();
        }
        
        // Send back an ElectionAck to the ELECTION sender
        parent.sendWithNwkDelay(parent.getSender(), new ElectionAck(parent.replicaID));
    }
    
    public void onElectionAck(ElectionAck msg) {
        // if (QTOB.VERBOSE) parent.log("ElectionAck from " + msg.from);
        this.election_ack_timers.cancelFirstTimer();
    }
    
    private void onElectionAckTimeout() {
        if (QTOB.VERBOSE) parent.log("ElectionAck timeout");
        parent.crashed_nodes.add(parent.getNextIDInRing());
        electing = false;
        beginElection();
    }
    
    public void onCoordinator(Coordinator msg) {
        if (coordinatorID != null)
            return; // End recirculation
        
	// Election based on ID
        coordinatorID = findMaxID(msg.IDs);
        electing = false;
        new_coord_callback.run();
        
        parent.getNextActorInRing().tell(
            new Coordinator(new ArrayList<>(msg.IDs)),
            parent.getSelf()
        );
    }
}

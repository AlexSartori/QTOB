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
        
        UpdateList updates = new UpdateList();
        updates.add(parent.getMostRecentUpdate());
        
        return new Election(ids, updates, parent.replicaID);
    }
    
    private Election expandElectionMsg(Election msg) {
        ArrayList<Integer> ids = new ArrayList<>(msg.IDs);
        ids.add(parent.replicaID);
        
        UpdateList latest = msg.most_recent_updates;
        int update_owner = msg.most_recent_update_owner;
        Update my_latest = parent.getMostRecentUpdate();
        if (my_latest != null && my_latest.happensAfter(latest.getMostRecent())) {
            if (QTOB.VERBOSE) parent.log("Adding most recent update to election");
            latest.add(my_latest);
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
        
        int winner_so_far = getWinner(msg);
        ActorRef next = parent.getNextActorInRing();
        if (QTOB.VERBOSE) parent.log("onElection, IDs=" + msg.IDs);
        
        if (msg.IDs.contains(parent.replicaID) && winner_so_far == parent.replicaID) {
            if (QTOB.VERBOSE) parent.log("Won election, synchronizing");
            // Change to synchronize message type
            for (int id : parent.nodes_by_id.keySet())
                if (!parent.crashed_nodes.contains(id))
                    parent.sendWithNwkDelay(
                        parent.nodes_by_id.get(id),
                        new Synchronize(msg.most_recent_updates)
                    );
        } else if (!msg.IDs.contains(parent.replicaID)) {
            // Add my updates and recirculate
            parent.sendWithNwkDelay(next, expandElectionMsg(msg));
            this.election_ack_timers.addTimer();
        }
        
        // Send back an ElectionAck to the ELECTION sender
        parent.sendWithNwkDelay(parent.getSender(), new ElectionAck(parent.replicaID));
    }
    
    private int getWinner(Election msg) {
        if (msg.most_recent_updates.isEmpty())
            return findMaxID(msg.IDs);
        else
            return msg.most_recent_update_owner;
    }
    
    public void onElectionAck(ElectionAck msg) {
        if (QTOB.VERBOSE) parent.log("ElectionAck from " + msg.from);
        this.election_ack_timers.cancelFirstTimer();
    }
    
    private void onElectionAckTimeout() {
        if (QTOB.VERBOSE) parent.log("ElectionAck timeout");
        parent.crashed_nodes.add(parent.getNextIDInRing());
        electing = false;
        beginElection();
    }
    
    public void onSynchronize(Synchronize msg) {
        ActorRef coord = parent.getSender();
        for (int id : parent.nodes_by_id.keySet())
            if (parent.nodes_by_id.get(id) == coord) {
                coordinatorID = id;
                break;
            }
        
        if (QTOB.VERBOSE) parent.log("Recv synch, updates -> " + msg.updates);
        electing = false;
        new_coord_callback.run();
    }
}

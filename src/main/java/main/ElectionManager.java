package main;

import akka.actor.ActorRef;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import main.Messages.*;

/**
 *
 * @author alex
 */
public class ElectionManager {
    ReplicaActor parent;
    public Integer coordinatorID;
    public final TimeoutList election_ack_timers, election_timers;
    Consumer<UpdateList> new_coord_callback;
    public boolean electing;
    
    public ElectionManager(ReplicaActor parent, Consumer<UpdateList> new_coord_callback) {
        this.parent = parent;
        this.new_coord_callback = new_coord_callback;
        this.coordinatorID = null;
        this.election_ack_timers = new TimeoutList(this::onElectionAckTimeout, QTOB.NWK_TIMEOUT_MS);
        this.election_timers = new TimeoutList(this::onElectionTimeout, QTOB.ELECTION_TIMEOUT);
        this.electing = false;
    }
    
    public void beginElection() {
        if (electing) return;
        
        if (CrashHandler.getInstance().shouldCrash(parent.replicaID, CrashHandler.Situation.ON_BEGIN_ELECTION)) {
            parent.setStateToCrashed();
            return;
        }
        
        if (QTOB.VERBOSE) parent.log("beginElection()");
        electing = true;
        coordinatorID = null;
        election_timers.addTimer();
        
        Election msg = createElectionMsg();
        parent.sendWithNwkDelay(parent.getNextActorInRing(), msg);
        this.election_ack_timers.addTimer();
    }
    
    private Election createElectionMsg() {
        HashMap<Integer, Update> updates = new HashMap<>();
        Update most_recent = parent.getMostRecentUpdate();
        updates.put(parent.replicaID, most_recent);
        
        if (QTOB.VERBOSE) parent.log("Created: " + updates);
        return new Election(updates);
    }
    
    public void onElection(Election msg) {
        if (CrashHandler.getInstance().shouldCrash(parent.replicaID, CrashHandler.Situation.ON_ELECTION_MSG_RCV)) {
            parent.setStateToCrashed();
            return;
        }
        if (QTOB.VERBOSE) parent.log("onElection, updates=" + msg.most_recent_updates);
        coordinatorID = null;
        
        // Send back an ElectionAck to the ELECTION sender
        parent.sendWithNwkDelay(parent.getSender(), new ElectionAck(parent.replicaID));
        
        if (CrashHandler.getInstance().shouldCrash(parent.replicaID, CrashHandler.Situation.ON_ELECTION_ACK_SND)) {
            parent.setStateToCrashed();
            return;
        }
        
        if (!msg.most_recent_updates.containsKey(parent.replicaID)) {
            //if (electing && election_timers.size() > 0)
            //    election_timers.cancelFirstTimer();
            electing = true;
            election_timers.addTimer();
        }
        
        boolean won = msg.most_recent_updates.containsKey(parent.replicaID) && getWinner(msg) == parent.replicaID;
        
        if (won) {
            if (QTOB.VERBOSE) parent.log("Won election, synchronizing");
            Synchronize(msg.most_recent_updates);
        } else {
            // Add my updates and recirculate
            parent.sendWithNwkDelay(parent.getNextActorInRing(), expandElectionMsg(msg));
            this.election_ack_timers.addTimer();
            if (CrashHandler.getInstance().shouldCrash(parent.replicaID, CrashHandler.Situation.ON_ELECTION_MSG_SND)) {
                parent.setStateToCrashed();
                return;
            }
        }
    }
    
    private int getWinner(Election msg) {
        Map<Integer, Update> u = msg.most_recent_updates;
        int update_owner = findMaxID(u.keySet().toArray());
        Update latest = u.get(update_owner);
        
        for (int id : u.keySet())
            if (u.get(id) != null)
                if (latest == null || u.get(id).happensAfter(latest)) {
                    latest = u.get(id);
                    update_owner = id;
                }
        
        return update_owner;
    }
    
    private Election expandElectionMsg(Election msg) {
        Map<Integer, Update> updates = new HashMap<>(msg.most_recent_updates);
        
        if (!updates.containsKey(parent.replicaID))
            updates.put(parent.replicaID, parent.getMostRecentUpdate());
        
        return new Election(updates);
    }
    
    private int findMaxID(Object[] ids) {
        int max = -1;
        for (Object id : ids)
            if ((int)id > max)
                max = (int)id;
        return max;
    }
    
    private void Synchronize(Map<Integer, Update> updates) {
        if (CrashHandler.getInstance().shouldCrash(parent.replicaID, CrashHandler.Situation.ON_SYNCH_SND)) {
            parent.setStateToCrashed();
            return;
        }
        
        UpdateList list = new UpdateList();
        for (Update u : updates.values())
            if (u != null)
                list.add(u);

        for (int id : parent.nodes_by_id.keySet())
            if (!parent.crashed_nodes.contains(id))
                parent.sendWithNwkDelay(
                    parent.nodes_by_id.get(id),
                    new Synchronize(list)
                );
    }
    
    public void onElectionAck(ElectionAck msg) {
        if (CrashHandler.getInstance().shouldCrash(parent.replicaID, CrashHandler.Situation.ON_ELECTION_ACK_RCV)) {
            parent.setStateToCrashed();
            return;
        }
        
        if (QTOB.VERBOSE) parent.log("ElectionAck from " + msg.from);
        this.election_ack_timers.cancelFirstTimer();
    }
    
    private void onElectionAckTimeout() {
        if (QTOB.VERBOSE) parent.log("ElectionAck timeout");
        if (electing) parent.crashed_nodes.add(parent.getNextIDInRing()); // Election might have already stopped
        electing = false;
        election_timers.cancelFirstTimer();
        //beginElection();
    }
    
    private void onElectionTimeout() {
        if (QTOB.VERBOSE) parent.log("Election timeout");
        electing = false;
        // beginElection();
    }
    
    public void onSynchronize(Synchronize msg) {
        if (CrashHandler.getInstance().shouldCrash(parent.replicaID, CrashHandler.Situation.ON_SYNCH_RCV)) {
            parent.setStateToCrashed();
            return;
        }
        
        ActorRef coord = parent.getSender();
        for (int id : parent.nodes_by_id.keySet())
            if (parent.nodes_by_id.get(id) == coord) {
                coordinatorID = id;
                break;
            }
        
        if (QTOB.VERBOSE) parent.log("Recvd synch, updates -> " + msg.updates);
        electing = false;
        if (election_timers.size() > 0)
            election_timers.cancelFirstTimer();
        new_coord_callback.accept(msg.updates);
    }
}

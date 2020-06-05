package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import main.Messages.*;

/**
 *
 * @author alex
 */
public class ReplicaActor extends AbstractActor {
    private enum State { ELECTING, BROADCAST, CRASHED };
    private State state;
    
    private final int replicaID;
    private int[] vector_clock;
    private int value;
    private final Map<UpdateID, Integer> updateHistory;

    // Only used if replica is the coordinator
    private final List<ActorRef> peers;
    private Integer coordinator;
    private int epoch, seqNo;
    private final Map<UpdateID, Integer> acks;
    

    
    public ReplicaActor(int ID, int value) {
        this.state = State.ELECTING;
        this.replicaID = ID;
        this.peers = new ArrayList<>();
        this.value = value;
        
        this.coordinator = null;
        this.epoch = 0;
        this.seqNo = 0;
        
        this.updateHistory = new HashMap<>();
        this.acks = new HashMap<>();
    }

    static public Props props(int ID, int value) {
        return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }

    private void updateVectorClock(int[] vc) {
        // If vc != null update all VCs, otherwise only mine
        if (vc != null)
            for (int i = 0; i < vc.length; i++)
                if (i != replicaID && vc[i] > vector_clock[i])
                    vector_clock[i] = vc[i];
        
        vector_clock[replicaID]++;
    }
    
    private String vcToString() {
        String res = "[";
        for (int vc : vector_clock)
            res += vc + ", ";
        return res.substring(0, res.length() - 2) + "]";
    }
    
    private void beginElection() {
        // Ring-based Algorithm
        this.state = State.ELECTING;
        ArrayList<Integer> ids = new ArrayList<>();
        ids.add(replicaID);
        
        int next = getNext(this.peers, this.getSelf());
        System.out.println("from ID " + this.replicaID + 
                            " to peer index " + next);
        
        this.peers.get(next).tell(
            new Election(ids),
            getSelf()
        );
    }
    
    private int getNext(List<ActorRef> peers, ActorRef replica) {
        int idx = peers.indexOf(replica);
		
        if (idx==peers.size()-1) {
            return 0;   // first element of the list
        }
        else {
            idx++;
            return idx;
        }

//		Iterator iter = peers.listIterator(idx);
//		if (!iter.hasNext()) {   // findNext called by the last element
//	        return 0;
//		}
//		else {
//			return peers.indexOf(iter.next());
//		}
    }
	
    private void onJoinGroup(JoinGroupMsg msg) {
        this.peers.addAll(msg.group);
        
        this.vector_clock = new int[msg.group.size()];
        
        beginElection();
    }
    
    private void onReadRequest(ReadRequest req) {
        System.out.println("Replica " + replicaID + " read request");
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
    }
    
    private void onWriteRequest(WriteRequest req) {
        if (this.state == State.ELECTING) {
            System.out.println("Replica " + this.replicaID + " election in progress, dropping request (<-- TODO!)");
            return;
        }
        
        if (this.coordinator == this.replicaID) {
            // Propagate Update Msg
            UpdateID u_id = new UpdateID(epoch, seqNo++);
            Update u = new Update(u_id, req.new_value);
            
            this.acks.put(u_id, 0);
            
            for (ActorRef a : this.peers)
                a.tell(new UpdateMsg(u), getSelf());
        } else {
            // Forward to coordinator
            this.peers.get(this.coordinator).tell(
                new WriteRequest(req.client, req.new_value),
                getSelf()
            );
        }
        
        updateVectorClock(null);
        System.out.println("Replica " + this.replicaID + " TODO: Complete handle of write request " + vcToString());
    }
    
    private void onUpdateMsg(UpdateMsg msg) {
        getSender().tell(
            new Ack(msg.u),
            getSelf()
        );
    }
    
    private void onAck(Ack msg) {
        if (this.coordinator != this.replicaID) {
            System.err.println("!!! Received Ack even if not coordinator");
            return;
        }
        
        // Wait for Q acks and propagate writeok to everyone
        int curr_acks = acks.get(msg.u.id) + 1;
        this.acks.replace(msg.u.id, curr_acks);
        
        if (curr_acks == peers.size()) { // TODO: set quorum to correct value
            for (ActorRef r : peers)
                r.tell(
                    new WriteOk(msg.u),
                    getSelf()
                );
        }
    }
    
    private void onWriteOk(WriteOk msg) {
        Update u = msg.u;
        this.updateHistory.put(u.id, u.value);
        this.value = u.value;
        
        System.out.println("Confirmed write <" + u.id.epoch + "," + u.id.seqNo + ">: " + u.value);
    }

    private void onElection (Election msg) {
        Boolean recirculate = !msg.IDs.contains(this.replicaID);
        int next = getNext(this.peers, this.getSelf());
        
        if (recirculate) {
            // Add my ID and recirculate
            ArrayList<Integer> ids = new ArrayList<>(msg.IDs);
            ids.add(this.replicaID);
            
            this.peers.get(next).tell(
                new Election(ids),
                getSelf()
            );
        } else {
            // Change to coordinator message type
            this.peers.get(next).tell(
                new Coordinator(new ArrayList<>(msg.IDs)),
                getSelf()
            );
        }
    }
    
    private void onCoordinator(Coordinator msg) {
        if (this.state != State.ELECTING)
            return; // End recirculation
        
        int coord = -1;
        for (int id : msg.IDs)
            if (id > coord)
                coord = id;
        this.coordinator = coord;
        this.state = State.BROADCAST;
        System.out.println("Replica " + replicaID + " - Coordinator => " + coord);
        
        int next = getNext(this.peers, this.getSelf());
        this.peers.get(next).tell(
            new Coordinator(new ArrayList<>(msg.IDs)),
            getSelf()
        );
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroup)
            .match(ReadRequest.class, this::onReadRequest)
            .match(WriteRequest.class, this::onWriteRequest)
            .match(UpdateMsg.class, this::onUpdateMsg)
            .match(Ack.class, this::onAck)
            .match(WriteOk.class, this::onWriteOk)
            .match(Election.class, this::onElection)
            .match(Coordinator.class, this::onCoordinator)
            .build();
    }
}

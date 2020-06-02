package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import main.Messages.*;

/**
 *
 * @author alex
 */
public class ReplicaActor extends AbstractActor {
    private final int replicaID;
    private int[] vector_clock;
    private int value;
	
    private final List<ActorRef> peers;
    private Integer coordinator;
	
    private enum State { ELECTING, BROADCAST };
    private State state;

    private final Map<Integer, Update> UpdatesHistory;

    
    public ReplicaActor(int ID, int value) {
        this.replicaID = ID;
        this.peers = new ArrayList<>();
        this.value = value;
        this.coordinator = null;
        this.UpdatesHistory = new HashMap<>();
        this.state = State.ELECTING;
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
        
        System.out.println("Replica " + this.replicaID + " TODO: Actually handle write request " + vcToString());
        updateVectorClock(null);
        
        req.client.tell(
            new WriteResponse(), getSelf()
        );
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
        System.out.println("Replica " + replicaID + " - Coordinator: " + coord);
        
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
            .match(Election.class, this::onElection)
            .match(Coordinator.class, this::onCoordinator)
            .build();
    }
}

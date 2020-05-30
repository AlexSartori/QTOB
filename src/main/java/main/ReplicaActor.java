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
    private final int replicaID;
    private int[] vector_clock;
    private int value;
	
    private final List<ActorRef> peers;
    private ActorRef coordinator;
	
    private enum State { ELECTING };
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
    
    private void onJoinGroup(JoinGroupMsg msg) {
        for (ActorRef r : msg.group)
            if (!r.equals(getSelf()))
                this.peers.add(r);
        
        this.vector_clock = new int[msg.group.size()];
    }
    
    private void onReadRequest(ReadRequest req) {
        System.out.println("Replica " + replicaID + " read request " + vcToString());
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
    }
    
    private void onWriteRequest(WriteRequest req) {
        System.out.println("Replica " + this.replicaID + " TODO: Actually handle write request " + vcToString());
        updateVectorClock(null);
        
        req.client.tell(
            new WriteResponse(), getSelf()
        );
    }

    private void onElection (Election election) {
        
    }
	
    private void onSynchronization (Synchronization synch) {

    }
	
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroup)
            .match(ReadRequest.class, this::onReadRequest)
            .match(WriteRequest.class, this::onWriteRequest)
            .match(Election.class, this::onElection)
            .match(Synchronization.class, this::onSynchronization)
            .build();
    }
}

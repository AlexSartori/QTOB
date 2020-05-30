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

	
    private void onJoinGroup(JoinGroupMsg msg) {
        for (ActorRef r : msg.group)
            if (!r.equals(getSelf()))
                this.peers.add(r);
    }
    
    private void onReadRequest(ReadRequest req) {
        req.client.tell(
            new ReadResponse(this.value), getSelf()
        );
    }
    
    private void onWriteRequest(WriteRequest req) {
        req.client.tell(
            new WriteResponse(), getSelf()
        );
        System.out.println("TODO: Actually handle write request");
    }

    private void onElection (Election election) {
        if (election.predecessorID==this.replicaID) {
			
        }
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

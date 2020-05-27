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
    private final List peers;
    private final ActorRef coordinator;
    private int v;

    private final Map<Integer, Update> Updatehistory;

    /*-- Actor constructors --------------------------------------------------- */
    public ReplicaActor(ActorRef coordinator, int ID, int value) {
            this.replicaID = ID;
            this.peers = new ArrayList<>();
            this.v = value;
            this.coordinator = coordinator;
            this.Updatehistory = new HashMap<>();
    }

    static public Props props(ActorRef coordinator, int ID, int value) {
            return Props.create(ReplicaActor.class, () -> new ReplicaActor(coordinator, ID, value));
    }

    private void onJoinGroup(JoinGroupMsg msg) {
        for (ActorRef r : msg.group) {
            if (!r.equals(getSelf())) {
                this.peers.add(r);
			}
		}
    }
    
    private void onReadRequest(ReadRequest req) {
        req.client.tell(
            new ReadResponse(this.v),
            getSelf()
        );
    }
    
    private void onWriteRequest(WriteRequest req) {
        req.client.tell(
            new WriteResponse(),
            getSelf()
        );
        System.out.println("TODO: Actually handle write request");
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroup)
            .match(ReadRequest.class, this::onReadRequest)
            .match(WriteRequest.class, this::onWriteRequest)
            .build();
    }
    
    // ========================================================================= Message classes
}

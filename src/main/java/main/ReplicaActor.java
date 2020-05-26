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
    // sarà final?
    private int v;
    private final ActorRef coordinator;

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
        for (ActorRef r : msg.group)
            if (!r.equals(getSelf()))
                this.peers.add(r);
    }
    
    private void onReadRequest(ReadRequest req) {
        System.out.println("Replica " + this.replicaID + " read done " + this.v);
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroup)
            .match(ReadRequest.class, this::onReadRequest)
            .build();
    }
    
    // ========================================================================= Message classes
}

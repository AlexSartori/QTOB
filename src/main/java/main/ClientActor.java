package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import main.Messages.*;
import scala.concurrent.duration.Duration;

/**
 *
 * @author alex
 */
public class ClientActor extends AbstractActor {
    private final int clientID;
    private final Random rng;
    private ActorRef target_replica;
    private int target_replica_id;
    
    public ClientActor(int id) {
        this.clientID = id;
        this.target_replica = null;
        this.target_replica_id = -1;
        this.rng = new Random();
    }

    static public Props props(int id) {
        return Props.create(ClientActor.class, () -> new ClientActor(id));
    }
    
    @Override
    public void preStart() {
        // Schedule messages with fixed intervals
        // to remind sending out requests
        
        Cancellable reqs_timer = getContext().system().scheduler().scheduleWithFixedDelay(
            Duration.create(1, TimeUnit.SECONDS), // When to start
            Duration.create(1, TimeUnit.SECONDS), // Delay between msgs
            getSelf(),                            // To who
            new RequestTimer(),                   // Msg to send
            getContext().system().dispatcher(),   // System dispatcher
            getSelf()                             // Source of the msg
        );
    }
    
    private void onView(View msg) {
        // Choose a destination replica
        this.target_replica_id = rng.nextInt(msg.peers.size());
        this.target_replica = msg.peers.get(target_replica_id);
    }
    
    private void onRequestTimer(RequestTimer req) {        
        // Simulate network delay
        try {
            Thread.sleep(this.rng.nextInt(main.QTOB.MAX_NWK_DELAY_MS));
        } catch (InterruptedException ex) {
            System.err.println("Could not simulate network delay");
        }
        
        if (this.rng.nextBoolean()) {
            // Send read request
            target_replica.tell(
                new ReadRequest(getSelf()),
                getSelf()
            );
            System.out.println("Client " + this.clientID + " read req to " + target_replica_id);
        } else {
            // Send write request
            target_replica.tell(
                new WriteRequest(getSelf(), this.rng.nextInt(1000)),
                getSelf()
            );
            System.out.println("Client " + this.clientID + " write req to " + target_replica_id);
        }
    }
    
    private void onReadResponse(ReadResponse res) {
        System.out.println("Client " + this.clientID + " read done " + res.value);
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(View.class, this::onView)
            .match(RequestTimer.class, this::onRequestTimer)
            .match(ReadResponse.class, this::onReadResponse)
            .build();
    }
    
    // ========================================================================= Message classes
    private static class RequestTimer implements Serializable { }
}

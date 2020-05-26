package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import main.Messages.*;
import scala.concurrent.duration.Duration;

/**
 *
 * @author alex
 */
public class ClientActor extends AbstractActor {
    private final int id;
    private final List replicas;
    private Random rng;
    
    public ClientActor(int id) {
        this.id = id;
        this.replicas = new ArrayList<>();
        this.rng = new Random();
    }

    static public Props props(int id) {
        return Props.create(ClientActor.class, () -> new ClientActor(id));
    }
    
    @Override
    public void preStart() {
        // Schedule messages with fixed intervals
        // to remind sending out requests
        
        // Write requests to replicas
        Cancellable reqs_timer = getContext().system().scheduler().scheduleWithFixedDelay(
            Duration.create(0, TimeUnit.SECONDS), // When to start
            Duration.create(1, TimeUnit.SECONDS), // Delay between msgs
            getSelf(),                            // To who
            new RequestTimer(),                   // Msg to send
            getContext().system().dispatcher(),   // System dispatcher
            getSelf()                             // Source of the msg
        );
    }
    
    private void onJoinGroup(JoinGroupMsg msg) {
        for (ActorRef r : msg.group)
            if (!r.equals(getSelf()))
                this.replicas.add(r);
    }
    
    private void onRequestTimer(RequestTimer req) {
        int destination_id = this.rng.nextInt(this.replicas.size());
        ActorRef destination = (ActorRef)this.replicas.get(
            destination_id
        );
        
        // Simulate network delay
        try {
            Thread.sleep(this.rng.nextInt(main.QTOB.MAX_NWK_DELAY_MS));
        } catch (InterruptedException ex) {
            System.err.println("Could not simulate network delay");
        }
        
        if (this.rng.nextBoolean()) {
            // Send read request
            destination.tell(
                new ReadRequest(getSelf()),
                getSelf()
            );
            
            System.out.println("Client " + this.id + " read req to " + destination_id);
        } else {
            // Send write request
            destination.tell(
                new WriteRequest(getSelf(), this.rng.nextInt()),
                getSelf()
            );
            
            System.out.println("Client " + this.id + " write req to " + destination_id);
        }
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroup)
            .match(RequestTimer.class, this::onRequestTimer)
            .build();
    }
    
    // ========================================================================= Message classes
    public static class RequestTimer implements Serializable { }
}

package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

/**
 *
 * @author alex
 */
public class ClientActor extends AbstractActor {
    private int id;
    
    public ClientActor(int id) {
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(ClientActor.class, () -> new ClientActor(id));
    }
    
    @Override
    public void preStart() {
        // Schedule messages with fixed intervals
        // requesting a replica to read or write
        
        // Write requests to replicas
        Cancellable write_reqs_timer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(2, TimeUnit.SECONDS), // When to start
                Duration.create(3, TimeUnit.SECONDS), // Delay between msgs
                null,             // TODO!            // To who
                null,             // TODO!            // Msg to send
                getContext().system().dispatcher(),   // System dispatcher
                getSelf()                             // Aource of the msg
        );
        
        // Read requests to replicas
        Cancellable read_reqs_timer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(2, TimeUnit.SECONDS), // When to start
                Duration.create(3, TimeUnit.SECONDS), // Delay between msgs
                null,             // TODO!            // To who
                null,             // TODO!            // Msg to send
                getContext().system().dispatcher(),   // System dispatcher
                getSelf()                             // Aource of the msg
        );
    }
    
    @Override
    public Receive createReceive() {
        throw new UnsupportedOperationException("Not supported yet.");
        // return receiveBuilder().match(...).build();
    }
    
}

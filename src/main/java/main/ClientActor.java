package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
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
    private final Random RNG;
    private ActorRef target_replica;
    private int target_replica_id;
    
    public ClientActor(int id) {
        this.clientID = id;
        this.target_replica = null;  // ---
        this.target_replica_id = -1; // TODO ridondante, trovare modo per ricavare una dall'altra
        this.RNG = new Random();
    }

    static public Props props(int id) {
        return Props.create(ClientActor.class, () -> new ClientActor(id));
    }
    
    @Override
    public void preStart() {
        scheduleRequests();
    }
    
    private void scheduleRequests() {
        getContext().system().scheduler().scheduleWithFixedDelay(
            Duration.create(1, TimeUnit.SECONDS), // When to start
            Duration.create(1, TimeUnit.SECONDS), // Delay between msgs
            getSelf(),                            // To who
            new RequestTimer(),                   // Msg to send
            getContext().system().dispatcher(),   // System dispatcher
            getSelf()                             // Source of the msg
        );
    }
    
    private void onRequestTimer(RequestTimer req) {        
        simulateNwkDelay();
        
        if (this.RNG.nextBoolean())
            sendReadReq();
        else
            sendWriteReq();
    }
    
    private void simulateNwkDelay() {
        try {
            Thread.sleep(this.RNG.nextInt(main.QTOB.MAX_NWK_DELAY_MS));
        } catch (InterruptedException ex) {
            System.err.println("Could not simulate network delay");
        }
    }
    
    private void sendReadReq() {
        target_replica.tell(
            new ReadRequest(getSelf()),
            getSelf()
        );
        
        System.out.println("Client " + this.clientID + " read req to " + target_replica_id);
    }
    
    private void sendWriteReq() {
        target_replica.tell(
            new WriteRequest(getSelf(), this.RNG.nextInt(1000)),
            getSelf()
        );
        
        System.out.println("Client " + this.clientID + " write req to " + target_replica_id);
    }
    
    private void onReadResponse(ReadResponse res) {
        System.out.println("Client " + this.clientID + " read done " + res.value);
    }
    
    private void onViewChange(ViewChange msg) {
        // Choose a destination replica
        this.target_replica_id = RNG.nextInt(msg.peers.size());
        this.target_replica = msg.peers.get(target_replica_id);
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(ViewChange.class, this::onViewChange)
            .match(RequestTimer.class, this::onRequestTimer)
            .match(ReadResponse.class, this::onReadResponse)
            .build();
    }
    
    // ========================================================================= Message classes
    private static class RequestTimer implements Serializable { }
}

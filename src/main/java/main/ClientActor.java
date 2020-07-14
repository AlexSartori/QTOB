package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
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
    private final int clientID;
    private final Random RNG;
    private final List<ActorRef> replicas;
    private Integer target_replica_id;
    private final TimeoutManager read_req_timers;
    
    public ClientActor(int id) {
        this.clientID = id;
        this.replicas = new ArrayList<>();
        this.target_replica_id = null;
        this.RNG = new Random();
        this.read_req_timers = new TimeoutManager(this::onReadTimeout);
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
            Thread.sleep(this.RNG.nextInt(QTOB.MAX_NWK_DELAY_MS));
        } catch (InterruptedException ex) {
            System.err.println("Could not simulate network delay");
        }
    }
    
    private void sendReadReq() {
        this.replicas.get(this.target_replica_id).tell(
            new ReadRequest(getSelf()),
            getSelf()
        );
        
        read_req_timers.addTimer(QTOB.NWK_TIMEOUT_MS);
        System.out.println("Client " + this.clientID + " read req to " + target_replica_id);
    }
    
    private void sendWriteReq() {
        this.replicas.get(this.target_replica_id).tell(
            new WriteRequest(getSelf(), this.RNG.nextInt(1000)),
            getSelf()
        );
    }
    
    private void onReadTimeout() {
        chooseTargetReplica();
    }
    
    private void chooseTargetReplica() {
        this.target_replica_id = RNG.nextInt(this.replicas.size());
    }
    
    private void onReadResponse(ReadResponse res) {
        read_req_timers.cancelFirstTimer();
        System.out.println("Client " + this.clientID + " read done: " + res.value);
    }
    
    private void onViewChange(ViewChange msg) {
        for (ActorRef a : msg.view.peers)
            this.replicas.add(a);
        
        chooseTargetReplica();
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(ViewChange.class, this::onViewChange)
            .match(RequestTimer.class, this::onRequestTimer)
            .match(ReadResponse.class, this::onReadResponse)
            .build();
    }
    
    private static class RequestTimer implements Serializable { }
    
    private static class ReadTimeout implements Serializable { }
}

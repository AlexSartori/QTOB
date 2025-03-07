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
    private final TimeoutList read_req_timers;
    
    public ClientActor(int id) {
        this.clientID = id;
        this.replicas = new ArrayList<>();
        this.target_replica_id = null;
        this.RNG = new Random();
        this.read_req_timers = new TimeoutList(this::onReadTimeout, QTOB.NWK_TIMEOUT_MS);
    }

    static public Props props(int id) {
        return Props.create(ClientActor.class, () -> new ClientActor(id));
    }
    
    public void log(String msg) {
        System.out.println("[C" + clientID + "] " + msg);
    }
    
    private void scheduleRequests() {
        getContext().system().scheduler().scheduleWithFixedDelay(
            Duration.create(RNG.nextInt(4), TimeUnit.SECONDS), // When to start
            Duration.create(RNG.nextInt(4)+2, TimeUnit.SECONDS), // Delay between msgs
            getSelf(),                            // To who
            new RequestTimer(),                   // Msg to send
            getContext().system().dispatcher(),   // System dispatcher
            getSelf()                             // Source of the msg
        );
    }
    
    private void onRequestTimer(RequestTimer req) {        
        QTOB.simulateNwkDelay();
        
        if (this.RNG.nextBoolean())
            sendReadReq();
        else
            sendWriteReq();
    }
    
    private void sendReadReq() {
        System.out.println("Client " + clientID + " read req to " + target_replica_id);
        
        QTOB.simulateNwkDelay();
        this.replicas.get(this.target_replica_id).tell(
            new ReadRequest(getSelf()),
            getSelf()
        );
        
        read_req_timers.addTimer();
    }
    
    private void sendWriteReq() {
        if (QTOB.VERBOSE) log("Write req to " + target_replica_id);
        
        QTOB.simulateNwkDelay();
        this.replicas.get(this.target_replica_id).tell(
            new WriteRequest(getSelf(), this.RNG.nextInt(1000)),
            getSelf()
        );
    }
    
    private void onReadTimeout() {
        if (QTOB.VERBOSE) log("Timeout for replica " + target_replica_id);
        chooseTargetReplica();
    }
    
    private void chooseTargetReplica() {
        this.target_replica_id = RNG.nextInt(this.replicas.size());
    }
    
    private void onReadResponse(ReadResponse res) {
        read_req_timers.cancelFirstTimer();
        System.out.println("Client " + this.clientID + " read done " + res.value);
    }
    
    private void onInitializeGroup(InitializeGroup msg) {
        for (ActorRef a : msg.group)
            this.replicas.add(a);
        
        chooseTargetReplica();
        scheduleRequests();
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeGroup.class, this::onInitializeGroup)
            .match(RequestTimer.class, this::onRequestTimer)
            .match(ReadResponse.class, this::onReadResponse)
            .build();
    }
    
    private static class RequestTimer implements Serializable { }
}

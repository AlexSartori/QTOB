package main;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import main.Messages.*;
import scala.concurrent.duration.Duration;


/**
 *
 * @author alex
 */
public class QTOB {
    final static int N_CLIENTS = 4;
    final static int N_REPLICAS = 4;
    final static int MAX_NWK_DELAY_MS = 200;
    final static int NWK_TIMEOUT_MS = MAX_NWK_DELAY_MS + 100;
    final static int HEARTBEAT_DELAY_MS = 500;
    final static Random RNG = new Random();
    static ActorSystem akka;
    
    
    public static void main(String[] args) {
	// Create the actor system for the Quorum-based Total Order Broadcast
        akka = ActorSystem.create("QTOB");
        
        // Create actors
        List<ActorRef> clients = createClients();
        List<ActorRef> replicas = createReplicas();
		
        // Make everyone aware of the group
        initializeActors(replicas, clients);
        
        // Schedule random crashes
        scheduleCrashes(replicas);
        
        // Handle termination
        waitForKeypress();
        akka.terminate();
    }

    private static List<ActorRef> createClients() {
        List<ActorRef> clients = new ArrayList<>();
        
        for (int i=0; i < N_CLIENTS; i++)
            clients.add(akka.actorOf(ClientActor.props(i)));
        
        return clients;
    }

    private static List<ActorRef> createReplicas() {
        List<ActorRef> replicas = new ArrayList<>();
        
        for (int i=0; i < N_REPLICAS; i++)
            replicas.add(akka.actorOf(ReplicaActor.props(i, 0)));

        return replicas;
    }
    
    private static void initializeActors(List<ActorRef> replicas, List<ActorRef> clients) {
        InitializeGroup msg = new InitializeGroup(replicas);
        
        replicas.get(0).tell(msg, ActorRef.noSender());
        
        for (ActorRef r : clients)
            r.tell(msg, ActorRef.noSender());
    }

    private static void scheduleCrashes(List<ActorRef> replicas) {
        int how_many = RNG.nextInt(Math.floorDiv(N_REPLICAS, 2));
        System.out.println(how_many + " replica(s) will crash.");
        
        for (int i = 0; i < how_many; i++) {
            akka.scheduler().scheduleOnce(
                Duration.create(2 + RNG.nextInt(5), TimeUnit.SECONDS), // When
                replicas.get(i),     // To whom
                new CrashMsg(),      // Msg to send
                akka.dispatcher(),   // System dispatcher
                ActorRef.noSender()  // Source of the msg
            );
        }
    }

    public static void waitForKeypress() {
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ioe) {
            System.out.println("Exiting...");
        }
    }
    
    public static void simulateNwkDelay() {
        try {
            Thread.sleep(RNG.nextInt(MAX_NWK_DELAY_MS));
        } catch (InterruptedException ex) {
            System.err.println("Could not simulate network delay");
        }
    }
}

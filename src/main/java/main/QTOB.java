package main;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import main.Messages.*;
import scala.concurrent.duration.Duration;


/**
 *
 * @author alex
 */
public class QTOB {
    final static boolean VERBOSE = true;
    final static int N_CLIENTS = 4;
    final static int N_REPLICAS = 6;
    final static int MAX_NWK_DELAY_MS = 100;
    final static int NWK_TIMEOUT_MS = MAX_NWK_DELAY_MS*2*N_REPLICAS + 200;
    final static int HEARTBEAT_DELAY_MS = 700;
    final static int HEARTBEAT_TIMEOUT_MS = HEARTBEAT_DELAY_MS + MAX_NWK_DELAY_MS*2*N_REPLICAS + 200;
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
        
        for (ActorRef r : replicas)
            r.tell(msg, ActorRef.noSender());
        
        for (ActorRef r : clients)
            r.tell(msg, ActorRef.noSender());
    }

    private static void scheduleCrashes(List<ActorRef> replicas) {
        int[] who  = new int[] {}; // RNG.nextInt(Math.floorDiv(N_REPLICAS, 2));
        int[] when = new int[] {5, 10}; // 2 + RNG.nextInt(5);
        System.out.println(who.length + " replica(s) will crash.");
        
        for (int i = 0; i < who.length; i++) {
            akka.scheduler().scheduleOnce(
                Duration.create(when[i], TimeUnit.SECONDS), // When
                replicas.get(who[i]), // To whom
                new CrashMsg(),       // Msg to send
                akka.dispatcher(),    // System dispatcher
                ActorRef.noSender()   // Source of the msg
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

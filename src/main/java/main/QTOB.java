package main;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Random;
import main.Messages.*;


/**
 *
 * @author alex
 */
public class QTOB {
    final static boolean VERBOSE = true;
    final static int N_CLIENTS = 3;
    final static int N_REPLICAS = 7;
    final static int MAX_NWK_DELAY_MS = 50;
    
    final static int NWK_TIMEOUT_MS = 500*N_REPLICAS + 200;
    final static int HEARTBEAT_DELAY_MS = 200;
    final static int HEARTBEAT_TIMEOUT_MS = 500*N_REPLICAS + 200;
    final static int ELECTION_TIMEOUT = 700*N_REPLICAS;
    
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
        scheduleCrashes();
        
        // Handle termination
        waitForKeypress();
        akka.terminate();
        System.exit(0);
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

    private static void scheduleCrashes() {
        CrashHandler ch = CrashHandler.getInstance();
        
        ch.scheduleCrash(6, CrashHandler.Situation.ON_ELECTION_ACK_SND);
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

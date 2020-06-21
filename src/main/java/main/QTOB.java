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
    final static int N_CLIENTS = 4;
    final static int N_REPLICAS = 4;
    final static int MAX_NWK_DELAY_MS = 200;
    final static int CRASH_TIMEOUT_MS = MAX_NWK_DELAY_MS + 100;
    final static Random rng = new Random();
    
    
    public static void main(String[] args) {
	// Create the actor system for the Quorum-based Total Order Broadcast
        final ActorSystem akka = ActorSystem.create("QTOB");
        
        // Create client actors
        List<ActorRef> clients = new ArrayList<>();
        for (int i=0; i < N_CLIENTS; i++)
            clients.add(akka.actorOf(ClientActor.props(i)));
        
        // Create replica actors
        List<ActorRef> replicas = new ArrayList<>();
        for (int i=0; i < N_REPLICAS; i++)
            replicas.add(akka.actorOf(ReplicaActor.props(i, 0)));

		
        // Make everyone aware of the group
        View msg = new View(0, replicas);
        for (ActorRef r : replicas)
            r.tell(msg, ActorRef.noSender());
        
        for (ActorRef c : clients)
            c.tell(msg, ActorRef.noSender());
        
        
    
        // Schedule random crashes
        int how_many = rng.nextInt(N_REPLICAS);
        
        for (int i = 0; i < how_many; i++) {
            akka.scheduler().scheduleOnce(
                Duration.create(2 + rng.nextInt(5), TimeUnit.SECONDS), // When
                replicas.get(i),     // To who
                new CrashMsg(),      // Msg to send
                akka.dispatcher(),   // System dispatcher
                ActorRef.noSender()  // Source of the msg
            );
        }
        
        // Handle termination
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ioe) {
            System.out.println("Exiting...");
        }
        akka.terminate();
    }
    
}

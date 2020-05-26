package main;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import main.Messages.*;


/**
 *
 * @author alex
 */
public class QTOB {
    final static int N_CLIENTS = 10;
    final static int N_REPLICAS = 10;
    final static int MAX_NWK_DELAY_MS = 200;
    final static int CRASH_TIMEOUT_MS = MAX_NWK_DELAY_MS + 100;
    
    
    public static void main(String[] args) {
        final ActorSystem akka = ActorSystem.create("QTOB");
        
        // Create client actors
        List<ActorRef> clients = new ArrayList<>();
        for (int i=0; i < N_CLIENTS; i++)
            clients.add(akka.actorOf(ClientActor.props(i)));
        
        // Create replica actors
        List<ActorRef> replicas = new ArrayList<>();
        for (int i=0; i < N_REPLICAS; i++)
            replicas.add(akka.actorOf(ReplicaActor.props(null, i, 0)));
        
        
        
        // Make everyone aware of the group
        JoinGroupMsg msg = new Messages.JoinGroupMsg(replicas);
        for (ActorRef r : replicas)
            r.tell(msg, ActorRef.noSender());
        for (ActorRef c : clients)
            c.tell(msg, ActorRef.noSender());
        
        
        
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

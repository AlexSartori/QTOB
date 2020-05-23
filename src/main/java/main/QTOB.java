package main;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;


/**
 *
 * @author alex
 */
public class QTOB {
    final static int N_CLIENTS = 10;
    final static int N_REPLICAS = 10;
    final static int MAX_NWK_DELAY_MS = 200;
    final static int CRASH_TIMEOUT_MS = MAX_NWK_DELAY_MS + 50;
    
    
    public static void main(String[] args) {
        final ActorSystem akka = ActorSystem.create("QTOB");
        
        List<ActorRef> clients = new ArrayList<>();
        for (int i=0; i < N_CLIENTS; i++) {
            // Add clients to ArrayList
        }
        
        List<ActorRef> replicas = new ArrayList<>();
        for (int i=0; i < N_CLIENTS; i++) {
            // Add replicas to ArrayList
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

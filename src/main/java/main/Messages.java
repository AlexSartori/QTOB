package main;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author alex
 */
public class Messages {    
    public static class View implements Serializable {
        public int viewID;
        public List<ActorRef> peers;

        public View(int id, List<ActorRef> peers) {
            this.viewID = id;
            this.peers = Collections.unmodifiableList(peers);
        }
    }
    
    public static class Flush implements Serializable {
        public final int id;
        
        public Flush(int id) {
            this.id = id;
        }
    }
      
    public static class Election implements Serializable {
        public final List<Integer> IDs;
        
        public Election(List<Integer> ids) {
            this.IDs = Collections.unmodifiableList(ids);
        }
    }
    
    public static class Coordinator implements Serializable {
        public final List<Integer> IDs;
        
        public Coordinator(List<Integer> ids) {
            this.IDs = Collections.unmodifiableList(ids);
        }
    }
    
    public static class ReadRequest implements Serializable {
        public final ActorRef client;	// the client asking to read
        
        public ReadRequest(ActorRef client) {
            this.client = client;
        }
    }
    
    public static class ReadResponse implements Serializable {
        public final int value;	  // the value requested for reading
        
        public ReadResponse(int v) {
            this.value = v;
        }
    }
    
    public static class WriteRequest implements Serializable {
        public final ActorRef client;   // the client asking to write
        public final int new_value;
        
        public WriteRequest(ActorRef client, int new_value) {
            this.client = client;
            this.new_value = new_value;
        }
    }
      
    public static class UpdateMsg implements Serializable {
        public final Update u;
        
        public UpdateMsg(Update u) {
            this.u = u;
        }
    }
    
    public static class Ack implements Serializable {
        public final Update u;
        
        public Ack(Update u) {
            this.u = u;
        }
    }
    
    public static class WriteOk implements Serializable {
        public final Update u;
        
        public WriteOk(Update u) {
            this.u = u;
        }
    }
    
    public static class CrashMsg { }
}

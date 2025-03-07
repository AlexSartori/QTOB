package main;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 * @author alex
 */
public class Messages {    
    public static class InitializeGroup implements Serializable {
        public final List<ActorRef> group;
        
        public InitializeGroup(List<ActorRef> group) {
            this.group = group;
        }
    }
      
    public static class Election implements Serializable {
        public final Map<Integer, Update> most_recent_updates;
        
        public Election(Map<Integer, Update> most_recent) {
            this.most_recent_updates = Collections.unmodifiableMap(most_recent);
        }
    }
    
    public static class ElectionAck implements Serializable {
        public final int from;
        
        public ElectionAck(int id) {
            this.from = id;
        }
    }
    
    public static class Synchronize implements Serializable {
        public final UpdateList updates;
        
        public Synchronize(UpdateList updates) {
            this.updates = updates.duplicate();
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
    
    public static class UpdateAck implements Serializable {
        public final Update u;
        
        public UpdateAck(Update u) {
            this.u = u;
        }
    }
    
    public static class WriteOk implements Serializable {
        public final Update u;
        
        public WriteOk(Update u) {
            this.u = u;
        }
    }
    
    public static class CrashMsg implements Serializable { }
    
    public static class Heartbeat implements Serializable { }
    
    public static class HeartbeatReminder implements Serializable { }
}

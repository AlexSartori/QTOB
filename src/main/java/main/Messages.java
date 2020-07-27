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
    public static class InitializeGroup implements Serializable {
        public final List<ActorRef> group;
        
        public InitializeGroup(List<ActorRef> group) {
            this.group = group;
        }
    }
      
    public static class Election implements Serializable {
        public final List<Integer> IDs;
        public final UpdateList most_recent_updates; // TODO must be a list!
        public final int most_recent_update_owner;
        
        public Election(List<Integer> ids, UpdateList most_recent, int update_owner) {
            this.IDs = Collections.unmodifiableList(ids);
            this.most_recent_updates = most_recent.duplicate();
            this.most_recent_update_owner = update_owner;
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

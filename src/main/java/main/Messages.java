package main;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author alex
 */
public class Messages {
    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group;  // an array of group members
        
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
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
        public final ActorRef client;
        public final int new_value;
        
        public WriteRequest(ActorRef client, int new_value) {
            this.client = client;
            this.new_value = new_value;
        }
    }
    
    public static class WriteResponse implements Serializable { }
}

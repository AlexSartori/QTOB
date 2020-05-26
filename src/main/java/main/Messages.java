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
        public final List<ActorRef> group;
        
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }
      
    public static class ReadRequest implements Serializable {
        private ActorRef client;
        
        public ReadRequest(ActorRef client) {
            this.client = client;
        }
    }
    
    public static class WriteRequest implements Serializable {
        private ActorRef client;
        private int new_value;
        
        public WriteRequest(ActorRef client, int new_value) {
            this.client = client;
            this.new_value = new_value;
        }
    }
}

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
}

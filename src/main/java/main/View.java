package main;

import akka.actor.ActorRef;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author alex
 */
public class View {
    public int viewID;
    public List<ActorRef> peers;

    public View(int id, List<ActorRef> peers) {
        this.viewID = id;
        this.peers = Collections.unmodifiableList(peers);
    }
}

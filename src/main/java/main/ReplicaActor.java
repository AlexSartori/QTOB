package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author alex
 */
public class ReplicaActor extends AbstractActor {
	
	private final int replicaID;
	// sarà final?
	private int v;
	private final ActorRef coordinator;
	
	private final Map<Integer, Update> Updatehistory;
	
	/*-- Actor constructors --------------------------------------------------- */
	public ReplicaActor(ActorRef coordinator, int ID, int value) {
		this.replicaID = ID;
		this.v = value;
		this.coordinator = coordinator;
		this.Updatehistory = new HashMap<>();
	}
	
	static public Props props(ActorRef coordinator, int ID, int value) {
		return Props.create(ReplicaActor.class, () -> new ReplicaActor(coordinator, ID, value));
	}
	
	
	
	@Override
    public Receive createReceive() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}

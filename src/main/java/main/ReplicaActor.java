package main;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import main.Messages.*;

/**
 *
 * @author alex
 */
public class ReplicaActor extends AbstractActor {
	private final int replicaID;
    private int v;
	
    private final List<ActorRef> peers;
    private ActorRef coordinator;
	
	private boolean electing;	// election in progress

    private final Map<Integer, Update> UpdatesHistory;

    /*-- Actor constructors --------------------------------------------------- */
	
    public ReplicaActor(int ID, int value) {
            this.replicaID = ID;
            this.peers = new ArrayList<>();
            this.v = value;
            this.coordinator = null;
            this.UpdatesHistory = new HashMap<>();
			this.electing = true;	// at the beginning, there's no coordinator
    }

    static public Props props(int ID, int value) {
            return Props.create(ReplicaActor.class, () -> new ReplicaActor(ID, value));
    }

	/*-- Actor logic ---------------------------------------------------------- */

	@Override
	public void preStart() {
		if (this.electing) {
			System.out.println("replicas now: " + this.peers.size());
			// election, but you don't have the peers yet
		}
	}
	
	private void onJoinGroup(JoinGroupMsg msg) {
        for (ActorRef r : msg.group) {
            if (!r.equals(getSelf())) {
                this.peers.add(r);
			}
		}
    }
    
    private void onReadRequest(ReadRequest req) {
        while (!this.electing) {
			req.client.tell(
	            new ReadResponse(this.v),
	            getSelf()
	        );
		}
    }
    
    private void onWriteRequest(WriteRequest req) {
        while (!this.electing) {
			req.client.tell(
	            new WriteResponse(),
	            getSelf()
	        );
	        System.out.println("TODO: Actually handle write request");
	    }
	}

	private void onElection (Election election) {
		if (election.predecessorID==this.replicaID) {
			
		}
	}
	
	private void onSynchronization (Synchronization synch) {
		
	}
	
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroup)
            .match(ReadRequest.class, this::onReadRequest)
            .match(WriteRequest.class, this::onWriteRequest)
			.match(Election.class, this::onElection)
			.match(Synchronization.class, this::onSynchronization)
            .build();
    }
    
    // ========================================================================= Message classes
}

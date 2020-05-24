package main;
import akka.japi.Pair;

/**
 *
 * @author Mask
 */
public class Update {
	
	private final Pair<Integer, Integer> pair;
	
	public Update(int epoch, int seqno) {
		this.pair = new Pair<>(epoch, seqno);
	}
}

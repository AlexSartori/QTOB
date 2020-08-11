package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author alex
 */
public class CrashHandler {
    private static final CrashHandler _instance = new CrashHandler();
    
    public enum Situation {
        ON_INIT,
        ON_READ_REQ,
        ON_WRITE_REQ,
        ON_UPDATE_MSG_SND,
        ON_UPDATE_MSG_RCV,
        ON_UPDATE_ACK_SND,
        ON_UPDATE_ACK_RCV,
        ON_WRITE_OK_SND,
        ON_WRITE_OK_RCV,
        ON_HEARTBEAT,
        ON_NEW_COORD,
        ON_COORD_CRASH,
        ON_BEGIN_ELECTION,
        ON_ELECTION_MSG_SND,
        ON_ELECTION_MSG_RCV,
        ON_ELECTION_ACK_SND,
        ON_ELECTION_ACK_RCV,
        ON_SYNCH_SND,
        ON_SYNCH_RCV
    }
    
    private final Map<Integer, List<Situation>> crashes;
    
    private CrashHandler() {
        crashes = new HashMap<>();
    }
    
    public static CrashHandler getInstance() {
        return _instance;
    }
    
    public void scheduleCrash(int replicaID, Situation s) {
        if (!crashes.containsKey(replicaID))
            crashes.put(replicaID, new ArrayList<>());
        
        crashes.get(replicaID).add(s);
    }
    
    public boolean shouldCrash(int replicaID, Situation s) {
        if (!crashes.containsKey(replicaID))
            return false;
        if (!crashes.get(replicaID).contains(s))
            return false;
        
        if (QTOB.VERBOSE) System.out.println("[CH] replica " + replicaID + " reached situation " + s.toString());
        return true;
    }
}

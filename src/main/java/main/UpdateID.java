package main;

/**
 *
 * @author alex
 */
public class UpdateID {
    public final int epoch;
    public final int seqNo;
    
    public UpdateID(int e, int s) {
        this.epoch = e;
        this.seqNo = s;
    }
    
    @Override
    public boolean equals(Object b) {
        UpdateID u = (UpdateID)b;
        return epoch == u.epoch && seqNo == u.seqNo;
    }
}

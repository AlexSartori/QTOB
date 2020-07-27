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

    public boolean happensBefore(UpdateID other) {
        if (this.epoch < other.epoch)
            return true;
        
        if (this.epoch == other.epoch && this.seqNo < other.seqNo)
            return true;
        
        return false;
    }
    
    public boolean happensAfter(UpdateID other) {
        return !this.equals(other) && !this.happensBefore(other);
    }
    
    @Override
    public String toString() {
        return epoch + ":" + seqNo;
    }
}

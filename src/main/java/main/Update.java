package main;

/**
 *
 * @author Mask
 */
public class Update {
    public final UpdateID id;
    public final int value;
    
    public Update(UpdateID id, int v) {
        this.id = id;
        this.value = v;
    }
    
    public boolean happensBefore(Update other) {
        return id.happensBefore(other.id);
    }
    
    public boolean happensAfter(Update other) {
        return id.happensAfter(other.id);
    }
}

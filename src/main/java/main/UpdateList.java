package main;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alex
 */
public class UpdateList {
    List<Update> list;
    
    public UpdateList() {
        list = new ArrayList<>();
    }
    
    public UpdateList(List<Update> u) {
        list = new ArrayList<>(u);
    }
    
    public void add(Update u) {
        for (int i = 0; i < list.size(); i++) {
            if (u.id.happensBefore(list.get(i).id)) {
                list.add(i, u);
                break;
            }
        }
    }
    
    public Update remove(Update u) {
        return remove(u.id);
    }
    
    public Update remove(UpdateID id) {
        for (int i = 0; i < list.size(); i++)
            if (list.get(i).id == id)
                return list.remove(i);
        return null;
    }
    
    public int size() {
        return list.size();
    }
    
    public boolean isEmpty() {
        return list.isEmpty();
    }
    
    public Update getMostRecent() {
        if (list.isEmpty())
            return null;
        return list.get(0);
    }
    
    public UpdateList duplicate() {
        return new UpdateList();
    }
    
    @Override
    public String toString() {
        String s = "{";
        for (Update u : list)
            s += u.id + ", ";
        return s + "}";
    }
}

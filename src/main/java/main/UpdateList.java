package main;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author alex
 */
public class UpdateList implements Iterable<Update> {
    List<Update> list;
    
    public UpdateList() {
        list = new ArrayList<>();
    }
    
    public UpdateList(List<Update> u) {
        list = new ArrayList<>(u);
    }
    
    public void add(Update u) {
        if (list.contains(u))
            return;
        
        int idx = 0;
        while (idx < list.size() && list.get(idx).id.happensBefore(u.id))
            idx++;
        list.add(idx, u);
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
    
    public boolean contains(Update u) {
        return list.contains(u);
    }
    
    public Update getMostRecent() {
        if (list.isEmpty())
            return null;
        return list.get(list.size()-1);
    }
    
    public UpdateList duplicate() {
        return new UpdateList(list);
    }
    
    @Override
    public String toString() {
        String s = "{";
        for (Update u : list)
            s += u + ", ";
        return s + "}";
    }

    @Override
    public Iterator<Update> iterator() {
        return list.iterator();
    }
}

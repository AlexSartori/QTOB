package main;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

/**
 *
 * @author alex
 */
public class TimeoutMap<T> {
    private final Consumer<T> callback;
    private final int delay_ms;
    private final Map<T, Timer> timers;
    
    public TimeoutMap(Consumer<T> callback, int delay_ms) {
        this.callback = callback;
        this.delay_ms = delay_ms;
        this.timers = new HashMap<>();
    }
    
    public void addTimer(T key) {
        Timer t = new Timer();
        t.schedule(new TimerTask() {
            @Override
            public void run() {
                callback.accept(key);
            }
        }, this.delay_ms);
        this.timers.put(key, t);
    }
    
    public void cancelTimer(T key) {
        Timer t = this.timers.remove(key);
        t.cancel();
    }
    
    public boolean containsKey(T key) {
        return timers.containsKey(key);
    }
    
    public void cancelAll() {
        for (Timer t : timers.values())
            t.cancel();
    }
}

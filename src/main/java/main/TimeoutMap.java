package main;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 * @author alex
 */
public class TimeoutMap<T> {
    private final Runnable callback;
    private final int delay_ms;
    private final Map<T, Timer> timers;
    
    public TimeoutMap(Runnable callback, int delay_ms) {
        this.callback = callback;
        this.delay_ms = delay_ms;
        this.timers = new HashMap<>();
    }
    
    public void addTimer(T key) {
        Timer t = new Timer();
        t.schedule(new TimerTask() {
            @Override
            public void run() {
                callback.run();
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
}

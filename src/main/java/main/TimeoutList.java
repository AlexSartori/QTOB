package main;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 * @author alex
 */
public class TimeoutList {
    private final Runnable callback;
    private final List<Timer> timers;
    private final int delay_ms;
    
    public TimeoutList(Runnable callback, int delay_ms) {
        this.callback = callback;
        timers = new ArrayList<>();
        this.delay_ms = delay_ms;
    }
    
    private void preCallback() {
        this.timers.remove(0);
        this.callback.run();
    }
    
    public void addTimer() {
        Timer t = new Timer();
        t.schedule(new TimerTask() {
            @Override
            public void run() {
                preCallback();
            }
        }, delay_ms);
        this.timers.add(t);
    }
    
    public void cancelFirstTimer() {
        this.timers.get(0).cancel();
        this.timers.remove(0);
    }
    
    public void cancelAll() {
        while (!this.timers.isEmpty())
            cancelFirstTimer();
    }
    
    public int size() {
        return this.timers.size();
    }
}

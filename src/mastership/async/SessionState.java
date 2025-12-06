package mastership.async;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;

import java.util.function.Consumer;

public class SessionState implements Watcher {
    private final Logger log;

    /**
     * connected and expired are volatile because they are:
     * • Written from the ZooKeeper event thread (inside process(WatchedEvent e)), and
     * • Read from the main thread (inside isConnected(), isExpired(), and the loops in main).
     * <p>
     * If connected and expired were plain booleans:
     * • The main thread might keep seeing the old value cached in a register or CPU cache.
     * • It might spin forever in the while (!m.isConnected()) loop even though the event thread has already set connected = true.
     * • You would get weird “hangs” that only appear under some timings or JVM optimisations.
     * <p>
     * This is a classic visibility problem in Java’s memory model.
     */
    private volatile boolean connected;
    private volatile boolean expired;

    private Consumer<Event.KeeperState> eventListener;

    public SessionState(Logger log) {
        this.log = log;
    }

    public SessionState(Logger log, Consumer<Event.KeeperState> eventListener) {
        this.log = log;
        this.eventListener = eventListener;
    }

    public void setEventListener(Consumer<Event.KeeperState> eventListener) {
        this.eventListener = eventListener;
    }

    @Override
    public void process(WatchedEvent event) {
        log.info("[Session Event]: {}", event);

        // ZooKeeper uses EventType.None for session state changes
        if (event.getType() != Event.EventType.None) {
            return;
        }

        // Default session handling logic
        switch (event.getState()) {
            case SyncConnected -> connected = true;
            case Disconnected -> connected = false;
            case Expired -> {
                expired = true;
                connected = false;
                log.error("[Session Event] Session expired");
            }
        }

        // Optional extra behaviour for Master/Worker etc.
        if (eventListener != null) {
            try {
                eventListener.accept(event.getState());
            } catch (Exception e) {
                log.warn("[Session Event] Listener threw exception", e);
            }
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isExpired() {
        return expired;
    }
}

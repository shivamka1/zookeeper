package zookeeper.masterworker;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class SessionState implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(SessionState.class);

    /**
     * {@code connected} and {@code expired} are {@code volatile} because they are:
     * <ul>
     *   <li>Written from the ZooKeeper event thread (inside {@code process(WatchedEvent e)})</li>
     *   <li>Read from the main thread (inside {@code isConnected()}, {@code isExpired()},
     *       and the loops in {@code main})</li>
     * </ul>
     *
     * <p>If connected and expired were plain booleans:</p>
     * <ul>
     *   <li>The main thread might keep seeing the old value cached in a register or CPU cache</li>
     *   <li>It might spin forever in the {@code while (!m.isConnected())} loop even though
     *       the event thread has already set {@code connected = true}</li>
     *   <li>You would get weird “hangs” that only appear under some timings or JVM optimisations</li>
     * </ul>
     *
     * <p>This is a classic visibility problem in Java’s memory model.</p>
     */
    private volatile boolean connected;
    private volatile boolean expired;

    private Consumer<Event.KeeperState> eventListener;

    public SessionState() {
    }

    public SessionState(Consumer<Event.KeeperState> eventListener) {
        this.eventListener = eventListener;
    }

    public void setEventListener(Consumer<Event.KeeperState> eventListener) {
        this.eventListener = eventListener;
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("[Session Event]: {}", event);

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
                LOG.error("[Session Event] Session expired");
            }
        }

        // Optional extra behaviour for Master/Worker etc.
        if (eventListener != null) {
            try {
                eventListener.accept(event.getState());
            } catch (Exception e) {
                LOG.warn("[Session Event] Listener threw exception", e);
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

package mastership.async;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Master implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    private final String connectString;
    private ZooKeeper zk;

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
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    private MasterBootstrap masterBootstrap;
    private MasterElection masterElection;

    public Master(String connectString) {
        this.connectString = connectString;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(connectString, 15000, this);
        masterBootstrap = new MasterBootstrap(zk);
        masterElection = new MasterElection(zk);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("Processing event: {}", event.toString());
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected -> connected = true;
                case Disconnected -> connected = false;
                case Expired -> {
                    expired = true;
                    connected = false;
                    LOG.error("Session expiration");
                }
            }
        }
    }

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    MasterState getMasterState() {
        return masterElection.getMasterState();
    }

    String getServerId() {
        return masterElection.getServerId();
    }

    static void main(String[] args) throws Exception {
        Master master = new Master(args[0]);

        master.startZk();

        while (!master.isConnected()) {
            Thread.sleep(100);
        }

        master.masterBootstrap.bootstrap();
        master.masterElection.runForMaster();

        while (!master.isExpired()) {
            Thread.sleep(1000);
        }

        master.stopZk();
    }
}

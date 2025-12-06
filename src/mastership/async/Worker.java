package mastership.async;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private ZooKeeper zk;
    private final String connectString;
    private final String serverId = IdGenerator.newId();

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    private Bootstrapper bootstrapper;

    Worker(String connectString) {
        this.connectString = connectString;
    }

    boolean isConnected() {
        return this.connected;
    }

    boolean isExpired() {
        return this.expired;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(this.connectString, 15000, this);
        bootstrapper = new Bootstrapper(zk);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    void bootstrap() {
        bootstrapper.createPersistent("/assign/worker-" + serverId);
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("[{}] processing event: {}", getClass().getSimpleName(), event.toString());
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


    static void main(String[] args) throws Exception {
        Worker worker = new Worker(args[0]);

        worker.startZk();

        while (!worker.isConnected()) {
            Thread.sleep(1000);
        }

        worker.bootstrap();
    }
}

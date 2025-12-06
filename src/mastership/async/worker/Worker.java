package mastership.async.worker;

import mastership.async.Bootstrapper;
import mastership.async.IdGenerator;
import mastership.async.SessionState;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;

public class Worker {
    private ZooKeeper zk;
    private final String connectString;
    private final String serverId = IdGenerator.newId();

    private Bootstrapper bootstrapper;
    private final SessionState sessionState = new SessionState();

    Worker(String connectString) {
        this.connectString = connectString;
    }

    boolean isConnected() {
        return sessionState.isConnected();
    }

    boolean isExpired() {
        return sessionState.isExpired();
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(this.connectString, 15000, sessionState);
        bootstrapper = new Bootstrapper(zk);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    void bootstrap() {
        bootstrapper.createPersistent("/assign/worker-" + serverId);
    }

    static void main(String[] args) throws Exception {
        Worker worker = new Worker(args[0]);

        worker.startZk();

        while (!worker.isConnected()) {
            Thread.sleep(1000);
        }

        worker.bootstrap();

        while (!worker.isExpired()) {
            Thread.sleep(1000);
        }

        worker.stopZk();
    }
}

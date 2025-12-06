package mastership.async;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Master {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    private final String connectString;
    private ZooKeeper zk;

    private Bootstrapper bootstrapper;
    private MasterElection masterElection;
    private final SessionState sessionState = new SessionState(LOG);

    public Master(String connectString) {
        this.connectString = connectString;
    }

    boolean isConnected() {
        return sessionState.isConnected();
    }

    boolean isExpired() {
        return sessionState.isExpired();
    }

    MasterState getMasterState() {
        return masterElection.getMasterState();
    }

    String getServerId() {
        return masterElection.getServerId();
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(connectString, 15000, sessionState);
        bootstrapper = new Bootstrapper(zk);
        masterElection = new MasterElection(zk);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    void bootstrap() {
        bootstrapper.createPersistent("/workers");
        bootstrapper.createPersistent("/assign");
        bootstrapper.createPersistent("/tasks");
        bootstrapper.createPersistent("/status");
    }

    static void main(String[] args) throws Exception {
        Master master = new Master(args[0]);

        master.startZk();

        while (!master.isConnected()) {
            Thread.sleep(100);
        }

        master.bootstrap();
        master.masterElection.runForMaster();

        while (!master.isExpired()) {
            Thread.sleep(1000);
        }

        master.stopZk();
    }
}

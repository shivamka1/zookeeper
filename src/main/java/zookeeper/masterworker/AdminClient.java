package zookeeper.masterworker;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

public class AdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(AdminClient.class);

    private ZooKeeper zk;
    private final String connectString;
    private final SessionState sessionState;

    AdminClient(String connectString) {
        this.connectString = connectString;
        this.sessionState = new SessionState();
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(connectString, 15000, sessionState);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    boolean isConnected() {
        return sessionState.isConnected();
    }

    boolean isExpired() {
        return sessionState.isExpired();
    }

    void listState() throws InterruptedException, KeeperException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startDate);
        } catch (KeeperException.NodeExistsException e) {
            LOG.error("No Master");
        }

        LOG.info("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            LOG.info("\t {}: {}", w, state);
        }

        LOG.info("Tasks:");
        for (String t : zk.getChildren("/assign", false)) {
            LOG.info("\t{}", t);
        }
    }

    static void main(String[] args) throws Exception {
        AdminClient adminClient = new AdminClient(args[0]);

        adminClient.startZk();

        while (!adminClient.isConnected()) {
            Thread.sleep(100);
        }

        adminClient.listState();

        while (!adminClient.isExpired()) {
            Thread.sleep(1000);
        }

        adminClient.stopZk();
    }
}

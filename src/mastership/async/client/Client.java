package mastership.async.client;

import mastership.async.SessionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Client {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private ZooKeeper zk;
    private final String connectString;
    private final SessionState sessionState;

    Client(String connectString) {
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

    String queueCommand(String command) throws Exception {
        while (true) {
            String name = null;
            try {
                name = zk.create(
                        "/task/task-",
                        command.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL
                );
                return name;
            } catch (KeeperException.NodeExistsException e) {
                throw new Exception(name + " already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {

            }
        }
    }

    static void main(String[] args) throws Exception {
        Client client = new Client(args[0]);

        client.startZk();

        while (!client.isConnected()) {
            Thread.sleep(100);
        }

        String name = client.queueCommand(args[1]);
        System.out.println("Created " + name);

        while (!client.isExpired()) {
            Thread.sleep(1000);
        }

        client.stopZk();
    }
}

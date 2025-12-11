package zookeeper.masterworker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class MasterSync implements Watcher {
    private final Random random = new Random(this.hashCode());

    private final String connectString;
    private ZooKeeper zk;
    private boolean isLeader;
    private final String serverId = Integer.toHexString(random.nextInt());

    public MasterSync(String connectString) {
        this.connectString = connectString;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(connectString, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    void runForMaster() throws InterruptedException, KeeperException {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch(KeeperException.ConnectionLossException _) {
            }
            if (checkMaster()) break;
        }
    }

    boolean checkMaster() throws InterruptedException, KeeperException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                return false;
            } catch(KeeperException.ConnectionLossException _) {
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    static void main(String[] args) throws Exception {
        MasterSync master = new MasterSync(args[0]);

        master.startZk();
        master.runForMaster();

        if (master.isLeader) {
            System.out.println("I'm the leader: " + master.serverId);
            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader");
        }

        master.stopZk();
    }
}

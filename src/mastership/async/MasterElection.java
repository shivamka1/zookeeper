package mastership.async;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class MasterElection {
    private static final Logger LOG = LoggerFactory.getLogger(MasterElection.class);
    private final Random random = new Random(this.hashCode());

    private final ZooKeeper zk;
    private final String serverId = Integer.toHexString(random.nextInt());
    private volatile MasterState state = MasterState.RUNNING;

    MasterElection(ZooKeeper zk) {
        this.zk = zk;
    }

    public MasterState getMasterState() {
        return state;
    }

    public String getServerId() {
        return serverId;
    }

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                if ("/master".equals(event.getPath())) {
                    runForMaster();
                }
            }
        }
    };

    private final AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> masterExists();
                case OK -> state = MasterState.NOT_ELECTED;
                case NONODE -> {
                    state = MasterState.RUNNING;
                    runForMaster();
                    LOG.info("Looks like previous master is gone. Let's run for master again.");
                }
                default -> checkMaster();
            }
        }
    };

    void masterExists() {
        zk.exists(
                "/master",
                masterExistsWatcher,
                masterExistsCallback,
                null
        );
    }

    private final AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> checkMaster();
                case NONODE -> {
                    state = MasterState.RUNNING;
                    runForMaster();
                }
                case OK -> {
                    if (serverId.equals(new String(data))) {
                        state = MasterState.ELECTED;
                    } else {
                        state = MasterState.NOT_ELECTED;
                        masterExists();
                    }
                }
                default -> LOG.error(
                        "Error when reading data.",
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    private final AsyncCallback.Create2Callback masterCreateCallback = new AsyncCallback.Create2Callback() {

        @Override
        public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> checkMaster();
                case OK -> state = MasterState.ELECTED;
                case NODEEXISTS -> {
                    state = MasterState.NOT_ELECTED;
                    masterExists();
                }
                default -> {
                    state = MasterState.NOT_ELECTED;
                    LOG.error(
                            "Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path)
                    );
                }
            }
            System.out.println("I'm" + (state == MasterState.ELECTED ? "" : "not") + " the leader");
        }
    };

    void runForMaster() {
        LOG.info("Running for master");
        zk.create(
                "/master",
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null
        );
    }
}

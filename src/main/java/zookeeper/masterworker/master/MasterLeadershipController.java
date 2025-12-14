package zookeeper.masterworker.master;

import zookeeper.masterworker.IdGenerator;
import zookeeper.masterworker.master.tasks.TasksWatcher;
import zookeeper.masterworker.master.tasks.WorkersWatcher;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterLeadershipController {
    private static final Logger LOG = LoggerFactory.getLogger(MasterLeadershipController.class);

    private final ZooKeeper zk;
    private final WorkersWatcher workersWatcher;
    private final TasksWatcher tasksWatcher;

    private final String serverId = IdGenerator.newId();

    private volatile MasterState state = MasterState.RUNNING;

    MasterLeadershipController(ZooKeeper zk, WorkersWatcher workersWatcher, TasksWatcher tasksWatcher) {
        this.zk = zk;
        this.workersWatcher = workersWatcher;
        this.tasksWatcher = tasksWatcher;
    }

    public MasterState getMasterState() {
        return state;
    }

    public String getServerId() {
        return serverId;
    }

    Watcher masterExistsWatcher = event -> {
        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            if ("/master".equals(event.getPath())) {
                runForMaster();
            }
        }
    };

    private final AsyncCallback.StatCallback masterExistsCallback =
            (rc, path, ctx, stat) -> {
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
            };

    void masterExists() {
        zk.exists(
                "/master",
                masterExistsWatcher,
                masterExistsCallback,
                null
        );
    }

    private final AsyncCallback.DataCallback masterCheckCallback =
            (rc, path, ctx, data, stat) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> checkMaster();
                    case NONODE -> {
                        state = MasterState.RUNNING;
                        runForMaster();
                    }
                    case OK -> {
                        if (serverId.equals(new String(data))) {
                            state = MasterState.ELECTED;
                            takeLeadership();
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
            };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    private final AsyncCallback.Create2Callback masterCreateCallback =
            (rc, path, ctx, name, stat) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> checkMaster();
                    case OK -> {
                        state = MasterState.ELECTED;
                        takeLeadership();
                    }
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
            };

    private void createMaster() {
        zk.create(
                "/master",
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null
        );
    }

    public void takeLeadership() {
        workersWatcher.startWatchingWorkers();
        tasksWatcher.startWatchingTasks();
    }

    public void runForMaster() {
        LOG.info("Running for master");
        createMaster();
    }
}

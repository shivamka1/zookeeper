package zookeeper.masterworker.worker;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zookeeper.masterworker.ChildrenCache;

import java.util.List;

public class AssignedTasksWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(AssignedTasksWatcher.class);

    private final ZooKeeper zk;
    private final String assignPath;
    private final TaskExecutor taskExecutor;

    private final ChildrenCache assignedTasksCache = new ChildrenCache();

    public AssignedTasksWatcher(ZooKeeper zk, String workerName, TaskExecutor taskExecutor) {
        this.zk = zk;
        this.assignPath = "/assign/" + workerName;
        this.taskExecutor = taskExecutor;
    }

    Watcher assignChildrenWatcher = new Watcher() {

        @Override
        public void process(WatchedEvent event) {
            if (
                    event.getType() == Event.EventType.NodeChildrenChanged &&
                            assignPath.equals(event.getPath())
            ) {
                getAssignments();
            }
        }
    };

    AsyncCallback.ChildrenCallback assignmentsCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getAssignments();
                case OK -> handleTasksUpdated(children);
                case NONODE -> LOG.warn(
                        "Assignment parent {} does not exist yet (bootstrap ordering issue?)",
                        assignPath
                );
                default -> LOG.error(
                        "getChildren({}) failed",
                        assignPath,
                        KeeperException.create(KeeperException.Code.get(rc), assignPath)
                );
            }
        }
    };

    private void handleTasksUpdated(List<String> tasks) {
        LOG.info("Updating assigned tasks list and checking for newly assigned tasks");
        List<String> newTasks = assignedTasksCache.refreshCacheAndGetAddedWorkersSinceLastUpdate(tasks);

        if (newTasks.isEmpty()) return;
        LOG.info("Detected {} new taskName(s): {} under {}", newTasks.size(), newTasks, assignPath);

        taskExecutor.executeTasks(newTasks);
    }

    private void getAssignments() {
        zk.getChildren(assignPath, assignChildrenWatcher, assignmentsCallback, null);
    }

    public void startWatching() {
        getAssignments();
    }
}

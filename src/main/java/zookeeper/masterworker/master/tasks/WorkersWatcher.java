package zookeeper.masterworker.master.tasks;

import zookeeper.masterworker.ChildrenCache;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WorkersWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(WorkersWatcher.class);

    private final ZooKeeper zk;
    private final TaskUnassignmentManager taskUnassignmentManager;

    private ChildrenCache workersCache = new ChildrenCache();

    public WorkersWatcher(ZooKeeper zk, TaskUnassignmentManager taskUnassignmentManager) {
        this.zk = zk;
        this.taskUnassignmentManager = taskUnassignmentManager;
    }

    // Re-registers watch on /workers whenever children change.
    private final Watcher workersChildrenWatcher = event -> {
        if (
                event.getType() == Watcher.Event.EventType.NodeChildrenChanged &&
                        "/workers".equals(event.getPath())
        ) {
            // The watch fires only once. We must call getWorkers() to re-register a new watch on /workers.
            // Without this, we would miss future worker join/leave events.
            getWorkers();
        }
    };

    private final AsyncCallback.ChildrenCallback workersChildrenCallback =
            (rc, path, ctx, children) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> getWorkers();
                    case OK -> {
                        LOG.info("Successfully got a list of workers: {} workers", children.size());
                        handleWorkersUpdated(children);
                    }
                    default -> LOG.error(
                            "getChildren(/workers) failed",
                            KeeperException.create(KeeperException.Code.get(rc), path)
                    );
                }
            };

    private void getWorkers() {
        zk.getChildren(
                "/workers",
                workersChildrenWatcher,
                workersChildrenCallback,
                null
        );
    }

    // Update cache and trigger taskName recovery for removed workers.
    private void handleWorkersUpdated(List<String> workers) {
        if (workersCache.isEmpty()) {
            workersCache = new ChildrenCache(workers);
            return;
        }

        LOG.info("Updating worker list and checking for removed workers");
        List<String> removedWorkers = workersCache.refreshCacheAndGetRemovedWorkersSinceLastUpdate(workers);

        if (removedWorkers.isEmpty()) return;

        LOG.info("Detected {} removed worker(s): {}", removedWorkers.size(), removedWorkers);
        taskUnassignmentManager.recoverTasks(removedWorkers);
    }

    // Returns the current cached view of workers.
    public List<String> getCurrentWorkers() {
        return workersCache.getList();
    }

    public void startWatchingWorkers() {
        getWorkers();
    }
}

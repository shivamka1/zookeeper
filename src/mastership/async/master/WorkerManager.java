package mastership.async.master;

import mastership.async.WorkersCache;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class WorkerManager {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerManager.class);

    private final ZooKeeper zk;
    private WorkersCache workersCache = new WorkersCache();

    WorkerManager(ZooKeeper zk) {
        this.zk = zk;
    }

    private final Watcher workersChildrenWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (
                    event.getType() == Event.EventType.NodeChildrenChanged &&
                            "/workers".equals(event.getPath())
            ) {
                // The watch fires only once. We must call getWorkers() to re-register a new watch on /workers.
                // Without this, we would miss future worker join/leave events.
                getWorkers();
            }
        }
    };

    private final AsyncCallback.ChildrenCallback workersChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getWorkers();
                case OK -> {
                    LOG.info("Successfully got a list of workers: {} workers", children.size());
                    recoverTasksAndRefreshCache(children);
                }
                default -> LOG.error(
                        "getChildren(/workers) failed",
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
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

    private void recoverTasksAndRefreshCache(List<String> workers) {
        if (workersCache.isEmpty()) {
            workersCache = new WorkersCache(workers);
            return;
        }

        LOG.info("Updating worker list and checking for removed workers");
        List<String> removedWorkers = workersCache.getRemovedSinceLastUpdateAndRefreshCache(workers);

        if (!removedWorkers.isEmpty()) {
            for (String worker : removedWorkers) {
                getAssignmentsForRemovedWorker(worker);
            }
        }
    }

    /*
     * When a worker dies, its tasks live under:
     *     /assign/<worker>/<task>
     *
     * We must recover these tasks.
     *
     * We DO NOT assign them directly to another worker here.
     * Instead:
     *
     *     1. Fetch the task data from /assign/<worker>/<task>.
     *     2. Recreate the task under /tasks/<task>.
     *     3. Delete the stale assignment.
     *
     * This ensures the Master detects the task under /tasks (via its watcher)
     * and performs assignment the normal way.
     *
     * This keeps assignment logic centralized and avoids distributed race conditions.
     */
    private final AsyncCallback.ChildrenCallback removedWorkerAssignmentsCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            String worker = (String) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getAssignmentsForRemovedWorker(worker);
                case OK -> {
                    LOG.info("Successfully got a list of assigments: {} tasks", children.size());
                    for (String task : children) {
                        getAssignedTaskData(path + "/" + task, task);
                    }
                }
                default -> LOG.error(
                        "getChildren({}) for absent worker failed",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void getAssignmentsForRemovedWorker(String worker) {
        zk.getChildren(
                "/assign/" + worker,
                false,
                removedWorkerAssignmentsCallback,
                worker
        );
    }

    private record RecreateTaskCtx(String path, String task, byte[] data) {
    }

    private final AsyncCallback.DataCallback assignedTaskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getAssignedTaskData(path, (String) ctx);
                case OK -> recreateTask(new RecreateTaskCtx(path, (String) ctx, data));
            }
        }
    };

    private void getAssignedTaskData(String path, String task) {
        zk.getData(
                path,
                false,
                assignedTaskDataCallback,
                task
        );
    }

    private final AsyncCallback.StringCallback taskRecreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            RecreateTaskCtx rctx = (RecreateTaskCtx) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> recreateTask(rctx);
                case OK -> deleteAssignment(rctx.path);
                case NODEEXISTS -> {
                    LOG.info("Task node already exists, retrying recreate: {}", path);
                    recreateTask(rctx);
                }
                default -> LOG.error(
                        "Error when recreating task",
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void recreateTask(RecreateTaskCtx ctx) {
        zk.create(
                "/task/" + ctx.task,
                ctx.data,
                OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                taskRecreateCallback,
                ctx
        );
    }

    private final AsyncCallback.VoidCallback deleteAssignmentCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> deleteAssignment(path);
                case OK -> LOG.info("Task correctly deleted from {}", path);
                default -> LOG.error(
                        "Failed to delete task assignment {}",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void deleteAssignment(String path) {
        zk.delete(path, -1, deleteAssignmentCallback, null);
    }
}

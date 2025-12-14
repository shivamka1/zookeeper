package zookeeper.masterworker.master.tasks;

import zookeeper.masterworker.ChildrenCache;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TasksWatcher {
    private final static Logger LOG = LoggerFactory.getLogger(TasksWatcher.class);

    private final ZooKeeper zk;
    private final TaskAssignmentManager taskAssignmentManager;

    /**
     * Explains why a {@link ChildrenCache} is used for tracking tasks.
     *
     * <p>Without a cache, every watcher notification under {@code /tasks} would
     * require re-running {@code assignTasks()} on the full list of tasks.</p>
     *
     * <p>This leads to several problems:
     * <ul>
     *   <li>All tasks would be reprocessed on every change, even if most are unchanged</li>
     *   <li>Tasks that are already in the process of being assigned could be churned repeatedly</li>
     *   <li>{@code assignTasks()} would need additional deduplication or idempotency logic</li>
     * </ul>
     *
     * <p>Using {@link ChildrenCache} allows the master to react only to newly added
     * or removed tasks, keeping task assignment stable and incremental.</p>
     */
    private final ChildrenCache tasksCache = new ChildrenCache();

    public TasksWatcher(ZooKeeper zk, TaskAssignmentManager taskAssignmentManager) {
        this.zk = zk;
        this.taskAssignmentManager = taskAssignmentManager;
    }

    private final Watcher tasksChildrenWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (
                    event.getType() == Event.EventType.NodeChildrenChanged &&
                            "/tasks".equals(event.getPath())
            ) {
                // The watch fires only once. We must call getTasks() to re-register a new watch on /tasks.
                // Without this, we would miss future tasks join/leave events.
                getTasks();
            }
        }
    };

    private final AsyncCallback.ChildrenCallback tasksChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getTasks();
                case OK -> {
                    LOG.info("Successfully got list of tasks: {}", children.size());
                    handleTasksUpdated(children);
                }
                default -> LOG.error(
                        "getChildren(/tasks) failed",
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    // Update cache and trigger taskName assignment to next available worker.
    private void handleTasksUpdated(List<String> tasks) {
        LOG.info("Updating tasks list and checking for new tasks");
        List<String> newTasks = tasksCache.refreshCacheAndGetAddedWorkersSinceLastUpdate(tasks);

        if (newTasks.isEmpty()) return;

        LOG.info("Detected {} new taskName(s): {}", newTasks.size(), newTasks);
        taskAssignmentManager.assignTasks(newTasks);
    }

    private void getTasks() {
        zk.getChildren(
                "/tasks",
                tasksChildrenWatcher,
                tasksChildrenCallback,
                null
        );
    }

    public void startWatchingTasks() {
        getTasks();
    }
}

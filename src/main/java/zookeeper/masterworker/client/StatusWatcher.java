package zookeeper.masterworker.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class StatusWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(StatusWatcher.class);

    private final ZooKeeper zk;

    private final ConcurrentHashMap<String, Task> ctxMap = new ConcurrentHashMap<>();

    public StatusWatcher(ZooKeeper zk) {
        this.zk = zk;
    }

    // Unlike TasksWatcher and WorkersWatcher, this watcher is intentionally short-lived.
    // We do not maintain a long-running watch on the status znode.
    // Instead, we use an exists() watch only to detect when the status znode appears.
    // Once it exists, we read it once (getData), complete the Task, and clean up.
    // The watch is re-registered only when retrying after CONNECTIONLOSS.
    Watcher statusWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent e) {
            if (e.getType() != Event.EventType.NodeCreated) return;

            String path = e.getPath();
            if (path == null) return;
            if (!path.startsWith("/status/task-")) return;

            Task task = ctxMap.get(path);
            if (task == null) return;

            readCompletedTaskStatus(path, task);
        }
    };

    AsyncCallback.StatCallback taskCompletedCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            Task task = (Task) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> watchStatus(path, task);
                case OK -> {
                    if (stat != null) {
                        LOG.info("Reading completed tasks {} status", path);
                        readCompletedTaskStatus(path, task);
                    }
                }
                case NONODE -> {
                    // Waiting for task to complete
                }
                default -> LOG.error(
                        "Error while checking if task {} completed",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    public void watchStatus(String path, Task task) {
        // IMPORTANT: Ordering matters here.
        // We must put the task into ctxMap *before* registering the watch.
        // Otherwise, a NodeCreated event could fire immediately after exists()
        // and before ctxMap.put(), leading to the watcher firing with no context.
        // This race is rare but possible due to ZooKeeper's asynchronous callbacks.
        ctxMap.put(path, task);
        zk.exists(
                path,
                statusWatcher,
                taskCompletedCallback,
                task
        );
    }

    AsyncCallback.DataCallback readCompletedTaskStatusCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            Task task = (Task) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> readCompletedTaskStatus(path, task);
                case OK -> {
                    String taskResult = new String(data);
                    if (ctxMap.remove(path, task)) {
                        task.complete(taskResult.contains("done"));
                        deleteStatus(path);
                    }
                }
                case NONODE -> {
                    // Task status node disappeared before we could read it.
                    // Ignore or re-watch depending on the semantics
                }
                default -> LOG.error(
                        "Failed to read completed task {} status",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void readCompletedTaskStatus(String path, Task task) {
        zk.getData(
                path,
                false,
                readCompletedTaskStatusCallback,
                task
        );
    }

    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> deleteStatus(path);
                case OK -> LOG.info("Completed task {} has been deleted", path);
                default -> LOG.error(
                        "Error deleting completed task {}",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    private void deleteStatus(String path) {
        zk.delete(path, -1, taskDeleteCallback, null);
    }
}

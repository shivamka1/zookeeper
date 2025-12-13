package zookeeper.masterworker.client;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class SubmitTask {
    private static final Logger LOG = LoggerFactory.getLogger(SubmitTask.class);

    private final ZooKeeper zk;
    private final StatusWatcher statusWatcher;

    public SubmitTask(ZooKeeper zk, StatusWatcher statusWatcher) {
        this.zk = zk;
        this.statusWatcher = statusWatcher;
    }

    private AsyncCallback.StringCallback createTaskCallback(
            String taskData,
            CompletableFuture<Task> future
    ) {
        return new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (future.isDone()) return;

                KeeperException.Code code = KeeperException.Code.get(rc);
                switch (code) {
                    case CONNECTIONLOSS -> {
                        // Retry must re-issue the zk.create request (new attempt),
                        // but complete the SAME caller-visible future.
                        submitAttempt(taskData, future);
                    }
                    case OK -> {
                        Task task = new Task(name, taskData);

                        String statusPath = name.replace("/tasks/", "/status/");
                        statusWatcher.watchStatus(statusPath, task);

                        future.complete(task);
                    }
                    case SESSIONEXPIRED -> {
                        KeeperException ex = KeeperException.create(code, path);
                        future.completeExceptionally(
                                new IllegalStateException("ZooKeeper session expired", ex)
                        );
                    }
                    default -> {
                        KeeperException ex = KeeperException.create(code, path);
                        LOG.error("Failed to submit task {}", taskData, ex);
                        future.completeExceptionally(ex);
                    }
                }
            }
        };
    }

    private void submitAttempt(String taskData, CompletableFuture<Task> future) {
        if (future.isDone()) return;

        zk.create(
                "/tasks/task-",
                taskData.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback(taskData, future),
                taskData
        );
    }

    // NOTE: With PERSISTENT_SEQUENTIAL, retrying on CONNECTIONLOSS can create a duplicate task
    // if the server actually created the znode but the client didn't receive the response.
    // This is acceptable only if tasks are idempotent or duplicates are tolerated.
    public CompletableFuture<Task> submit(String taskData) {
        CompletableFuture<Task> future = new CompletableFuture<>();
        submitAttempt(taskData, future);
        return future;
    }
}

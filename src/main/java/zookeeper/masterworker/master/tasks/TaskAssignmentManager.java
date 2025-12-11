package zookeeper.masterworker.master.tasks;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class TaskAssignmentManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskAssignmentManager.class);
    private final Random random = new Random();

    private final ZooKeeper zk;
    private final Supplier<List<String>> workersSupplier;

    public TaskAssignmentManager(ZooKeeper zk, Supplier<List<String>> workersSupplier) {
        this.zk = zk;
        this.workersSupplier = workersSupplier;
    }

    private final AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            String taskName = (String) ctx;

            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getTaskData(taskName);
                case OK -> assignTask(taskName, data);
                default -> LOG.error(
                        "Error when trying to get task data for {}",
                        taskName,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void getTaskData(String task) {
        zk.getData(
                "/tasks/" + task,
                false,
                taskDataCallback,
                task
        );
    }

    private final AsyncCallback.MultiCallback assignTaskCallback = new AsyncCallback.MultiCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
            TaskCtx taskCtx = (TaskCtx) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> assignTask(taskCtx.taskName(), taskCtx.taskData());
                case OK -> {
                    LOG.info(
                            "Task {} assigned atomically at {} and removed from /tasks",
                            taskCtx.taskName(),
                            taskCtx.path()
                    );
                }
                default -> LOG.error(
                        "Error when trying to assign task {} at {}",
                        taskCtx.taskName(),
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void assignTask(String taskName, byte[] taskData) {
        List<String> workers = workersSupplier.get();
        if (workers.isEmpty()) {
            LOG.warn("No workers available to assign task {}", taskName);
            return;
        }

        String designateWorker = workers.get(random.nextInt(workers.size()));
        String assignmentPath = "/assign/" + designateWorker + "/" + taskName;
        String taskPath = "/tasks/" + taskName;

        LOG.info(
                "Assigning task {} to worker {} at {}",
                taskName,
                designateWorker,
                assignmentPath
        );

        // The transaction builder provides a fluent interface
        zk.transaction()
                .create(
                        assignmentPath,
                        taskData,
                        OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT
                ).delete(
                        taskPath,
                        -1
                )
                // Commit asynchronously with callback and context
                .commit(
                        assignTaskCallback,
                        new TaskCtx(assignmentPath, taskName, taskData)
                );
    }

    public void assignTasks(List<String> newTasks) {
        for (String task : newTasks) {
            getTaskData(task);
        }
    }
}

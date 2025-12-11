package mastership.async.master.tasks;

import mastership.async.ChildrenCache;
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

    private final AsyncCallback.StringCallback assignTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            TaskCtx taskCtx = (TaskCtx) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> assignTask(taskCtx.task(), taskCtx.data());
                case OK -> {
                    LOG.info("Task {} assigned correctly at {}", taskCtx.task(), path);
                    deleteTask(taskCtx.task());
                }
                case NODEEXISTS -> {
                    LOG.warn("Task {} already assigned at {}", taskCtx.task(), path);
                    // Optionally: still try to delete the original /tasks/<task>
                    deleteTask(taskCtx.task());
                }
                default -> LOG.error(
                        "Error when trying to assign task {} at {}",
                        taskCtx.task(),
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

        LOG.info(
                "Assigning task {} to worker {} at {}",
                taskName,
                designateWorker,
                assignmentPath
        );

        zk.create(
                assignmentPath,
                taskData,
                OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                assignTaskCallback,
                new TaskCtx(taskName, taskData)
        );
    }

    private final AsyncCallback.VoidCallback deleteTaskCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> deleteTask((String) ctx);
                case OK -> LOG.info("Successfully deleted {}", path);
                case NONODE -> LOG.info("Task {} already deleted", path);
                default -> LOG.error(
                        "Failed to delete task node {}",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void deleteTask(String taskName) {
        zk.delete(
                "/tasks/" + taskName,
                -1,
                deleteTaskCallback,
                taskName
        );
    }

    public void assignTasks(List<String> newTasks) {
        for (String task : newTasks) {
            getTaskData(task);
        }
    }
}

package zookeeper.masterworker;

import zookeeper.masterworker.master.tasks.TaskCtx;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * IdempotentTaskAssignmentManager
 *
 * This manager assigns tasks using a simple three-step pattern:
 *
 *   1) read /tasks/<task> to get the task data
 *   2) create /assign/<worker>/<task> with that data
 *   3) delete /tasks/<task>
 *
 * These steps are issued as separate ZooKeeper operations, so they are
 * not atomic. This makes the design intentionally at-least-once rather
 * than exactly-once. The key point is that tasks are assumed to be
 * idempotent: executing the same task more than once must be safe.
 *
 * There are two interesting failure scenarios:
 *
 *   1) Connection loss around the create:
 *      - The create("/assign/...") may actually succeed on the server,
 *        but the client loses the connection before seeing the reply.
 *      - On retry, create() returns NODEEXISTS for the assignment znode.
 *      - In that case we log and still delete /tasks/<task>, completing
 *        the assignment as if the first create had succeeded.
 *
 *   2) Crash after create, before delete:
 *      - The manager successfully creates /assign/<worker>/<task>.
 *      - Before delete("/tasks/<task>") runs or completes, the process
 *        crashes, restarts, or loses its ZooKeeper session.
 *      - A new master later scans /tasks, still sees <task>, and assigns
 *        it again, potentially to a different worker.
 *      - As a result, the same logical task can be executed more than once.
 *
 * This class deliberately accepts scenario (2) and relies on
 * idempotent task processing. In other words, it implements the
 * following design choice:
 *
 *   1. Treat tasks as idempotent and allow at-least-once semantics:
 *        - Document that tasks may be retried or executed multiple times.
 *        - Ensure workers handle duplicates safely (for example, by
 *          deduplicating on a task identifier or making side effects
 *          naturally idempotent).
 *
 * For completeness, there are two other conceptual options which this
 * implementation does not take:
 *
 *   2. Use ZooKeeper multi() or Transaction:
 *        - Bundle “create /assign/<worker>/<task>” and
 *          “delete /tasks/<task>” into one atomic operation.
 *        - Either both succeed or neither does, which closes the window
 *          where a task is both in /assign and still visible in /tasks.
 *
 *   3. Add extra coordination or metadata:
 *        - For example, maintain a separate flag or znode that records
 *          which worker a task has been assigned to, and have new masters
 *          reconcile based on that state.
 *        - This reduces duplicate execution but is essentially a more
 *          complex way of achieving what an atomic multi() would provide.
 *
 * If you need strict exactly-once assignment, you should not use this
 * idempotent variant and should instead adopt a multi-based approach
 * such as the one used in the task reassignment path
 * (see TaskReassignmentManager).
 */
public class IdempotentTaskAssignmentManager {
    private static final Logger LOG = LoggerFactory.getLogger(IdempotentTaskAssignmentManager.class);
    private final Random random = new Random();

    private final ZooKeeper zk;
    private final Supplier<List<String>> workersSupplier;

    public IdempotentTaskAssignmentManager(ZooKeeper zk, Supplier<List<String>> workersSupplier) {
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
                case CONNECTIONLOSS -> assignTask(taskCtx.taskName(), taskCtx.taskData());
                case OK -> {
                    LOG.info("Task {} assigned correctly at {}", taskCtx.taskName(), path);
                    deleteTask(taskCtx.taskName());
                }
                case NODEEXISTS -> {
                    LOG.warn("Task {} already assigned at {}", taskCtx.taskName(), path);
                    // Optionally: still try to delete the original /tasks/<task>
                    deleteTask(taskCtx.taskName());
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
                new TaskCtx(assignmentPath, taskName, taskData)
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

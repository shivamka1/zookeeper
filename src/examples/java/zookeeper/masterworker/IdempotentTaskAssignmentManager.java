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
 * <p>This manager assigns tasks using a simple three-step pattern:</p>
 *
 * <ol>
 *   <li>Read {@code /tasks/<task>} to get the task data</li>
 *   <li>Create {@code /assign/<worker>/<task>} with that data</li>
 *   <li>Delete {@code /tasks/<task>}</li>
 * </ol>
 *
 * <p>These steps are issued as separate ZooKeeper operations, so they are
 * not atomic. This makes the design intentionally at-least-once rather
 * than exactly-once. The key point is that tasks are assumed to be
 * idempotent: executing the same task more than once must be safe.</p>
 *
 * <p>There are two interesting failure scenarios:</p>
 *
 * <ol>
 *   <li>
 *     Connection loss around the create:
 *     <ul>
 *       <li>The {@code create("/assign/...")} may actually succeed on the server,
 *           but the client loses the connection before seeing the reply.</li>
 *       <li>On retry, {@code create()} returns {@code NODEEXISTS} for the assignment znode.</li>
 *       <li>In that case we log and still delete {@code /tasks/<task>},
 *           completing the assignment as if the first create had succeeded.</li>
 *     </ul>
 *   </li>
 *
 *   <li>
 *     Crash after create, before delete:
 *     <ul>
 *       <li>The manager successfully creates {@code /assign/<worker>/<task>}.</li>
 *       <li>Before {@code delete("/tasks/<task>")} runs or completes, the process
 *           crashes, restarts, or loses its ZooKeeper session.</li>
 *       <li>A new master later scans {@code /tasks}, still sees {@code <task>},
 *           and assigns it again, potentially to a different worker.</li>
 *       <li>As a result, the same logical task can be executed more than once.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>This class deliberately accepts scenario (2) and relies on
 * idempotent task processing. In other words, it implements the
 * following design choice:</p>
 *
 * <ol>
 *   <li>
 *     Treat tasks as idempotent and allow at-least-once semantics:
 *     <ul>
 *       <li>Document that tasks may be retried or executed multiple times.</li>
 *       <li>Ensure workers handle duplicates safely (for example, by
 *           deduplicating on a task identifier or making side effects
 *           naturally idempotent).</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>For completeness, there are two other conceptual options which this
 * implementation does not take:</p>
 *
 * <ol>
 *   <li>
 *     Use ZooKeeper {@code multi()} or Transaction:
 *     <ul>
 *       <li>Bundle {@code create /assign/<worker>/<task>} and
 *           {@code delete /tasks/<task>} into one atomic operation.</li>
 *       <li>Either both succeed or neither does, which closes the window
 *           where a task is both in {@code /assign} and still visible in {@code /tasks}.</li>
 *     </ul>
 *   </li>
 *
 *   <li>
 *     Add extra coordination or metadata:
 *     <ul>
 *       <li>For example, maintain a separate flag or znode that records
 *           which worker a task has been assigned to, and have new masters
 *           reconcile based on that state.</li>
 *       <li>This reduces duplicate execution but is essentially a more
 *           complex way of achieving what an atomic {@code multi()} would provide.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>If you need strict exactly-once assignment, you should not use this
 * idempotent variant and should instead adopt a multi-based approach
 * such as the one used in the task reassignment path
 * (see {@link zookeeper.masterworker.master.tasks.TaskUnassignmentManager}).</p>
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

package zookeeper.masterworker.master.tasks;

import org.apache.zookeeper.*;
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

    private final AsyncCallback.DataCallback taskDataCallback =
            (rc, path, ctx, data, stat) -> {
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
            };

    private void getTaskData(String task) {
        zk.getData(
                "/tasks/" + task,
                false,
                taskDataCallback,
                task
        );
    }

    private final AsyncCallback.MultiCallback assignTaskCallback =
            (rc, path, ctx, opResults) -> {
                TaskCtx taskCtx = (TaskCtx) ctx;
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS ->
                        // NOTE ON CONNECTIONLOSS DURING TASK ASSIGNMENT
                        // ---------------------------------------------------------
                        //
                        // Assignment is done via a transaction:
                        //   1) create /assign/<worker>/<task>
                        //   2) delete /tasks/<task>
                        //
                        // If we get CONNECTIONLOSS, we do NOT know whether the transaction committed.
                        // Retrying blindly can lead to duplicate assignment:
                        //
                        //   - Attempt #1 might have committed (task removed from /tasks and assigned)
                        //   - Client sees CONNECTIONLOSS and retries
                        //   - Attempt #2 might assign again (possibly to a different worker)
                        //
                        // This is okay if tasks are idempotent.
                            assignTask(taskCtx.taskName(), taskCtx.taskData());
                    case OK -> LOG.info(
                            "Task {} assigned atomically at {} and removed from /tasks",
                            taskCtx.taskName(),
                            taskCtx.assignmentPath()
                    );
                    case NODEEXISTS ->
                        // NOTE ON NODEEXISTS DURING TASK ASSIGNMENT
                        // ----------------------------------------
                        //
                        // This callback runs on the MASTER side when assigning a task from /tasks to a worker.
                        // The transaction performed is:
                        //
                        //   1) create /assign/<worker>/<task>   (with the task payload)
                        //   2) delete /tasks/<task>
                        //
                        // At a high level, this is the “move” operation:
                        //   /tasks/<task>  →  /assign/<worker>/<task>
                        //
                        // When can NODEEXISTS happen?
                        // ---------------------------
                        // NODEEXISTS here refers to create(/assign/<worker>/<task>) failing because that znode
                        // already exists. There are two broad categories:
                        //
                        // 1) Benign retry after unknown commit (most common in ZooKeeper):
                        //    - The master attempted the transaction, but got CONNECTIONLOSS.
                        //    - The transaction may have actually COMMITTED on the server.
                        //    - On retry, create(/assign/<worker>/<task>) returns NODEEXISTS.
                        //
                        //    In this case, because the earlier transaction *committed atomically*:
                        //      - /assign/<worker>/<task> already exists
                        //      - /tasks/<task> is already deleted
                        //
                        //    So NODEEXISTS is not a bug by itself — it can simply mean
                        //    “we already assigned this task, but the client didn’t learn it”.
                        //
                        // 2) Real invariant violation / bug (should be rare in a correct master design):
                        //    NODEEXISTS can also indicate a logic error where we attempt to assign the same
                        //    task twice to the SAME worker path, for example due to:
                        //      - Duplicate triggering of assignment logic for the same task name
                        //          This is when your code calls assignTask(task-7) twice even though there was only one real task.
                        //
                        //          Common ways it happens:
                        //	       • You call assignTasks(children) repeatedly without diffing
                        //           Example: getChildren("/tasks", watch=...) fires, you get ["task-7"], you start assignment.
                        //           Then you also call assignTasks(...) again (maybe from a periodic poll / recovery path / reconnection path) and you again see ["task-7"] and start assignment again.
                        //         • You retry “the whole flow” on CONNECTIONLOSS at the wrong layer
                        //           If you retry from the outer layer (e.g., “re-run assignTasks(allChildren)”) instead of retrying just the specific getData(task-7) or the specific transaction, you can accidentally enqueue another assignment attempt for the same task.
                        //	       • You have two “entry points” that both trigger assignment
                        //           e.g. /tasks watcher triggers assignment and some “recovery scan” also triggers assignment, both running at the same time.
                        //
                        //      - Missing de-duplication in the master’s /tasks watcher handling
                        //          ZooKeeper’s children watch (getChildren(..., watch=true)) is:
                        //	      • one-shot (you must re-register after it fires), and
                        //	      • gives you a list of children, not “the one new child”.
                        //
                        //          So if you do:
                        //              getChildren("/tasks", watch=true, ... -> assignTasks(children))
                        //          and children contains task-7, you must avoid re-assigning task-7 if you already started assigning it earlier but the transaction hasn’t finished yet.
                        //
                        //          The classic fix is:
                        //	      • keep a Set<String> assigning (or inProgressAssignments)
                        //	      • before starting assignment for a task, do if (!assigning.add(taskName)) return;
                        //	      • remove from the set once you reach a terminal outcome (OK / confirmed assigned / confirmed gone)
                        //
                        //          Without that de-dupe set, every time you re-read children you’ll re-trigger assignment for tasks that are still present (because your “move” hasn’t completed yet).
                        //
                        //      - Two concurrent assignment attempts for the same taskName in the same leader
                        //          Even if you only have one master process, you can still have concurrency inside it:
                        //	      • ZooKeeper callbacks can run on the event thread, and you might hand work off to an executor.
                        //	      • You might have multiple callbacks in flight:
                        //	      • one from /tasks children watch
                        //	      • another from a retry path (CONNECTIONLOSS)
                        //	      • another from a “scan tasks on reconnect” routine
                        //
                        //          If those paths can overlap, you can end up with:
                        //	     1. Thread T1 starts assigning task-7 (reads data, starts multi-op).
                        //	     2. Before T1 finishes, thread T2 also starts assigning task-7 (because it saw /tasks again).
                        //	     3. Both attempt create(/assign/<worker>/task-7).
                        //
                        //          If they pick the same worker, you’ll get NODEEXISTS for the loser.
                        //          If they pick different workers, one will likely succeed and delete /tasks/task-7, while the other might get NONODE (or other failures depending on exact timing).
                        //
                        //          This is why “single master” doesn’t automatically mean “single attempt per task” — you still need a small amount of local synchronisation / de-duplication.
                        //
                        //    In a correct single-leader master implementation, there should be a single
                        //    “assignment authority”, so concurrent double-assignments should not happen.
                        //
                        // Important nuance (transaction semantics):
                        // ----------------------------------------
                        // Because assignment is performed via a TRANSACTION, NODEEXISTS on the create step
                        // means THIS attempt did not commit. That does NOT automatically tell us whether
                        // a previous attempt committed.
                        // Therefore, on NODEEXISTS we should treat the outcome as “task may already be assigned”,
                        // not “task is still in /tasks”.
                        //
                        // Handling policy:
                        // ----------------
                        // On NODEEXISTS, the safe and informative approach is:
                        //   - Verify whether /tasks/<task> still exists.
                        //       * If /tasks/<task> is NONODE → assignment already succeeded earlier (benign).
                        //       * If /tasks/<task> still exists → something is inconsistent (bug), because
                        //         create succeeded earlier but delete did not (which cannot happen if the
                        //         earlier operation was truly the same committed transaction).
                        //
                        // In other words: NODEEXISTS here is usually a retry artifact, but if /tasks/<task>
                        // still exists at the same time, it points to broken task lifecycle invariants.
                            LOG.error(
                                    "Invariant violation: task {} already exists under /assign/<worker> during assignment",
                                    taskCtx.taskName()
                            );
                    default -> LOG.error(
                            "Error when trying to assign task {} at {}",
                            taskCtx.taskName(),
                            path,
                            KeeperException.create(KeeperException.Code.get(rc), path)
                    );
                }
            };

    private void assignTask(String taskName, byte[] taskData) {
        List<String> workers = workersSupplier.get();
        if (workers.isEmpty()) {
            LOG.warn("No workers available to assign task {}", taskName);
            return;
        }

        String assignedWorker = workers.get(random.nextInt(workers.size()));
        String assignmentPath = "/assign/" + assignedWorker + "/" + taskName;
        String taskPath = "/tasks/" + taskName;

        LOG.info(
                "Assigning task {} to worker {} at {}",
                taskName,
                assignedWorker,
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
                        new TaskCtx(assignmentPath, assignedWorker, taskName, taskData)
                );
    }

    public void assignTasks(List<String> newTasks) {
        for (String task : newTasks) {
            getTaskData(task);
        }
    }
}

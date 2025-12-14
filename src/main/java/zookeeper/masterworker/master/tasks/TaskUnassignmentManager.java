package zookeeper.masterworker.master.tasks;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TaskUnassignmentManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskUnassignmentManager.class);

    private final ZooKeeper zk;

    public TaskUnassignmentManager(ZooKeeper zk) {
        this.zk = zk;
    }

    /**
     * When a worker dies, its tasks live under: {@code /assign/<worker>/<task>}
     *
     * <p>We must recover these tasks.</p>
     *
     * <p>We DO NOT assign them directly to another worker here.</p>
     *
     * <p>Instead:</p>
     *
     * <ol>
     *   <li>Fetch the task data from {@code /assign/<worker>/<task>}</li>
     *   <li>Recreate the task under {@code /tasks/<task>}</li>
     *   <li>Delete the stale assignment</li>
     * </ol>
     *
     * <p>This ensures the Master detects the task under {@code /tasks} (via its watcher)
     * and performs assignment the normal way.</p>
     *
     * <p>This keeps assignment logic centralized and avoids distributed race conditions.</p>
     */
    private final AsyncCallback.ChildrenCallback removedWorkerAssignmentsCallback =
            (rc, path, ctx, children) -> {
                String worker = (String) ctx;
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> getAssignmentsForRemovedWorker(worker);
                    case OK -> {
                        LOG.info("Successfully got a list of assignments: {} tasks", children.size());
                        if (children.isEmpty()) {
                            deleteAssignedWorker("/assign/" + worker);
                            return;
                        }
                        for (String task : children) {
                            TaskCtx taskCtx = new TaskCtx(path + "/" + task, worker, task);
                            getAssignedTaskData(taskCtx);
                        }
                    }
                    default -> LOG.error(
                            "getChildren({}) for absent worker failed",
                            path,
                            KeeperException.create(KeeperException.Code.get(rc), path)
                    );
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

    private final AsyncCallback.DataCallback assignedTaskDataCallback =
            (rc, path, ctx, data, stat) -> {
                TaskCtx taskCtx = (TaskCtx) ctx;
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> getAssignedTaskData(taskCtx);
                    case OK -> unassignTask(taskCtx.withTaskData(data));
                    default -> LOG.error(
                            "Error when getting assigned task data for {}",
                            ctx,
                            KeeperException.create(KeeperException.Code.get(rc), path)
                    );
                }
            };

    private void getAssignedTaskData(TaskCtx taskCtx) {
        zk.getData(
                taskCtx.assignmentPath(),
                false,
                assignedTaskDataCallback,
                taskCtx
        );
    }

    private final AsyncCallback.MultiCallback taskUnassignMultiCallback =
            (rc, path, ctx, opResults) -> {
                TaskCtx taskCtx = (TaskCtx) ctx;
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> unassignTask(taskCtx);
                    case OK -> {
                        LOG.info(
                                "Reassigned task {} from {} back to /tasks/{}",
                                taskCtx.taskName(),
                                taskCtx.assignmentPath(),
                                taskCtx.taskName()
                        );
                        // Try to delete the parent; if NOTEMPTY we’ll try again later.
                        String assignedWorkerPath = "/assign/" + taskCtx.assignedWorker();
                        deleteAssignedWorker(assignedWorkerPath);
                    }
                    case NODEEXISTS ->
                        // NOTE ON NODEEXISTS DURING TASK UNASSIGNMENT
                        // ------------------------------------------
                        //
                        // This callback runs on the MASTER side when recovering tasks from a
                        // *dead worker*. The transaction performed is:
                        //
                        //   1) create /tasks/<task>
                        //   2) delete /assign/<worker>/<task>
                        //
                        // In steady state, NODEEXISTS on create(/tasks/<task>) is *unexpected*,
                        // because at the moment of unassignment:
                        //
                        //   - The task was previously removed from /tasks during assignment
                        //   - It should exist ONLY under /assign/<worker>/<task>
                        //   - The master is the single authority performing recovery
                        //
                        // Therefore, /tasks/<task> should normally not already exist.
                        //
                        // Important nuance (ZooKeeper retry semantics):
                        // ---------------------------------------------
                        // NODEEXISTS *can* still occur legitimately if a previous multi()
                        // actually COMMITTED, but the master observed CONNECTIONLOSS and retried.
                        // In that case:
                        //
                        //   - /tasks/<task> already exists (from the earlier successful commit)
                        //   - /assign/<worker>/<task> is already deleted
                        //
                        // This is NOT a correctness violation — it is a consequence of ZooKeeper’s
                        // "unknown commit outcome" on connection loss.
                        //
                        // Contrast with WORKER COMPLETION:
                        // --------------------------------
                        // NODEEXISTS is *expected* when workers complete tasks (at-least-once
                        // execution, duplicate workers, races). See TaskExecutor.taskCompletionCallback.
                        //
                        // This code path is different:
                        //   - Master-driven
                        //   - Centralized
                        //   - Aims for exactly-once *recovery*, not execution
                        //
                        // Handling policy:
                        // ----------------
                        // On NODEEXISTS, we must distinguish:
                        //   - Benign retry after unknown commit (assignment already gone)
                        //   - Real invariant break (assignment still present)
                        //
                        // We therefore log loudly and may verify assignment state rather than
                        // blindly performing best-effort cleanup, to avoid masking real bugs.
                            LOG.error(
                                    "Invariant violation: task {} already exists under /tasks during unassignment",
                                    taskCtx.taskName()
                            );
                    default -> LOG.error(
                            "Error when recreating taskName",
                            KeeperException.create(KeeperException.Code.get(rc), path)
                    );
                }
            };

    private void unassignTask(TaskCtx taskCtx) {
        String newTaskPath = "/tasks/" + taskCtx.taskName();

        Op createTaskUnderNewTaskPath = Op.create(
                newTaskPath,
                taskCtx.taskData(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT
        );

        // ZooKeeper version semantics for delete():
        //   version = -1  → delete unconditionally (ignore the current znode version)
        //   version ≥ 0   → delete only if the current znode version matches
        // Using -1 ensures the assignment is removed regardless of concurrent updates.
        Op deleteTaskFromAssignmentPath = Op.delete(taskCtx.assignmentPath(), -1);

        List<Op> ops = Arrays.asList(
                createTaskUnderNewTaskPath,
                deleteTaskFromAssignmentPath
        );

        zk.multi(ops, taskUnassignMultiCallback, taskCtx);
    }

    private final AsyncCallback.VoidCallback deleteAssignedWorkerCallback =
            (rc, path, ctx) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> deleteAssignedWorker(path);
                    case OK -> LOG.info("Assigned worker path {} has been deleted", path);
                    case NONODE -> {
                        // Already deleted, ignore
                    }
                    case NOTEMPTY -> {
                        // Children still exist; we'll try again after more tasks are recovered.
                    }
                    default -> LOG.error(
                            "Failed to delete assigned worker path {}",
                            path,
                            KeeperException.create(KeeperException.Code.get(rc), path)
                    );
                }
            };

    private void deleteAssignedWorker(String assignedWorkerPath) {
        zk.delete(assignedWorkerPath, -1, deleteAssignedWorkerCallback, assignedWorkerPath);
    }

    public void recoverTasks(List<String> removedWorkers) {
        for (String worker : removedWorkers) {
            getAssignmentsForRemovedWorker(worker);
        }
    }
}

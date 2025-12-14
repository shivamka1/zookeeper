package zookeeper.masterworker.worker;


import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskCompletionPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(TaskCompletionPublisher.class);

    private final ZooKeeper zk;

    public TaskCompletionPublisher(ZooKeeper zk) {
        this.zk = zk;
    }

    private AsyncCallback.MultiCallback taskCompletionCallback(
            String taskName,
            String statusPath,
            String assignmentPath,
            Runnable onTaskFinished
    ) {
        return (rc, path, ctx, results) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS ->
                        publishStatusAndDeleteAssignment(taskName, assignmentPath, (Boolean) ctx, onTaskFinished);
                case OK -> {
                    LOG.info("Completed task {} (wrote {}, deleted {})", taskName, statusPath, assignmentPath);
                    onTaskFinished.run();
                }
                case NODEEXISTS -> {
                    // Status node already exists.
                    //
                    // When this needs special handling?
                    // --------------------
                    // This can occur when a task completion races with failure handling or task unassignment (See TaskUnassignmentManager),
                    // especially if a worker revives with the SAME worker name.
                    //
                    // Example scenario:
                    // 1) Worker A is executing task-7 under /assign/worker-A/task-7.
                    // 2) Worker A is considered dead (session expiry or long partition), so the master:
                    //      - removes worker-A from /workers
                    //      - moves /assign/worker-A/task-7 back to /tasks
                    // 3) Now:
                    //      - Worker A reconnects with the SAME name and manages to find /assign/worker-A/task-7
                    //          still available i.e, before it is moved to /tasks by TaskUnassignmentManager. It starts executing task-7,
                    //      - In the meanwhile, /assign/worker-A/task-7 is eventually moved to /tasks and another worker picks up task-7
                    //          from /tasks and also starts executing it.
                    // 4) Depending on which worker finishes first, the other worker when attempts completion will find that the status
                    //      node is already created.
                    //      - create(/status/task-7) → NODEEXISTS (task already completed elsewhere)
                    //
                    // Important nuance:
                    // -----------------
                    // Completion is done via a TRANSACTION:
                    //   create /status/<task>
                    //   delete /assign/<this-worker>/<task>
                    //
                    // If create() hits NODEEXISTS, the ENTIRE transaction aborts.
                    // This means:
                    //   - /status/task-7 already exists (written by another worker)
                    //   - BUT our assignment node is NOT deleted by this transaction
                    //
                    // Required handling:
                    // ------------------
                    // We must treat the task as already completed and perform best-effort cleanup
                    // of THIS worker’s assignment node to avoid leaving it dangling.
                    //
                    // Deleting the assignment is safe and idempotent:
                    //   - If it still exists → delete succeeds
                    //   - If it is already gone → NONODE is fine
                    //
                    // Since in our master worker implementation, we assign a unique workerName every time a new worker
                    // created or an old worker is resurrected, the resurrected worker will never find the task say at last
                    // assign path, for example, /assign/worker-A/task-7. Simply because it may have been given a new name, say worker-Z.
                    // However, we must make sure that the /assign/worker-A is deleted as part of task unassignment otherwise those
                    // assignment path will lie hanging there forever.
                    LOG.info("Status {} already exists for task {}", statusPath, taskName);
                    onTaskFinished.run();
                }
                default -> {
                    LOG.error("Completion transaction failed for task {}",
                            taskName, KeeperException.create(KeeperException.Code.get(rc), path));
                    onTaskFinished.run();
                }
            }
        };
    }

    public void publishStatusAndDeleteAssignment(
            String taskName,
            String assignmentPath,
            boolean success,
            Runnable onTaskFinished
    ) {
        String statusPath = "/status/" + taskName;
        byte[] statusData = (success ? "done" : "failed").getBytes();

        zk.transaction()
                .create(statusPath, statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                .delete(assignmentPath, -1)
                .commit(taskCompletionCallback(taskName, statusPath, assignmentPath, onTaskFinished), success);
    }
}

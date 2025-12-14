package zookeeper.masterworker.worker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

    private final ZooKeeper zk;
    private final String workerName;
    private final StatusUpdate statusUpdate;
    private final ExecutorService executor;

    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

    public TaskExecutor(ZooKeeper zk, String workerName, StatusUpdate statusUpdate, ExecutorService executor) {
        this.zk = zk;
        this.workerName = workerName;
        this.statusUpdate = statusUpdate;
        this.executor = executor;
    }

    private final AsyncCallback.DataCallback assignmentDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            String taskName = (String) ctx;

            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> getAssignmentData(path, taskName);
                case OK -> {
                    if (data == null) {
                        LOG.warn("Task {} had no payload at {}", taskName, path);
                        onTaskFinished(taskName);
                        return;
                    }
                    // Never execute work on the ZooKeeper callback thread.
                    executor.execute(() -> runTask(taskName, path, data));
                }
                case NONODE -> {
                    // The assignment disappeared before we could read it (master reassigned, worker cleanup, etc.)
                    LOG.warn("Assignment {} disappeared before read (task {})", path, taskName);
                    onTaskFinished(taskName);
                }
                default -> {
                    LOG.error("getData({}) failed for task {}", path, taskName,
                            KeeperException.create(KeeperException.Code.get(rc), path));
                    onTaskFinished(taskName);
                }
            }
        }
    };

    private void getAssignmentData(String assignmentPath, String taskName) {
        zk.getData(assignmentPath, false, assignmentDataCallback, taskName);
    }

    private void execute(String taskName) {
        String assignmentPath = "/assign/" + workerName + "/" + taskName;
        onTaskStarted(taskName);
        getAssignmentData(assignmentPath, taskName);
    }

    private void runTask(String taskName, String assignmentPath, byte[] payload) {
        boolean success = false;

        try {
            String taskData = new String(payload);
            LOG.info("Executing task {} with payload: {}", taskName, taskData);

            // Placeholder for real work
            success = true;
        } catch (Exception e) {
            LOG.error("Task {} failed during execution", taskName, e);
        }

        // Publish status and cleanup assignment (best-effort, retry on connection loss).
        writeStatusAndDeleteAssignment(taskName, assignmentPath, success);
    }

    private AsyncCallback.MultiCallback taskCompletionCallback(String taskName, String statusPath, String assignmentPath) {
        return (rc, path, ctx, results) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS -> writeStatusAndDeleteAssignment(taskName, assignmentPath, (Boolean) ctx);
                case OK -> {
                    LOG.info("Completed task {} (wrote {}, deleted {})", taskName, statusPath, assignmentPath);
                    onTaskFinished(taskName);
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
                    onTaskFinished(taskName);
                }
                default -> {
                    LOG.error("Completion transaction failed for task {}",
                            taskName, KeeperException.create(KeeperException.Code.get(rc), path));
                    onTaskFinished(taskName);
                }
            }
        };
    }

    private void writeStatusAndDeleteAssignment(String taskName, String assignmentPath, boolean success) {
        String statusPath = "/status/" + taskName;
        byte[] statusData = (success ? "done" : "failed").getBytes();

        zk.transaction()
                .create(statusPath, statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                .delete(assignmentPath, -1)
                .commit(taskCompletionCallback(taskName, statusPath, assignmentPath), success);
    }

    private void onTaskStarted(String taskName) {
        // Track which tasks are currently executing.
        //
        // Why 'inFlight' (a Set)?
        // -----------------------
        // ZooKeeper watchers and callbacks may fire more than once for the same task
        // (e.g. retries after CONNECTIONLOSS, re-reads, or duplicated child events).
        //
        // We must ensure that each logical task is counted and executed only once.
        // The Set guarantees per-task idempotence: the first thread wins, others exit.
        //
        // If add() returns false, this task was already started and should be ignored.
        if (!inFlight.add(taskName)) return;

        // Track how many tasks are currently executing.
        //
        // Why 'inFlightCount' (an AtomicInteger)?
        // --------------------------------------
        // Worker status should change:
        //   Idle    → Working when the FIRST task starts
        //   Working → Idle    when the LAST task finishes
        //
        // Multiple tasks may start concurrently, but only the thread that transitions
        // the count from 0 → 1 is allowed to publish "Working".
        //
        // getAndIncrement() returns the *previous* value atomically, so:
        //   - if it returns 0, we just became busy
        //   - otherwise, we were already working
        if (inFlightCount.getAndIncrement() == 0) {
            statusUpdate.setStatus("Working");
        }
    }

    private void onTaskFinished(String taskName) {
        // Only finish once
        if (!inFlight.remove(taskName)) return;

        // Only the thread that moves 1 -> 0 sets Idle
        if (inFlightCount.decrementAndGet() == 0) {
            statusUpdate.setStatus("Idle");
        }
    }

    public void executeTasks(List<String> tasks) {
        for (String task : tasks) {
            execute(task);
        }
    }
}

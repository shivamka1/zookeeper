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
    private final ExecutorService executor;
    private final String workerName;
    private final WorkerStatusUpdater workerStatusUpdater;
    private final TaskCompletionPublisher taskCompletionPublisher;

    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

    public TaskExecutor(
            ZooKeeper zk,
            String workerName,
            ExecutorService executor,
            WorkerStatusUpdater workerStatusUpdater,
            TaskCompletionPublisher taskCompletionPublisher
    ) {
        this.zk = zk;
        this.executor = executor;
        this.workerName = workerName;
        this.workerStatusUpdater = workerStatusUpdater;
        this.taskCompletionPublisher = taskCompletionPublisher;
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
        taskCompletionPublisher.publishStatusAndDeleteAssignment(taskName, assignmentPath, success, () -> onTaskFinished(taskName));
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
            workerStatusUpdater.setStatus("Working");
        }
    }

    private void onTaskFinished(String taskName) {
        // Only finish once
        if (!inFlight.remove(taskName)) return;

        // Only the thread that moves 1 -> 0 sets Idle
        if (inFlightCount.decrementAndGet() == 0) {
            workerStatusUpdater.setStatus("Idle");
        }
    }

    public void executeTasks(List<String> tasks) {
        for (String task : tasks) {
            execute(task);
        }
    }
}

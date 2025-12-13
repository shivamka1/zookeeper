package zookeeper.masterworker.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a single task submitted to the master–worker system.
 *
 * This class is intentionally thread-safe because:
 *  - ZooKeeper callbacks may retry on CONNECTIONLOSS
 *  - Watchers may fire more than once
 *  - Multiple callback paths may attempt to complete the same task
 *
 * The goal is:
 *  - Allow multiple threads to *wait* for completion
 *  - Ensure the task is *completed exactly once*
 *  - Make completion visible to all waiting threads
 */
public class Task {
    private final String taskName;
    private final String taskData;

    /**
     * Ensures that completion happens at most once.
     *
     * Why AtomicBoolean?
     * -------------------
     * ZooKeeper callbacks can be retried and re-entered:
     *  - A status watcher may fire
     *  - A reconnection retry may re-read the same status
     *
     * Without this guard, multiple threads could attempt to complete
     * the task and race to update its final state.
     *
     * compareAndSet(false, true) guarantees:
     *  - Only the *first* caller wins
     *  - All subsequent calls become no-ops
     *
     * This gives us idempotent completion.
     */
    private final AtomicBoolean completed = new AtomicBoolean(false);

    /**
     * Final task outcome.
     *
     * Visibility and concurrency guarantees:
     * --------------------------------------
     *
     * This field is read by multiple threads:
     *  - ZooKeeper callback threads (writers)
     *  - Client / application threads (readers)
     *
     * It is marked volatile to support *non-blocking* readers that may
     * observe task completion without calling await().
     *
     * Two supported usage patterns:
     * ------------------------------
     *
     * 1) Blocking usage (most common):
     *
     *     task.await();
     *     handle(task.getStatus());
     *
     * In this case:
     *  - complete() sets the final status and then calls latch.countDown().
     *  - CountDownLatch establishes a happens-before relationship:
     *      all writes before countDown() are visible after await() returns.
     *  - Once await() returns, getStatus() is guaranteed to see the final value
     *    (SUCCESS or FAILURE).
     *
     * 2) Non-blocking / polling usage:
     *
     *     if (task.isDone()) {
     *         handle(task.getStatus());
     *     }
     *
     * In this case:
     *  - The caller does NOT block on await().
     *  - Visibility relies entirely on 'status' being volatile.
     *  - volatile guarantees that writes in complete() are immediately visible
     *    and readers will never see a stale PENDING value.
     *
     * Why volatile is still required:
     * -------------------------------
     * Even though await() provides visibility for blocking callers,
     * other threads may legally read status without ever calling await().
     *
     * Marking status as volatile ensures:
     *  - Both blocking and non-blocking access patterns are safe
     *  - No thread can observe stale state after completion
     *
     * State transition:
     * -----------------
     *   PENDING → SUCCESS | FAILURE
     */
    private volatile TaskStatus status = TaskStatus.PENDING;

    /**
     * Used to block callers until the task completes.
     *
     * Why CountDownLatch?
     * -------------------
     *  - Any number of threads can call await()
     *  - All threads are released when count reaches zero
     *  - Exactly matches the "wait until task is done" semantics
     *
     * Unlike wait/notify:
     *  - No risk of missed notifications
     *  - No need for synchronized blocks
     *  - Much easier to reason about
     */
    private final CountDownLatch latch = new CountDownLatch(1);

    public Task(String taskName, String taskData) {
        this.taskName = taskName;
        this.taskData = taskData;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getTaskData() {
        return taskData;
    }

    /**
     * Returns true once the task has reached a terminal state.
     *
     * This method is non-blocking and safe to call from any thread.
     */
    public boolean isDone() {
        return status != TaskStatus.PENDING;
    }

    /**
     * Returns the current task status.
     *
     * If called before completion, this will return PENDING.
     */
    public TaskStatus getStatus() {
        return status;
    }

    /**
     * Marks the task as completed.
     *
     * This method is:
     *  - Thread-safe
     *  - Idempotent
     *  - Safe under ZooKeeper retries and reconnections
     *
     * Only the first caller is allowed to:
     *  - Set the final status
     *  - Release waiting threads
     *
     * All subsequent calls return immediately.
     */
    public void complete(boolean success) {
        // Ensure only one thread performs completion logic
        if (!completed.compareAndSet(false, true)) return;
        status = success ? TaskStatus.SUCCESS : TaskStatus.FAILURE;
        latch.countDown();
    }

    /**
     * Blocks the calling thread until the task completes.
     *
     * Happens-before guarantee:
     *  - Once await() returns, the final status is visible
     *
     * Typical usage:
     *   task.await();
     *   switch (task.getStatus()) { ... }
     */
    public void await() throws InterruptedException {
        latch.await();
    }
}

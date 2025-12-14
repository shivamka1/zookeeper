package zookeeper.masterworker.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a single task submitted to the master–worker system.
 *
 * <p>This class is intentionally thread-safe because:</p>
 * <ul>
 *   <li>ZooKeeper callbacks may retry on {@code CONNECTIONLOSS}</li>
 *   <li>Watchers may fire more than once</li>
 *   <li>Multiple callback paths may attempt to complete the same task</li>
 * </ul>
 *
 * <p>The goal is:</p>
 * <ul>
 *   <li>Allow multiple threads to <em>wait</em> for completion</li>
 *   <li>Ensure the task is <em>completed exactly once</em></li>
 *   <li>Make completion visible to all waiting threads</li>
 * </ul>
 */
public class Task {
    private final String taskName;
    private final String taskData;

    /**
     * Ensures that completion happens at most once.
     *
     * <p>Why {@link AtomicBoolean}?</p>
     *
     * <p>ZooKeeper callbacks can be retried and re-entered:</p>
     * <ul>
     *   <li>A status watcher may fire</li>
     *   <li>A reconnection retry may re-read the same status</li>
     * </ul>
     *
     * <p>Without this guard, multiple threads could attempt to complete
     * the task and race to update its final state.</p>
     *
     * <p>{@code compareAndSet(false, true)} guarantees:</p>
     * <ul>
     *   <li>Only the <em>first</em> caller wins</li>
     *   <li>All subsequent calls become no-ops</li>
     * </ul>
     *
     * <p>This gives us idempotent completion.</p>
     */
    private final AtomicBoolean completed = new AtomicBoolean(false);

    /**
     * Final task outcome.
     *
     * <p>Visibility and concurrency guarantees:</p>
     *
     * <p>This field is read by multiple threads:</p>
     * <ul>
     *   <li>ZooKeeper callback threads (writers)</li>
     *   <li>Client / application threads (readers)</li>
     * </ul>
     *
     * <p>It is marked {@code volatile} to support <em>non-blocking</em> readers that may
     * observe task completion without calling {@code await()}.</p>
     *
     * <p>Two supported usage patterns:</p>
     *
     * <ol>
     *   <li>
     *     Blocking usage (most common):
     *
     *     <pre>{@code
     * task.await();
     * handle(task.getStatus());
     *     }</pre>
     *
     *     <p>In this case:</p>
     *     <ul>
     *       <li>{@code complete()} sets the final status and then calls {@code latch.countDown()}</li>
     *       <li>{@code CountDownLatch} establishes a happens-before relationship:
     *           all writes before {@code countDown()} are visible after {@code await()} returns</li>
     *       <li>Once {@code await()} returns, {@code getStatus()} is guaranteed to see
     *           the final value ({@code SUCCESS} or {@code FAILURE})</li>
     *     </ul>
     *   </li>
     *
     *   <li>
     *     Non-blocking / polling usage:
     *
     *     <pre>{@code
     * if (task.isDone()) {
     *     handle(task.getStatus());
     * }
     *     }</pre>
     *
     *     <p>In this case:</p>
     *     <ul>
     *       <li>The caller does <em>not</em> block on {@code await()}</li>
     *       <li>Visibility relies entirely on {@code status} being {@code volatile}</li>
     *       <li>{@code volatile} guarantees that writes in {@code complete()} are
     *           immediately visible and readers will never see a stale {@code PENDING} value</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * <p>Why {@code volatile} is still required:</p>
     *
     * <p>Even though {@code await()} provides visibility for blocking callers,
     * other threads may legally read {@code status} without ever calling {@code await()}.</p>
     *
     * <p>Marking {@code status} as {@code volatile} ensures:</p>
     * <ul>
     *   <li>Both blocking and non-blocking access patterns are safe</li>
     *   <li>No thread can observe stale state after completion</li>
     * </ul>
     *
     * <p>State transition:</p>
     *
     * <pre>{@code
     * PENDING → SUCCESS | FAILURE
     * }</pre>
     */
    private volatile TaskStatus status = TaskStatus.PENDING;

    /**
     * Used to block callers until the task completes.
     *
     * <p>Why {@link CountDownLatch}?</p>
     * <ul>
     *   <li>Any number of threads can call {@code await()}</li>
     *   <li>All threads are released when the count reaches zero</li>
     *   <li>Exactly matches the "wait until task is done" semantics</li>
     * </ul>
     *
     * <p>Unlike {@code wait}/{@code notify}:</p>
     * <ul>
     *   <li>No risk of missed notifications</li>
     *   <li>No need for synchronized blocks</li>
     *   <li>Much easier to reason about</li>
     * </ul>
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
     * Returns {@code true} once the task has reached a terminal state.
     *
     * <p>This method is non-blocking and safe to call from any thread.</p>
     */
    public boolean isDone() {
        return status != TaskStatus.PENDING;
    }

    /**
     * Returns the current task status.
     *
     * <p>If called before completion, this will return {@code PENDING}.</p>
     */
    public TaskStatus getStatus() {
        return status;
    }

    /**
     * Marks the task as completed.
     *
     * <p>This method is:</p>
     * <ul>
     *   <li>Thread-safe</li>
     *   <li>Idempotent</li>
     *   <li>Safe under ZooKeeper retries and reconnections</li>
     * </ul>
     *
     * <p>Only the first caller is allowed to:</p>
     * <ul>
     *   <li>Set the final status</li>
     *   <li>Release waiting threads</li>
     * </ul>
     *
     * <p>All subsequent calls return immediately.</p>
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
     * <p>Happens-before guarantee:</p>
     * <ul>
     *   <li>Once {@code await()} returns, the final status is visible</li>
     * </ul>
     *
     * <p>Typical usage:</p>
     * <pre>{@code
     * task.await();
     * switch (task.getStatus()) { ... }
     * }</pre>
     */
    public void await() throws InterruptedException {
        latch.await();
    }
}

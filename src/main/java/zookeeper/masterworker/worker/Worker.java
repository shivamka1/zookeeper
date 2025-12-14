package zookeeper.masterworker.worker;

import zookeeper.masterworker.Bootstrapper;
import zookeeper.masterworker.IdGenerator;
import zookeeper.masterworker.SessionState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Worker {
    private ZooKeeper zk;
    private final String connectString;
    private final String serverId = IdGenerator.newId();
    private final String workerName = "worker-" + serverId;

    private final SessionState sessionState = new SessionState();
    private Bootstrapper bootstrapper;

    private RegisterWorker registerWorker;

    private ThreadPoolExecutor executor;
    private AssignedTasksWatcher assignedTasksWatcher;

    Worker(String connectString) {
        this.connectString = connectString;
    }

    boolean isConnected() {
        return sessionState.isConnected();
    }

    boolean isExpired() {
        return sessionState.isExpired();
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(this.connectString, 15000, sessionState);
        bootstrapper = new Bootstrapper(zk);
        registerWorker = new RegisterWorker(workerName, zk);

        StatusUpdate statusUpdate = new StatusUpdate(workerName, zk);
        executor = new ThreadPoolExecutor(
                1, 1, 1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(200)
        );
        TaskExecutor taskExecutor = new TaskExecutor(zk, workerName, statusUpdate, executor);
        assignedTasksWatcher = new AssignedTasksWatcher(zk, workerName, taskExecutor);
    }

    void stopZk() throws InterruptedException {
        if (executor != null) executor.shutdownNow();
        if (zk != null) zk.close();
    }

    /**
     * Worker registration ordering matters.
     *
     * <p>Each worker creates:</p>
     * <ol>
     *   <li>{@code /assign/<worker-id>} — parent for task assignments</li>
     *   <li>{@code /workers/<worker-id>} — signals presence to the master</li>
     * </ol>
     *
     * <p>We MUST create {@code /assign/<worker-id>} first.</p>
     *
     * <p>If {@code /workers/<worker-id>} were created before
     * {@code /assign/<worker-id>}, the master could observe the worker and attempt
     * to assign a task before the assignment parent exists, causing the assignment
     * to fail.</p>
     *
     * <p>After creating {@code /assign/<worker-id>}, the worker sets a watch on it
     * to be notified when the master assigns new tasks.</p>
     */
    void bootstrap() {
        bootstrapper.createPersistent("/assign/worker-" + serverId);
    }

    static void main(String[] args) throws Exception {
        Worker worker = new Worker(args[0]);

        worker.startZk();

        while (!worker.isConnected()) {
            Thread.sleep(1000);
        }

        worker.bootstrap();

        // Register the worker so that the leader knows that the worker is here
        worker.registerWorker.register();

        // Start watching for assignments.
        worker.assignedTasksWatcher.startWatching();

        while (!worker.isExpired()) {
            Thread.sleep(1000);
        }

        worker.stopZk();
    }
}

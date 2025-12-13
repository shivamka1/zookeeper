package zookeeper.masterworker.client;

import zookeeper.masterworker.SessionState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Client {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private ZooKeeper zk;
    private final String connectString;

    private final SessionState sessionState;
    private SubmitTask submitTask;
    private StatusWatcher statusWatcher;

    Client(String connectString) {
        this.connectString = connectString;
        this.sessionState = new SessionState();
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(connectString, 15000, sessionState);
        statusWatcher = new StatusWatcher(zk);
        submitTask = new SubmitTask(zk, statusWatcher);
    }

    void stopZk() throws InterruptedException {
        if (zk != null) zk.close();
    }

    boolean isConnected() {
        return sessionState.isConnected();
    }

    boolean isExpired() {
        return sessionState.isExpired();
    }

    static void main(String[] args) throws Exception {
        Client client = new Client(args[0]);

        client.startZk();

        while (!client.isConnected()) {
            Thread.sleep(100);
        }

        Task task1 = client.submitTask.submit("Sample task 1")
                .exceptionally(e -> {
                    throw new RuntimeException("Failed to submit task", e);
                })
                .join();
        Task task2 = client.submitTask.submit("Sample task 2")
                .exceptionally(e -> {
                    throw new RuntimeException("Failed to submit task", e);
                })
                .join();

        task1.await();
        task2.await();

        LOG.info("Task {} with payload {} finished with result = {}", task1.getTaskName(), task1.getTaskData(), task1.getStatus());
        LOG.info("Task {} with payload {} finished with result = {}", task2.getTaskName(), task2.getTaskData(), task2.getStatus());

        while (!client.isExpired()) {
            Thread.sleep(1000);
        }

        client.stopZk();
    }
}

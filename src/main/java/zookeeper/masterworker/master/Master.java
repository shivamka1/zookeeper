package zookeeper.masterworker.master;

import zookeeper.masterworker.Bootstrapper;
import zookeeper.masterworker.SessionState;
import zookeeper.masterworker.master.tasks.TaskAssignmentManager;
import zookeeper.masterworker.master.tasks.TaskUnassignmentManager;
import zookeeper.masterworker.master.tasks.TasksWatcher;
import zookeeper.masterworker.master.tasks.WorkersWatcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Master {
    private final String connectString;
    private ZooKeeper zk;

    private Bootstrapper bootstrapper;
    private final SessionState sessionState = new SessionState();
    private MasterElection masterElection;

    public Master(String connectString) {
        this.connectString = connectString;
    }

    boolean isConnected() {
        return sessionState.isConnected();
    }

    boolean isExpired() {
        return sessionState.isExpired();
    }

    MasterState getMasterState() {
        return masterElection.getMasterState();
    }

    String getServerId() {
        return masterElection.getServerId();
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(connectString, 15000, sessionState);
        bootstrapper = new Bootstrapper(zk);

        TaskUnassignmentManager taskUnassignmentManager = new TaskUnassignmentManager(zk);
        WorkersWatcher workersWatcher = new WorkersWatcher(zk, taskUnassignmentManager);

        TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(zk, workersWatcher::getCurrentWorkers);
        TasksWatcher tasksWatcher = new TasksWatcher(zk, taskAssignmentManager);

        masterElection = new MasterElection(zk, workersWatcher, tasksWatcher);
    }

    void stopZk() throws InterruptedException {
        if (zk != null) zk.close();
    }

    void bootstrap() {
        bootstrapper.createPersistent("/workers");
        bootstrapper.createPersistent("/assign");
        bootstrapper.createPersistent("/tasks");
        bootstrapper.createPersistent("/status");
    }

    static void main(String[] args) throws Exception {
        Master master = new Master(args[0]);

        master.startZk();

        while (!master.isConnected()) {
            Thread.sleep(100);
        }

        master.bootstrap();
        master.masterElection.runForMaster();

        while (!master.isExpired()) {
            Thread.sleep(1000);
        }

        master.stopZk();
    }
}

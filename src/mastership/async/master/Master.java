package mastership.async.master;

import mastership.async.Bootstrapper;
import mastership.async.SessionState;
import mastership.async.master.tasks.TaskAssignmentManager;
import mastership.async.master.tasks.TaskReassignmentManager;
import mastership.async.master.tasks.TasksTracker;
import mastership.async.master.tasks.WorkersTracker;
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

        TaskReassignmentManager taskReassignmentManager = new TaskReassignmentManager(zk);
        WorkersTracker workersTracker = new WorkersTracker(zk, taskReassignmentManager);

        TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(zk, workersTracker::getCurrentWorkers);
        TasksTracker tasksTracker = new TasksTracker(zk, taskAssignmentManager);

        masterElection = new MasterElection(zk, workersTracker, tasksTracker);
    }

    void stopZk() throws InterruptedException {
        zk.close();
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

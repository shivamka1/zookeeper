package zookeeper.masterworker.worker;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

    private final ZooKeeper zk;
    private final String workerName;
    private final StatusUpdate statusUpdate;
    private final ExecutorService executor;

    private final AtomicInteger tasksInProgress = new AtomicInteger(0);

    public TaskExecutor(ZooKeeper zk, String workerName, StatusUpdate statusUpdate, ExecutorService executor) {
        this.zk = zk;
        this.workerName = workerName;
        this.statusUpdate = statusUpdate;
        this.executor = executor;
    }


}

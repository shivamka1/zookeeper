package zookeeper.masterworker.worker;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(TaskWatcher.class);

    private final ZooKeeper zk;
    private String workerName;
    private TaskExecutor taskExecutor;

    public TaskWatcher(ZooKeeper zk, String workerName, TaskExecutor taskExecutor) {
        this.zk = zk;
        this.workerName = workerName;
        this.taskExecutor = taskExecutor;
    }



}

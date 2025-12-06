package mastership.async.worker;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class StatusUpdate {
    private static final Logger LOG = LoggerFactory.getLogger(StatusUpdate.class);

    private final ZooKeeper zk;
    private final String workerName;
    private String status;

    StatusUpdate(String serverId, ZooKeeper zk) {
        this.workerName = "worker-" + serverId;
        this.zk = zk;
    }

    private final AsyncCallback.StatCallback statusUpdateCallback =
            new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            // Retry with the same status stored in ctx
                            updateStatus((String) ctx);
                            break;
                        default:
                            // No extra handling needed for OK; others are debug-level here.
                            break;
                    }
                }
            };

    private synchronized void updateStatus(String newStatus) {
        // Only write if this is still the current status value
        if (Objects.equals(newStatus, this.status)) {
            String path = "/workers/" + workerName;
            zk.setData(
                    path,
                    newStatus.getBytes(),
                    -1,
                    statusUpdateCallback,
                    newStatus
            );
        }
    }

    public synchronized void setStatus(String newStatus) {
        // Save our status locally in case a status update fails, and we need to retry.
        this.status = newStatus;
        updateStatus(newStatus);
    }
}

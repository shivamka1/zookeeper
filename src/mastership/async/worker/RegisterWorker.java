package mastership.async.worker;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterWorker {
    private static final Logger LOG = LoggerFactory.getLogger(RegisterWorker.class);

    private final ZooKeeper zk;
    private final String workerName;

    RegisterWorker(String serverId, ZooKeeper zk) {
        this.workerName = "worker-" + serverId;
        this.zk =zk;
    }

    private final AsyncCallback.StringCallback createWorkerCallback =
            new AsyncCallback.StringCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, String name) {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            // Retry registration
                            register();
                            break;
                        case OK:
                            LOG.info("Worker registered successfully: {}", workerName);
                            break;
                        case NODEEXISTS:
                            LOG.warn("Worker already registered: {}", workerName);
                            break;
                        default:
                            LOG.error(
                                    "Error registering worker {}",
                                    workerName,
                                    KeeperException.create(KeeperException.Code.get(rc), path)
                            );
                    }
                }
            };

    public void register() {
        String path = "/workers/" + workerName;
        zk.create(
                path,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null
        );
    }
}

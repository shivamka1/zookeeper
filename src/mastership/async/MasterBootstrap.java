package mastership.async;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterBootstrap {
    private static final Logger LOG = LoggerFactory.getLogger(MasterBootstrap.class);

    private final ZooKeeper zk;

    MasterBootstrap(ZooKeeper zk) {
        this.zk = zk;
    }

    private final AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                // Retry: if it already exists, we'll get NODEEXISTS next time.
                case CONNECTIONLOSS -> createParent(path, (byte[]) ctx);
                case OK -> LOG.info("Parent created: {}", path);
                case NODEEXISTS -> LOG.warn("Parent already registered: {}", path);
                default -> LOG.error(
                        "Error creating parent {}",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    private void createParent(String path, byte[] data) {
        zk.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data
        );
    }

    void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }
}

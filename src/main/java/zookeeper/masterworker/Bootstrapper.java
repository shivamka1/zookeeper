package zookeeper.masterworker;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrapper {
    private static final Logger LOG = LoggerFactory.getLogger(Bootstrapper.class);

    private final ZooKeeper zk;

    public Bootstrapper(ZooKeeper zk) {
        this.zk = zk;
    }

    private final AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                // Retry: if it already exists, we'll get NODEEXISTS next time.
                case CONNECTIONLOSS -> createPersistent(path, (byte[]) ctx);
                case OK -> LOG.info("Parent znode created: {}", path);
                case NODEEXISTS -> LOG.warn("Parent znode already registered: {}", path);
                default -> LOG.error(
                        "Error creating parent {}",
                        path,
                        KeeperException.create(KeeperException.Code.get(rc), path)
                );
            }
        }
    };

    void createPersistent(String path, byte[] data) {
        zk.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data
        );
    }

    public void createPersistent(String path) {
        createPersistent(path, new byte[0]);
    }
}

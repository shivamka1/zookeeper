import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Master implements Watcher {

    String connectString;
    ZooKeeper zk;

    Master(String connectString) {
        this.connectString = connectString;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(connectString, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    static void main(String[] args) throws Exception {
        Master master = new Master(args[0]);
        master.startZk();

        Thread.sleep(60000);

        master.stopZk();
    }
}

package huangzehai.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Master implements Watcher {

    private Logger logger = LoggerFactory.getLogger(Master.class);
    private ZooKeeper zk;
    private String hostPort;

    private Random random = new Random(this.hashCode());

    String serviceId = Integer.toHexString(random.nextInt());

    boolean isLeader = false;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        if (args.length == 0) {
            System.out.println("Usage Master port");
            System.exit(1);
        }

        Master master = new Master(args[0]);
        master.startZk();
        master.runForMaster();
        Thread.sleep(10000);
        master.stopZk();
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master", serviceId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.info("Create master");
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                logger.info("Node exists");
                isLeader = false;
                break;
            } catch (KeeperException e) {
                logger.error("Connection Lost");
                e.printStackTrace();
            }
            if (checkMaster()) {
                break;
            }
        }
    }

    boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serviceId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                //No master, so try to create again
                logger.error("No node");
                return false;
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

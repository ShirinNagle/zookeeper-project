import org.apache.zookeeper.*;

import java.io.File;
import java.io.IOException;

public class ClusterHealer implements Watcher{

    // Path to the worker jar
    private final String pathToProgram;
    // The number of worker instances we need to maintain at all times
    private final int numberOfWorkers;
    //Code added by me
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static final String CLUSTER_NAMESPACE = "/worker";
    private String currentZnodeName;

    private ZooKeeper zooKeeper;
    private boolean isParent = false;//boolean to check if the znode is a parent

    public ClusterHealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }

    /**
     * Check if the `/workers` parent znode exists, and create it if it doesn't. Decide for yourself what type of znode
     * it should be (e.g.persistent, ephemeral etc.). Check if workers need to be launched.
     */
    public void initialiseCluster() throws KeeperException, InterruptedException {

        // **************
        // YOUR CODE HERE
        // **************
        //Persistant node as this node is the parent node - don't want this node to die.
        String znodePrefix = CLUSTER_NAMESPACE + "/w_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(CLUSTER_NAMESPACE + "/", "");
        System.out.println("znode name " + currentZnodeName);
    }
    /**
     * Instantiates a Zookeeper client, creating a connection to the Zookeeper server.
     */
    public void connectToZookeeper() throws IOException {
        // **************
        // YOUR CODE HERE
        // **************
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    /**
     * Keeps the application running waiting for Zookeeper events.
     */
    public void run() throws InterruptedException {
        // **************
        // YOUR CODE HERE
        // **************
        zooKeeper.wait();
    }

    /**
     * Closes the Zookeeper client connection.
     */
    public void close() throws InterruptedException {
        // **************
        // YOUR CODE HERE
        // **************
        zooKeeper.close();
    }

    /**
     * Handles Zookeeper events related to: - Connecting and disconnecting from the Zookeeper server. - Changes in the
     * number of workers currently running.
     *
     * @param event A Zookeeper event
     */
    public void process(WatchedEvent event) {
        // **************
        // YOUR CODE HERE
        // **************
    }

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    public void checkRunningWorkers() {
        // **************
        // YOUR CODE HERE
        // **************
    }

    /**
     * Starts a new worker using the path provided as a command line parameter.
     *
     * @throws IOException
     */
    public void startWorker() throws IOException {
        File file = new File(pathToProgram);
        String command = "java -jar " + file.getName();
        System.out.println(String.format("Launching worker instance : %s ", command));
        Runtime.getRuntime().exec(command, null, file.getParentFile());
    }
}

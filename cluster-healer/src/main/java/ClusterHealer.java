import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ClusterHealer implements Watcher{

    // Path to the worker jar
    private final String pathToProgram;
    private final int numberOfWorkers;// The number of worker instances needs maintain at all times
    //Code added by me
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static final String PARENT_NAMESPACE = "/worker";
    private int runningWorkers;
    String parentName = "";
    private ZooKeeper zooKeeper;

    public ClusterHealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }
    /**
     * Check if the `/workers` parent znode exists, and create it if it doesn't. Decide for yourself what type of znode
     * it should be (e.g.persistent, ephemeral etc.). Check if workers need to be launched.
     */
    public void initialiseCluster() throws KeeperException, InterruptedException {

        //Persistant mode as this node is the parent node - don't want this node to die.
        //need to check if parent exists - if it doesn't need to create a parent
        //persistent parent created
            parentName = zooKeeper.create(PARENT_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("Parent name is: " + parentName);


    }
    /**
     * Instantiates a Zookeeper client, creating a connection to the Zookeeper server.
     */
    public void connectToZookeeper() throws IOException {

        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    /**
     * Keeps the application running waiting for Zookeeper events.
     */
    public void run() throws InterruptedException {

        synchronized (zooKeeper)
        {
            zooKeeper.wait();
        }
    }

    /**
     * Closes the Zookeeper client connection.
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * Handles Zookeeper events related to: - Connecting and disconnecting from the Zookeeper server. - Changes in the
     * number of workers currently running.
     *
     * @param event A Zookeeper event
     */
    public void process(WatchedEvent event) {

        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                //if a node is deleted need to notify checking workers - or checking workers needs to notify watchedEvent!!
        }


    }

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    public void checkRunningWorkers() throws KeeperException, InterruptedException {

        List<String> workers = zooKeeper.getChildren(parentName, this);

        System.out.println(workers);

        if(workers.size() < numberOfWorkers){

        }

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

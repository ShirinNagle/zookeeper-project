import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ClusterHealer implements Watcher{

    private final String pathToProgram;// Path to the worker jar
    private final int numberOfWorkers;// The number of worker instances needs maintain at all times
    //variables to connect to zookeeper
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    //parent variables
    public static final String PARENT_ZNODE = "/worker";
    String parentName;
    //workers variables  - probably don't need this!!
    private int runningWorkers;
    //zooKeeper variable - nothing works without this!!
    private ZooKeeper zooKeeper;

    //Constructor - Don't change!!
    public ClusterHealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }
    /**
     * Check if the `/workers` parent znode exists, and create it if it doesn't. Decide for yourself what type of znode
     * it should be (e.g.persistent, ephemeral etc.). Check if workers need to be launched.
     */
    public void initialiseCluster() throws KeeperException, InterruptedException, IOException {

        //Persistant mode as this node is the parent node - don't want this node to die.
        //need to check if parent exists - if it doesn't need to create a parent
        //persistent parent created
        if(PARENT_ZNODE == null) {
            parentName = zooKeeper.create(PARENT_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        //check if workers need to be launched
        if(numberOfWorkers > 0){
            startWorker();
        }
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
                try {
                    startWorker();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Received node deleted event");
        }


    }

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    public void checkRunningWorkers() throws KeeperException, InterruptedException, IOException {

        List<String> workers = zooKeeper.getChildren(parentName, this);

        System.out.println(workers);

        if(workers.size() < numberOfWorkers){
            startWorker();
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

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ClusterHealer implements Watcher {

    private final String pathToProgram;// Path to the worker jar
    private final int numberOfWorkers;// The number of worker instances needs maintain at all times
    //variables to connect to zookeeper
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    //parent variables
    private static final String WORKERS_PARENT_ZNODE = "/workers";//parent string
    private String parentName;
    //workers variables
    private int workersNo;
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

        Stat parentZNodeName = zooKeeper.exists(WORKERS_PARENT_ZNODE, false);
        // Stat test = zooKeeper.exists("/workers", false);

        //need to check if parent exists - if it doesn't need to create a parent
        if(parentZNodeName == null) {
            //need to check if parent exists - if it doesn't need to create a parent
            parentName = zooKeeper.create(WORKERS_PARENT_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        else{//is this superflous??
            parentName = "/workers";
        }
        //check if workers need to be launched, launch startworker if necessary
        for(int i = 0; i <numberOfWorkers; i++){
            checkRunningWorkers();
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
        //only one thread can access this block of code at a time, zooKeeper is being used a lock
        synchronized (zooKeeper) {
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
                    System.out.println("Received node deleted event");
                    checkRunningWorkers();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    public void checkRunningWorkers() throws KeeperException, InterruptedException, IOException {
        //Stat workerStat = null;//need to look at Stat

        //if (workerStat == null) {
            List<String> workers = zooKeeper.getChildren(parentName, false);
            workersNo = workers.size();
            if(workersNo < numberOfWorkers)
            {
                startWorker();
            }
        //}

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

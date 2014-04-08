package com.mongodb.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class YarnApp {
    private static final Log LOG = LogFactory.getLog(YarnApp.class);
    private static final String BASE_PATH = System.getProperty("mongo.export.path", "/mongodb/");

    FileSystem fileSystem;
    @Parameter(names = {"--name"}, description = "The name of the application to list in the resource manager")
    private String applicationName;
    @Parameter(names = {"--mongoUri"}, converter = MongoURIConverter.class,
               description = "The mongo URI to use when connecting.  Use of this setting causes all other"
                             + " connection parameters to be ignored.")
    private MongoClientURI mongoUri;
    @Parameter(names = {"--host", "-h"}, description = "The host to connect to use when connecting.  Default: localhost")
    private String host = "localhost";
    @Parameter(names = {"--port", "-p"}, description = "The port to connect to use when connecting.  Default: 27017")
    private Integer port = 27017;
    @Parameter(names = {"--collection", "-c"}, description = "The collection to export")
    private String collection;
    @Parameter(names = {"--database", "-db"}, description = "The database to connect to")
    private String database;
    @Parameter(names = {"--query", "-q"}, description = "The query to use for exporting.  No query means exporting the entire collection.")
    private String query;
    private final Configuration conf;
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numRequestedContainers = new AtomicInteger();
    private boolean success;
    private int numTotalContainers = 1;
    private AMRMClientAsync<ContainerRequest> amrmClientAsync;
    private boolean done;
    private AMRMClient<ContainerRequest> amrmClient;

    public YarnApp(final String[] args) throws IOException {
        conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("mapred-site.xml");
        new JCommander(this, args);
        fileSystem = FileSystem.get(conf);
        if (mongoUri != null) {
            database = mongoUri.getDatabase();
            collection = mongoUri.getCollection();
        }
    }

    public void run() throws IOException, YarnException {
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amrmClientAsync.init(conf);
        amrmClientAsync.start();
//
        amrmClient = AMRMClient.createAMRMClient();
        amrmClient.init(conf);
        amrmClient.start();
        amrmClient.registerApplicationMaster(applicationName, -1, null);

                MongoClient client = mongoUri != null ? new MongoClient(mongoUri) : new MongoClient(host, port);
                DBCollection dbCollection = client.getDB(database).getCollection(collection);
        
                String query = System.getProperty(MongoConfigUtil.INPUT_QUERY);
                DBCursor cursor = query == null ? dbCollection.find() : dbCollection.find((DBObject) JSON.parse(query));
                DefaultDBEncoder encoder = new DefaultDBEncoder();
        
                Path path = new Path(BASE_PATH + "/" + collection);
                LOG.info("***************** path = " + path);
                fileSystem.delete(path, true);
                FSDataOutputStream out = fileSystem.create(path);
        
                while (cursor.hasNext()) {
                    out.write(encoder.encode(cursor.next()));
                }
        
                out.close();
//                finish();
        amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
        amrmClient.stop();
    }

    private void finish() {
        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        //      for (Thread launchThread : launchThreads) {
        //        try {
        //          launchThread.join(10000);
        //        } catch (InterruptedException e) {
        //          LOG.info("Exception thrown in thread join: " + e.getMessage());
        //          e.printStackTrace();
        //        }
        //      }

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus = FinalApplicationStatus.SUCCEEDED;
        success = true;
        try {
            amrmClientAsync.unregisterApplicationMaster(appStatus, null, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        amrmClientAsync.stop();
    }

    public static void main(String[] args) throws IOException, YarnException {
        new YarnApp(args).run();
        //        System.exit(12);
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(format("Got container status for containerID=%s,\n\tstate=%s,\n\texitStatus=%d\n\tdiagnostics=%s",
                                containerStatus.getContainerId(), containerStatus.getState(), containerStatus.getExitStatus(),
                                containerStatus.getDiagnostics()));

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        // shell script failed
                        // counts as completed
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                        // we do not need to release the container as it would be done
                        // by the RM
                    }
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info(format("Container completed successfully., containerId=%s", containerStatus.getContainerId()));
                }
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            return (float) numCompletedContainers.get() / numTotalContainers;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            amrmClientAsync.stop();
        }
    }

}

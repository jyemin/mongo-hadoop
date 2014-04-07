package com.mongodb.yarn;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BsonExportClient {
    private static final Log LOG = LogFactory.getLog(BsonExportClient.class);

    private final YarnClient yarnClient;
    private final Configuration conf;
    private String appMasterJar;
    private String applicationName = "BSON Collection Exporter";

    public BsonExportClient() {
        conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
    }

    public boolean run() throws IOException, YarnException {
        yarnClient.start();

        System.out.println("yarnClient.getAllQueues() = " + yarnClient.getAllQueues());
        QueueInfo queueInfo = yarnClient.getQueueInfo("default");
        System.out.println("Queue info"
                           + ", queueName=" + queueInfo.getQueueName()
                           + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                           + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                           + ", queueApplicationCount=" + queueInfo.getApplications().size()
                           + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setApplicationName(applicationName);

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources			
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        System.out.println("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem 
        // Create a local resource to point to the destination jar path 
        FileSystem fs = FileSystem.get(conf);
        String jar = JarFinder.getJar(YarnApp.class);
        addToLocalResources(fs, jar, new File(jar).getName(), appId.getId(), localResources, null);

        amContainer.setLocalResources(localResources);
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$())
                                         .append(File.pathSeparatorChar).append("./*");
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");
        Map<String, String> env = new HashMap<String, String>();
        env.put("CLASSPATH", classPathEnv.toString());

        amContainer.setEnvironment(env);
        amContainer.setCommands(Arrays.asList(format("%s/bin/java %s", Environment.JAVA_HOME.$(), YarnApp.class.getName())));

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(10);
        capability.setVirtualCores(1);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);
        appContext.setQueue(queueInfo.getQueueName());

        yarnClient.submitApplication(appContext);

        return monitorApplication(appId);
    }

    private boolean monitorApplication(final ApplicationId appId)
        throws YarnException, IOException {

        try {
            Awaitility.await()
                      .atMost(2, MINUTES)
                      .pollInterval(1, SECONDS)
                      .until(new Callable<Boolean>() {
                          @Override
                          public Boolean call() throws Exception {
                              ApplicationReport report = yarnClient.getApplicationReport(appId);

                              log(report, appId);

                              YarnApplicationState state = report.getYarnApplicationState();
                              FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
                              switch (state) {
                                  case FINISHED:
                                      if (FinalApplicationStatus.SUCCEEDED != dsStatus) {
                                          System.out.println(format("Application did finished unsuccessfully. YarnState=%s, " +
                                                                    "DSFinalStatus=%s. Breaking monitoring loop",
                                                                    state.toString(),
                                                                    dsStatus.toString()
                                                                   ));
                                          return true;
                                      }
                                  case KILLED:
                                  case FAILED:
                                      System.out.println(format("Application did not finish. YarnState=%s, " +
                                                                "DSFinalStatus=%s. Breaking monitoring loop",
                                                                state.toString(), dsStatus.toString()
                                                               ));
                                      return true;
                              }
                              return false;
                          }
                      });
            return true;
        } catch (ConditionTimeoutException e) {
            e.printStackTrace();
            yarnClient.killApplication(appId);
            return false;
        }
    }

    private void log(final ApplicationReport report, final ApplicationId appId) {
        System.out.println(format("Got application report from ASM for, appId=%d, clientToAMToken=%s, "
                                  + "appDiagnostics=%s, appMasterHost=%s, appQueue=%s, appMasterRpcPort=%d,"
                                  + " appStartTime=%d, yarnAppState=%s, distributedFinalState=%s, "
                                  + "appTrackingUrl=%s, appUser=%s", appId.getId(), report.getClientToAMToken(),
                                  report.getDiagnostics(), report.getHost(), report.getQueue(),
                                  report.getRpcPort(), report.getStartTime(),
                                  report.getYarnApplicationState().toString(),
                                  report.getFinalApplicationStatus().toString(), report.getTrackingUrl(),
                                  report.getUser()
                                 ));
    }

    private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, int appId,
                                     Map<String, LocalResource> localResources, String resources) throws IOException {
        String suffix = applicationName + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE,
                                                         LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
                                                         scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    public static void main(String[] args) throws IOException, YarnException {
        BsonExportClient bsonExportClient = new BsonExportClient();
        bsonExportClient.run();
    }
}

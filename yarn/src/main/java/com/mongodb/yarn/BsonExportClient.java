package com.mongodb.yarn;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.jayway.awaitility.core.ConditionTimeoutException;
import com.mongodb.MongoClientURI;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BsonExportClient {
    private static final Logger LOG = LoggerFactory.getLogger(BsonExportClient.class);

    private final YarnClient yarnClient;
    private final Configuration conf;
    @Parameter(names = {"--name"}, description = "The name of the application to list in the resource manager")
    private String applicationName;
    @Parameter(names = {"--mongoUri"}, description = "The mongo URI to use when connecting.  Use of this setting causes all other " +
                                                     "connection parameters to be ignored.", converter = MongoURIConverter.class)
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
    @Parameter(names = {"--timeout", "-t"}, description = "The time in minutes to wait for the job to complete before forcefully " +
                                                          "terminating it.  Default:  30", converter = DurationConverter.class)
    
    private Duration timeout = new Duration(300, SECONDS);

    public BsonExportClient(final String[] args) {
        conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("mapred-site.xml");
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        new JCommander(this, args);
        validateOptions();
        if (applicationName == null) {
            applicationName = format("BSON Collection Exporter (%s/%s)", getCollectionInformation());
        }
    }

    private Object[] getCollectionInformation() {
        if (mongoUri != null) {
            return new String[]{mongoUri.getDatabase(), mongoUri.getCollection()};
        } else {
            return new String[]{database, collection};
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public boolean run() throws IOException, YarnException {
        yarnClient.start();

        QueueInfo queueInfo = yarnClient.getQueueInfo("default");

        YarnClientApplication app = yarnClient.createApplication();
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setApplicationName(applicationName);

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

//        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
//        addLog4jFile(appId, localResources, FileSystem.get(conf));
//        amContainer.setLocalResources(localResources);

        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$())
                                         .append(File.pathSeparatorChar).append("./*");
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");
        Map<String, String> env = new HashMap<String, String>();
        classPathEnv.append(System.getProperty("java.class.path"));
        env.put("CLASSPATH", classPathEnv.toString());

        amContainer.setEnvironment(env);
        amContainer.setCommands(getCommand());

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(10);
        capability.setVirtualCores(1);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);
        appContext.setQueue(queueInfo.getQueueName());

        yarnClient.submitApplication(appContext);

        return monitorApplication(appId);
    }

    private void addLog4jFile(final ApplicationId appId, final Map<String, LocalResource> localResources, final FileSystem fs)
        throws IOException {
        File log4jPropFile = new File(System.getProperty("java.io.tmpdir"), "mongo_hadoop_log4j.properties");
        PrintWriter fileWriter = new PrintWriter(log4jPropFile);
        // set the output to DEBUG level
        fileWriter.write("log4j.rootLogger=debug,stdout");
        fileWriter.close();
        addToLocalResources(fs, log4jPropFile.getAbsolutePath(), "log4j.properties", appId.getId(), localResources, null);
    }

    private void addJar(final ApplicationId appId, final Map<String, LocalResource> localResources, final FileSystem fs,
                        final Class klass)
        throws IOException {
        String jar = JarFinder.getJar(klass);
        addToLocalResources(fs, jar, new File(jar).getName(), appId.getId(), localResources, null);
    }

    private List<String> getCommand() {
        StringBuilder command = new StringBuilder(format("%s/bin/java %s ", Environment.JAVA_HOME.$(), YarnApp.class.getName()));
        append(command, "--name", "'" + applicationName + "'");
        if(mongoUri != null) {
            append(command, "--mongoUri", mongoUri.toString());
        } else {
            append(command, "--host", host);
            append(command, "--port", port.toString());
            append(command, "--database", database);
            append(command, "--collection", collection);
        }
        if(query != null) {
            append(command, "--query", query);
            
        }
        return Arrays.asList(command.toString());
    }

    private void append(final StringBuilder command, final String option, final String value) {
        if (value != null) {
            command.append(format("%s %s ", option, value));
        }
    }

    private void validateOptions() {
        if (!notNull(mongoUri, host, port, database, collection, query)) {
            throw new IllegalArgumentException("No connection information was given");
        }
        if (mongoUri != null && notNull(database, collection)) {
            LOG.warn("A mongo URI was so all other connection options will be ignored.");
            host = null;
            port = null;
            database = null;
            collection = null;
            query = null;
        } else if (mongoUri == null && !allPresent(database, collection)) {
            throw new IllegalArgumentException("If mongoUri is not present all other connection options must be given.");
        }
    }

    private boolean allPresent(final Object... objects) {
        for (Object object : objects) {
            if (object == null) {
                return false;
            }
        }
        return true;
    }

    private boolean notNull(final Object... objects) {
        for (Object object : objects) {
            if (object != null) {
                return true;
            }
        }
        return false;
    }

    private boolean monitorApplication(final ApplicationId appId)
        throws YarnException, IOException {

        try {
            final AtomicBoolean success = new AtomicBoolean(false);
            Awaitility.await()
                      .atMost(timeout)
                      .pollInterval(5, SECONDS)
                      .until(new Callable<Boolean>() {
                          @Override
                          public Boolean call() throws Exception {
                              ApplicationReport report = yarnClient.getApplicationReport(appId);

                              log(report, appId);

                              YarnApplicationState state = report.getYarnApplicationState();
                              FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
                              success.set(FinalApplicationStatus.SUCCEEDED == dsStatus);
                              switch (state) {
                                  case FINISHED:
                                      if (FinalApplicationStatus.SUCCEEDED != dsStatus) {
                                          System.out.println(format("Application did finished unsuccessfully. YarnState=%s, " +
                                                                    "DSFinalStatus=%s. Breaking monitoring loop",
                                                                    state.toString(), dsStatus.toString()
                                                                   ));
                                          return true;
                                      }
                                      return true;
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
            return success.get();
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
        BsonExportClient bsonExportClient = new BsonExportClient(args);
        bsonExportClient.run();
    }

    private static class DurationConverter implements IStringConverter<Duration> {
        @Override
        public Duration convert(final String value) {
            return new Duration(Integer.parseInt(value), SECONDS);
        }
    }
}

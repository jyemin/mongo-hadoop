package com.mongodb.yarn;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.Callable;

public class TestYarnApp {
    private static final Log LOG = LogFactory.getLog(TestYarnApp.class);

    private MiniYARNCluster yarnCluster;
    private MiniDFSCluster dfsCluster;

    @Before
    public void setUpCluster() throws IOException {
/*
        if (dfsCluster == null) {
            Configuration conf = new Configuration();
            conf.addResource("core-site.xml");
            conf.addResource("hdfs-site.xml");
            conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "/tmp/dfs");
            dfsCluster = new MiniDFSCluster.Builder(conf)
                             .build();

            dfsCluster.waitActive();
        }
*/
        if (yarnCluster == null) {
            yarnCluster = new MiniYARNCluster(getClass().getSimpleName(), 1, 1, 1);
            Configuration conf = getConfiguration();
            yarnCluster.init(conf);
            yarnCluster.start();
            NodeManager nm = yarnCluster.getNodeManager(0);
            waitForNMToRegister(nm);

            URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
            if (url == null) {
                throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
            }
            Configuration yarnClusterConfig = yarnCluster.getConfig();
            yarnClusterConfig.set("yarn.application.classpath", new File(url.getPath()).getParent());
            //write the document to a buffer (not directly to the file, as that
            //can cause the file being written to get read -which will then fail.
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            yarnClusterConfig.writeXml(bytesOut);
            bytesOut.close();
            //write the bytes to the file in the classpath
            OutputStream os = new FileOutputStream(new File(url.getPath()));
            os.write(bytesOut.toByteArray());
            os.close();
        }
    }

    private Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.addResource("yarn-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("core-site.xml");
        return conf;
    }

    @Test
    public void connect() throws IOException {
        //        YarnClient yarnClient = YarnClient.createYarnClient();
        //        yarnClient.init(yarnCluster.getConfig());
        //        yarnClient.start();

        System.setProperty(MongoConfigUtil.INPUT_URI, "mongodb://localhost:27017/mongo_hadoop.messages");
        new YarnApp().run();
    }

    @Test
    public void deployYarnApp() {
        Configuration conf = getConfiguration();
        YarnRPC rpc = YarnRPC.create(conf);
        ApplicationClientProtocol applicationsManager;
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        InetSocketAddress rmAddress = 
            NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
        LOG.info("Connecting to ResourceManager at " + rmAddress);
        Configuration appsManagerServerConf = new Configuration(conf);
        applicationsManager = ((ApplicationClientProtocol) rpc.getProxy(
                                                                           ApplicationClientProtocol.class,
                                                                           rmAddress,
                                                                           appsManagerServerConf));
    }

    private static void waitForNMToRegister(NodeManager nm) {
        final ContainerManagerImpl cm = ((ContainerManagerImpl) nm.getNMContext().getContainerManager());
        Awaitility.await()
                  .atMost(Duration.TWO_MINUTES)
                  .until(new Callable<Boolean>() {
                      @Override
                      public Boolean call() throws Exception {
                          return !cm.getBlockNewContainerRequestsStatus();
                      }
                  });
    }

}

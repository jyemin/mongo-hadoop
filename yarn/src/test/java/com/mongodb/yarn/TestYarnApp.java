package com.mongodb.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URL;

public class TestYarnApp {
    private MiniYARNCluster yarnCluster;

    @Before
    public void setUpYarnCluster() {
        if (yarnCluster == null) {
            yarnCluster = new MiniYARNCluster(
              TestDistributedShell.class.getSimpleName(), 1, 1, 1, 1, true);
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
}

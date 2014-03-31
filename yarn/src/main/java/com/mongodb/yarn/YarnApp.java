package com.mongodb.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;

import java.net.InetSocketAddress;

public class YarnApp {
    private static final Log LOG = LogFactory.getLog(YarnApp.class);

    public static void main(String[] args) {
        YarnClient client = YarnClient.createYarnClient();
        client.start();
        
        ApplicationClientProtocol applicationsManager; 
            YarnConfiguration yarnConf = new YarnConfiguration(conf);
            InetSocketAddress rmAddress = 
                NetUtils.createSocketAddr(yarnConf.get(
                                                          YarnConfiguration.RM_ADDRESS,
                                                          YarnConfiguration.DEFAULT_RM_ADDRESS));             
            LOG.info("Connecting to ResourceManager at " + rmAddress);
            Configuration appsManagerServerConf = new Configuration(conf);
            appsManagerServerConf.setClass(
                YarnConfiguration.YARN_SECURITY_INFO,
                ClientRMSecurityInfo.class, SecurityInfo.class);
            applicationsManager = ((ApplicationClientProtocol) rpc.getProxy(
                ApplicationClientProtocol.class, rmAddress, appsManagerServerConf));    
    }}

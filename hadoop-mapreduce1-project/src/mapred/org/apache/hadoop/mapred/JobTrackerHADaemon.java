/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import com.google.protobuf.BlockingService;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;

public class JobTrackerHADaemon {
  
  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }
  
  private static final Log LOG =
    LogFactory.getLog(JobTrackerHADaemon.class);
  
  private Configuration conf;
  private JobTrackerHAServiceProtocol proto;
  private RPC.Server rpcServer;
  
  public JobTrackerHADaemon(Configuration conf) {
    this.conf = conf;
  }
  
  public Configuration getConf() {
    return conf;
  }

  public void start() throws IOException {
    
    Configuration jtConf = new Configuration(conf);
    String logicalName = HAUtil.getLogicalName(jtConf);
    String jtId = HAUtil.getJobTrackerId(jtConf);
    HAUtil.setGenericConf(jtConf, logicalName, jtId, HAUtil.JOB_TRACKER_SPECIFIC_KEYS);
    
    this.proto = new JobTrackerHAServiceProtocol(jtConf);
    
    RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
        ProtobufRpcEngine.class);
    
    HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator = 
      new HAServiceProtocolServerSideTranslatorPB(proto);
    BlockingService haPbService = HAServiceProtocolService
        .newReflectiveBlockingService(haServiceProtocolXlator);
    
    WritableRpcEngine.ensureInitialized();
    
    InetSocketAddress rpcAddr = HAUtil.getJtHaRpcAddress(conf);

    this.rpcServer = RPC.getServer(HAServiceProtocolPB.class, haPbService,
        rpcAddr.getHostName(), rpcAddr.getPort(), conf);
    
    this.rpcServer.start();
    
    // set port in config
    int port = rpcServer.getListenerAddress().getPort();
    HAUtil.setJtHaRpcAddress(conf, rpcAddr.getHostName() + ":" + port);
    LOG.info("Started " + getClass().getSimpleName() + " on port " + port);
  }
  
  public void makeActive() throws IOException {
    proto.transitionToActive(null);
  }
  
  public JobTracker getJobTracker() {
    return proto.getJobTracker();
  }
  
  public JobTrackerHAServiceProtocol getJobTrackerHAServiceProtocol() {
    return proto;
  }
  
  public HAServiceStatus getServiceStatus() throws AccessControlException,
      IOException {
    return proto.getServiceStatus();
  }
  
  public void stop() throws IOException {
    proto.stop();
    rpcServer.stop();
  }
  
  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      rpcServer.join();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
  
  public static void startService() throws Exception {
    JobTrackerHADaemon daemon = new JobTrackerHADaemon(new Configuration());
    daemon.start();
    daemon.join();
  }
  
  public static void main(String[] args) {
    try {
      startService();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
  
}

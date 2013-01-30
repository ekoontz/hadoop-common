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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class JobTrackerHADaemon {
  
  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }
  
  private static final Log LOG =
    LogFactory.getLog(JobTrackerHADaemon.class);
  
  private Configuration conf;
  private JobTrackerRunner jtRunner;
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

    // Login HA daemon, if auth is not kerberos login() is a NOP
    InetSocketAddress addr = HAUtil.getJtHaRpcAddress(conf, jtId);
    String localMachine = addr.getHostName();
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, JobTracker.JT_KEYTAB_FILE, JobTracker.JT_USER_NAME, localMachine);
    
    // To avoid the JT from doing a new login() as re-login seems to be destructive
    JobTracker.loggedIn = true;
    
    //Using a thread outside of all login context to start/stop the JT
    //otherwise the credentials of the UGI making the RPC call to activate
    //get in the way breaking things.
    jtRunner = new JobTrackerRunner();
    jtRunner.start();
            
    this.proto = new JobTrackerHAServiceProtocol(jtConf, jtRunner);
    
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

    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      rpcServer.refreshServiceAcl(conf, new MapReducePolicyProvider());
    }

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
    return jtRunner.jt;
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
  
  public static class JobTrackerRunner extends Thread {
    private JobTracker jt;
    private JobConf conf;
    private volatile CountDownLatch startLatch;
    private volatile CountDownLatch startedLatch;
    private volatile boolean jtClosing;
    private Thread jtThread;
    
    public JobTrackerRunner() {
      super(JobTrackerRunner.class.getSimpleName());
      setDaemon(true);
      startLatch = new CountDownLatch(1);
      startedLatch = new CountDownLatch(1);
    }

    public void run() {
      while (true) {
        try {
          startLatch.await();
          jt = JobTracker.startTracker(conf);
          jtThread = new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  jt.offerService();
                } catch (Throwable t) {
                  if (jtClosing) {
                    LOG.info("Exception while closing jobtracker", t);
                  } else {
                    doImmediateShutdown(t);
                  }
                }
              }
            }, JobTrackerRunner.class.getSimpleName() + "-JT");
          jtThread.start();
          waitForRunningState();
          startedLatch.countDown();
          jtThread.join();
        } catch (Throwable t) {
          doImmediateShutdown(t);
        } finally {
          startedLatch.countDown();
          startLatch = new CountDownLatch(1);
          startedLatch = new CountDownLatch(1);
        }
      }
    }

    private void waitForRunningState() {
      while (jt != null && jt.getClusterStatus(false).getJobTrackerStatus()
          != Cluster.JobTrackerStatus.RUNNING) {
        LOG.info("Waiting for jobtracker RUNNING state");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
      }
    }

    /**
     * Shutdown the JT immediately in an ungraceful way. Used when it would be
     * unsafe for the JT to continue operating, e.g. during a failed HA state
     * transition.
     *
     * @param t exception which warrants the shutdown. Printed to the JT log
     *          before exit.
     * @throws org.apache.hadoop.util.ExitUtil.ExitException thrown only for testing.
     */
    private synchronized void doImmediateShutdown(Throwable t)
      throws ExitUtil.ExitException {
      String message = "Error encountered requiring JT shutdown. " +
        "Shutting down immediately.";
      try {
        LOG.fatal(message, t);
      } catch (Throwable ignored) {
        // This is unlikely to happen, but there's nothing we can do if it does.
      }
      terminate(1, t);
    }

    public void startJobTracker(JobConf conf) throws InterruptedException {
      if (jt == null) {
        this.conf = conf;
        startLatch.countDown();
        startedLatch.await();
      }
    }
    
    public void stopJobTracker() {
      try {
        if (jt != null) {
          jtClosing = true;
          jt.close();
        }
        if (jtThread != null) {
          jtThread.join();
        }
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
      jt = null;
      jtClosing = false;
      jtThread = null;
    }

    public boolean jtThreadIsNotAlive() {
      return jtThread == null || !jtThread.isAlive();
    }

    @VisibleForTesting
    Thread getJobTrackerThread() {
      return jtThread;
    }


  }
}

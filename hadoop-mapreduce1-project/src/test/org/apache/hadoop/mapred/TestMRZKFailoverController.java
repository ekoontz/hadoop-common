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

import static org.apache.hadoop.mapred.HAUtil.addKeySuffixes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HealthMonitor;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.ZKFCTestUtil;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.mapred.HAUtil;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTrackerHADaemon;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.tools.MRHAAdmin;
import org.apache.hadoop.mapred.tools.MRZKFailoverController;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;

public class TestMRZKFailoverController extends ClientBaseWithFixes {
  
  private static final Log LOG = 
    LogFactory.getLog(TestMRZKFailoverController.class);
  
  private static final Path TEST_DIR = new Path("/tmp/tst");

  private Configuration conf;
  private MiniMRHACluster cluster;
  private TestContext ctx;
  private ZKFCThread thr1, thr2;
  
  @Before
  public void setup() throws Exception {
    String logicalName = MiniMRHACluster.LOGICAL_NAME;
    conf = new Configuration();
    conf.set(addKeySuffixes(ZKFailoverController.ZK_QUORUM_KEY, logicalName), hostPort);
    conf.set(HAUtil.MR_HA_FENCING_METHODS_KEY,
        AlwaysSucceedFencer.class.getName());
    conf.setBoolean(HAUtil.MR_HA_AUTO_FAILOVER_ENABLED_KEY, true);

    // Turn off IPC client caching, so that the suite can handle
    // the restart of the daemons between test cases.
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    
    conf.setInt(addKeySuffixes(HAUtil.MR_HA_ZKFC_PORT_KEY, logicalName, "jt1"), 10003);
    conf.setInt(addKeySuffixes(HAUtil.MR_HA_ZKFC_PORT_KEY, logicalName, "jt2"), 10004);

    cluster = new MiniMRHACluster(conf);

    ctx = new TestContext();
    ctx.addThread(thr1 = new ZKFCThread(ctx, 0));
    assertEquals(0, thr1.zkfc.run(new String[]{"-formatZK"}));

    thr1.start();
    waitForHAState(0, HAServiceState.ACTIVE);
    
    ctx.addThread(thr2 = new ZKFCThread(ctx, 1));
    thr2.start();
    
    cluster.startTaskTracker(0, 1);
    cluster.waitActive();
    
    // Wait for the ZKFCs to fully start up
    ZKFCTestUtil.waitForHealthState(thr1.zkfc,
        HealthMonitor.State.SERVICE_HEALTHY, ctx);
    ZKFCTestUtil.waitForHealthState(thr2.zkfc,
        HealthMonitor.State.SERVICE_HEALTHY, ctx);
  }
  
  @After
  public void shutdown() throws Exception {
    cluster.shutdown();
    
    if (thr1 != null) {
      thr1.interrupt();
    }
    if (thr2 != null) {
      thr2.interrupt();
    }
    if (ctx != null) {
      ctx.stop();
    }
  }
  
  @Test(timeout=60000)
  public void testFailoverWhileRunningJob() throws Exception {
    LOG.info("Running job failover test");

    // Inspired by TestRecoveryManager#testJobResubmission
    
    FileUtil.fullyDelete(new File("/tmp/tst"));
    
    // start a job on jt1
    JobConf job1 = new JobConf(conf);
    String signalFile = new Path(TEST_DIR, "signal").toString();
    UtilsForTests.configureWaitingJobConf(job1, new Path(TEST_DIR, "input"),
        new Path(TEST_DIR, "output3"), 2, 0, "test-resubmission", signalFile,
        signalFile);
    JobClient jc = new JobClient(job1);
    RunningJob rJob1 = jc.submitJob(job1);
    while (rJob1.mapProgress() < 0.5f) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be 50% done: " +
          rJob1.mapProgress());
      UtilsForTests.waitFor(500);
    }
    LOG.info("Waiting for job " + rJob1.getID() + " to be 50% done: " +
        rJob1.mapProgress());
    
    // Shut the first JT down, causing automatic failover
    LOG.info("Shutting down jt1");
    cluster.shutdownJobTracker(0);
    
    // allow job to complete
    FileSystem fs = FileSystem.getLocal(conf);
    fs.create(new Path(TEST_DIR, "signal"));
    while (!rJob1.isComplete()) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be successful: " +
          rJob1.mapProgress());
      UtilsForTests.waitFor(500);
    }
    assertTrue("Job should be successful", rJob1.isSuccessful());
  }
  
  @Test(timeout=60000)
  public void testManualFailover() throws Exception {
    LOG.info("Running manual failover test");
    thr2.zkfc.getLocalTarget().getZKFCProxy(conf, 15000).gracefulFailover();
    waitForHAState(0, HAServiceState.STANDBY);
    waitForHAState(1, HAServiceState.ACTIVE);

    thr1.zkfc.getLocalTarget().getZKFCProxy(conf, 15000).gracefulFailover();
    waitForHAState(0, HAServiceState.ACTIVE);
    waitForHAState(1, HAServiceState.STANDBY);
  }
  
  @Test(timeout=60000)
  public void testManualFailoverWithMRHAAdmin() throws Exception {
    LOG.info("Running manual failover test with MRHAAdmin");
    MRHAAdmin tool = new MRHAAdmin();
    tool.setConf(conf);
    assertEquals(0, 
        tool.run(new String[]{"-failover", "jt1", "jt2"}));
    waitForHAState(0, HAServiceState.STANDBY);
    waitForHAState(1, HAServiceState.ACTIVE);
    assertEquals(0,
        tool.run(new String[]{"-failover", "jt2", "jt1"}));
    waitForHAState(0, HAServiceState.ACTIVE);
    waitForHAState(1, HAServiceState.STANDBY);
  }
  
  private void waitForHAState(int jtidx, final HAServiceState state)
      throws TimeoutException, InterruptedException {
    final JobTrackerHADaemon jtHaDaemon = cluster.getJobTrackerHaDaemon(jtidx);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return jtHaDaemon.getServiceStatus().getState() == state;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        }
      }
    }, 50, 5000);
  }

  /**
   * Test-thread which runs a ZK Failover Controller corresponding
   * to a given JobTracker in the minicluster.
   */
  private class ZKFCThread extends TestingThread {
    private final MRZKFailoverController zkfc;

    public ZKFCThread(TestContext ctx, int idx) {
      super(ctx);
      this.zkfc = MRZKFailoverController.create(
          cluster.getJobTrackerHaDaemon(idx).getConf());
    }

    @Override
    public void doWork() throws Exception {
      try {
        assertEquals(0, zkfc.run(new String[0]));
      } catch (InterruptedException ie) {
        // Interrupted by main thread, that's OK.
      }
    }
  }

}

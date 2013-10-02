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

import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.util.ExitUtil;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests state transition from active->standby, and manual failover
 * and failback between two jobtrackers.
 */
public class TestHAStateTransitions {
  
  private static final Log LOG = 
    LogFactory.getLog(TestHAStateTransitions.class);

  private static final Path TEST_DIR = new Path("/tmp/tst");
  
  private static final StateChangeRequestInfo REQ_INFO = new StateChangeRequestInfo(
      RequestSource.REQUEST_BY_USER_FORCED);

  private MiniMRHACluster cluster;
  private JobTrackerHADaemon jt1;
  private JobTrackerHADaemon jt2;
  private JobTrackerHAServiceTarget target1;
  private JobTrackerHAServiceTarget target2;
  private Configuration conf;
  
  private void startCluster() throws Exception {
    startCluster(new Configuration());
  }
  private void startCluster(Configuration conf) throws Exception {
    FileUtil.fullyDelete(new File("/tmp/tst"));
    ExitUtil.disableSystemExit();
    this.conf = conf;
    conf.set(HAUtil.MR_HA_FENCING_METHODS_KEY,
        AlwaysSucceedFencer.class.getName());
    cluster = new MiniMRHACluster(conf);
    cluster.getJobTrackerHaDaemon(0).makeActive();
    cluster.startTaskTracker(0, 1);
    cluster.waitActive();

    jt1 = cluster.getJobTrackerHaDaemon(0);
    jt2 = cluster.getJobTrackerHaDaemon(1);
    target1 = new JobTrackerHAServiceTarget(jt1.getConf());
    target2 = new JobTrackerHAServiceTarget(jt2.getConf());
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  @Test(timeout=60000)
  public void testClientFailover() throws Exception {
    LOG.info("Running testClientFailover");
    startCluster();

    // Test with client. c.f. HATestUtil.setFailoverConfigurations
    JobClient jc = new JobClient(conf);
    assertEquals("client sees jt running", JobTrackerStatus.RUNNING,
        jc.getClusterStatus().getJobTrackerStatus());

    // failover to jt2
    FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    fc.failover(target1, target2, false, false);
    
    cluster.waitActive();
    
    assertEquals("jt2 running", JobTrackerStatus.RUNNING,
        jt2.getJobTracker().getClusterStatus().getJobTrackerStatus());
    assertNull("jt1 not running", jt1.getJobTracker());
    
    assertEquals("client still sees jt running", JobTrackerStatus.RUNNING,
        jc.getClusterStatus().getJobTrackerStatus());
  }
  
  @Test(timeout=60000)
  public void testFailoverWhileRunningJob() throws Exception {
    LOG.info("Running testFailoverWhileRunningJob");
    startCluster();

    // Inspired by TestRecoveryManager#testJobResubmission
    
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
    
    // failover to jt2
    FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    fc.failover(target1, target2, false, false);
    
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
  public void testTransitionToCurrentStateIsANop() throws Exception {
    LOG.info("Running testTransitionToCurrentStateIsANop");
    startCluster();

    JobTracker existingJt = jt1.getJobTracker();
    jt1.getJobTrackerHAServiceProtocol().transitionToActive(REQ_INFO);
    assertSame("Should not create a new JobTracker", existingJt,
        jt1.getJobTracker());
    jt1.getJobTrackerHAServiceProtocol().transitionToStandby(REQ_INFO);
    // Transitioning to standby for a second time should not throw an exception
    jt1.getJobTrackerHAServiceProtocol().transitionToStandby(REQ_INFO);
  }
  
  @Test(timeout=60000)
  public void testSecondActiveFencesFirst() throws Exception {
    LOG.info("Running testSecondActiveFencesFirst");
    startCluster();
    
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
    
    // force jt2 to become active, even though jt1 is still active
    jt2.getJobTrackerHAServiceProtocol().transitionToActive(REQ_INFO);
    
    // wait for jt1 to detect that it is no longer active and fence itself
    UtilsForTests.waitFor(1500);
    
    // jt1 should have exited
    assertTrue(ExitUtil.terminateCalled());
    jt1.getJobTracker().close(); // shut it down manually
    
    // allow job1 to complete
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
  public void testSecondActiveCausesFirstToRejectJob() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(HAUtil.MR_HA_ACTIVE_CHECK_MILLIS, Long.MAX_VALUE); // never check
    startCluster(conf);

    // Ensure client always uses jt1
    conf.set("mapred.job.tracker",
        jt1.getJobTracker().getConf().get("mapred.job.tracker"));

    // run a job on jt1
    SleepJob job = new SleepJob();
    job.setConf(conf);
    assertEquals("Job succeeded", 0, job.run(1, 0, 1, 1, 1, 1));

    // force jt2 to become active, even though jt1 is still active
    jt2.getJobTrackerHAServiceProtocol().transitionToActive(REQ_INFO);

    // try to submit a job to jt1, which should fail even though still running
    job = new SleepJob();
    job.setConf(conf);
    JobConf jobConf = job.setupJobConf(1, 0, 1, 1, 1, 1);
    JobClient jc = new JobClient(jobConf);
    try {
      jc.submitJob(jobConf);
      fail("Job submission should fail");
    } catch (Exception e) {
      // expected
    }
  }
}

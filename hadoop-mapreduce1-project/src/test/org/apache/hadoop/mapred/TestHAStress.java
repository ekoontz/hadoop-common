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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.util.ExitUtil;
import org.junit.*;

public class TestHAStress {

  private static final Log LOG = 
    LogFactory.getLog(TestHAStress.class);

  private static final StateChangeRequestInfo REQ_INFO = new StateChangeRequestInfo(
      RequestSource.REQUEST_BY_USER_FORCED);
  
  private static final int NUM_THREADS = 2;
  private static final long RUNTIME = 35000;

  private MiniMRHACluster cluster;
  private JobTrackerHADaemon jt1;
  private JobTrackerHADaemon jt2;
  private JobTrackerHAServiceTarget target1;
  private JobTrackerHAServiceTarget target2;
  private Configuration conf;
  
  private class FailoverThread extends RepeatingTestThread {
    
    private long msBetweenFailovers = 10000;
    private FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    
    public FailoverThread(TestContext ctx) {
      super(ctx);
      setName(getClass().getName());
    }
    
    @Override
    public void doAnAction() throws Exception {
      System.out.println("==============================\n" +
          "Failing over from 0->1\n" +
          "==================================");
      fc.failover(target1, target2, false, false);
      
      Thread.sleep(msBetweenFailovers);
      System.out.println("==============================\n" +
          "Failing over from 1->0\n" +
          "==================================");

      fc.failover(target2, target1, false, false);
      Thread.sleep(msBetweenFailovers);
    }
  }
  
  private static class JobSubmitterThread extends RepeatingTestThread {
    private Configuration conf;
    public JobSubmitterThread(TestContext ctx, Configuration conf) {
      super(ctx);
      this.conf = conf;
      setName(getClass().getName());
    }

    @Override
    public void doAnAction() throws Exception {
      System.out.println("==============================\n" +
          "Submitting job\n" +
          "==================================");
      SleepJob job = new SleepJob();
      job.setConf(conf);
      JobConf jobConf = job.setupJobConf(1, 0, 1, 1, 1, 1);
      JobClient jc = new JobClient(jobConf);
      RunningJob rj;
      try {
        rj = jc.submitJob(jobConf);
      } catch (IOException e) {
        // Job submission is not idempotent, so ignore a failure
        System.out.println("==============================\n" +
            "Job submission failed. Ignore.\n" +
            "==================================");
        return;
      }
      System.out.println("==============================\n" +
          "Successfully submitted job " + rj.getJobID() + "\n" +
          "==================================");
      if (!jc.monitorAndPrintJob(jobConf, rj)) {
        throw new IOException("Job failed! " + rj.getFailureInfo());
      }
    }
  }
  
  @Before
  public void setUp() throws Exception {
    ExitUtil.disableSystemExit();
    conf = new Configuration();
    conf.set(HAUtil.MR_HA_FENCING_METHODS_KEY,
        AlwaysSucceedFencer.class.getName());
    conf.setLong(HAUtil.MR_HA_ACTIVE_CHECK_MILLIS, 1000);
    conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", true);
    conf.setInt("mapred.job.tracker.persist.jobstatus.hours", 1);
    conf.set("mapred.job.tracker.persist.jobstatus.dir", "/tmp/jobtracker/jobsInfo");
    cluster = new MiniMRHACluster(conf);
    cluster.getJobTrackerHaDaemon(0).makeActive();
    cluster.startTaskTracker(0, 1);
    cluster.waitActive();
    
    jt1 = cluster.getJobTrackerHaDaemon(0);
    jt2 = cluster.getJobTrackerHaDaemon(1);
    target1 = new JobTrackerHAServiceTarget(jt1.getConf());
    target2 = new JobTrackerHAServiceTarget(jt2.getConf());
  }
  
  @Test
  public void test() throws Exception {
    TestContext flippers = new TestContext();
    flippers.addThread(new FailoverThread(flippers));
    TestContext submitters = new TestContext();
    for (int i = 0; i < NUM_THREADS; i++) {
      submitters.addThread(new JobSubmitterThread(submitters, conf));
    }
    flippers.startThreads();
    submitters.startThreads();
    submitters.waitFor(RUNTIME);
    submitters.stop();
    flippers.stop();
  }

}

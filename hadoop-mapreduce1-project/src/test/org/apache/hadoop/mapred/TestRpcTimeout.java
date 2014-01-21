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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRpcTimeout {
  private static String[] trackers = new String[] { "tracker_tracker1:1000",
    "tracker_tracker2:1000", "tracker_tracker3:1000" };
  private JobTracker jobTracker;
  private MiniMRCluster mr;

  @Before
  public void setUp() throws Exception {
    JobConf conf = new JobConf();
    conf.setClass("mapred.jobtracker.taskScheduler",
        TestClusterStatus.FakeTaskScheduler.class,
        TaskScheduler.class);
    mr = new MiniMRCluster(0, 0, 0, "file:///", 1, null, null, null, conf);
    jobTracker = mr.getJobTrackerRunner().getJobTracker();
    for (String tracker : trackers) {
      establishFirstContact(jobTracker, tracker);
    }
  }

  @After
  public void tearDown() throws Exception {
    mr.shutdown();
    mr = null;
    jobTracker = null;
  }

  static short sendHeartBeat(JobTracker jt, TaskTrackerStatus status,
      boolean initialContact, boolean acceptNewTasks,
      String tracker, short responseId)
      throws IOException {
    if (status == null) {
      status = new TaskTrackerStatus(tracker,
      JobInProgress.convertTrackerNameToHostName(tracker));
    }
    jt.heartbeat(status, false, initialContact, acceptNewTasks, responseId);
    return ++responseId ;
  }

  static void establishFirstContact(JobTracker jt, String tracker)
      throws IOException {
    sendHeartBeat(jt, null, true, false, tracker, (short) 0);
  }

  @Test
  public void testRpcTimeout() throws IOException {
    JobConf conf = mr.createJobConf();
    // Set a very short rpc timeout, and make sure we get a socket timeout
    // exception.
    conf.set(JobClient.MAPREDUCE_CLIENT_RPC_TIMEOUT_KEY, "1");
    JobClient clientWithTimeout = new JobClient(conf);
    try {
      for (Thread thread : Thread.getAllStackTraces().keySet()) {
        if (thread.getName().contains("IPC Server")) {
          thread.suspend();
        }
      }
      clientWithTimeout.getClusterStatus();
      fail();
    } catch (SocketTimeoutException ex) {
      // This is expected.
    } finally {
      for (Thread thread : Thread.getAllStackTraces().keySet()) {
        if (thread.getName().contains("IPC Server")) {
          thread.resume();
        }
      }
    }
  }
}

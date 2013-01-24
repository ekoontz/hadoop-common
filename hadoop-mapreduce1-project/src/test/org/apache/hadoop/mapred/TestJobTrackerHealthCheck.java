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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.*;

public class TestJobTrackerHealthCheck {
  
  private Configuration conf;
  private MiniMRHACluster cluster;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniMRHACluster(conf);
    cluster.getJobTrackerHaDaemon(0).makeActive();
    cluster.waitActive();
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }
  
  @SuppressWarnings("deprecation")
  @Test(timeout=60000)
  public void test() throws Exception {
    JobTrackerHAServiceProtocol haServiceProtocol =
      cluster.getJobTrackerHaDaemon(0).getJobTrackerHAServiceProtocol();
    
    // Should not throw error, which indicates healthy.
    haServiceProtocol.monitorHealth();
    
    // Forcibly stop JT thread
    haServiceProtocol.getJobTrackerThread().stop();
    haServiceProtocol.getJobTrackerThread().join();
    
    try {
      // Should throw error - JT is unhealthy.
      haServiceProtocol.monitorHealth();
      fail("Should not have succeeded in calling monitorHealth");
    } catch (HealthCheckFailedException hcfe) {
      GenericTestUtils.assertExceptionContains(
          "The JobTracker thread is not running", hcfe);
    }
  }

}

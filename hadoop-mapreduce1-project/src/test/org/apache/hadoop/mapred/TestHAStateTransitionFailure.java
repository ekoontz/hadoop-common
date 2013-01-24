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

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Test;

/**
 * Tests to verify the behavior of failing to fully start transition HA states.
 */
public class TestHAStateTransitionFailure {

  private static final Log LOG =
      LogFactory.getLog(TestHAStateTransitionFailure.class);
  /**
   * Ensure that a failure to fully transition to the active state causes a
   * shutdown of the jobtracker.
   */
  @Test(timeout=60000)
  public void testFailureToTransitionCausesShutdown() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRHACluster cluster = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1)
          .format(true)
          .checkExitOnShutdown(false)
          .build();

      // Set the owner of the system directory to a different user to the one
      // that starts the JT. This will cause the JT to fail to transition to
      // the active state.
      FileSystem fs = dfs.getFileSystem();
      Path mapredSysDir = new Path(conf.get("mapred.system.dir"), "seq-000000000000");
      fs.mkdirs(mapredSysDir, new FsPermission((short) 700));
      fs.setOwner(mapredSysDir, "mr", "mrgroup");

      cluster = new MiniMRHACluster(fs.getConf());
      final MiniMRHACluster finalCluster = cluster;
      UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
          "notmr", new String[]{"notmrgroup"});
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          finalCluster.getJobTrackerHaDaemon(0).makeActive();
          return null;
        }
      });
      cluster.getJobTrackerHaDaemon(0).makeActive();
      Thread.sleep(1000);
      assertTrue("Should have called terminate", ExitUtil.terminateCalled());
      assertExceptionContains("is not owned by", ExitUtil.getFirstExitException());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
}

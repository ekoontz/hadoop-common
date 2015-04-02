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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NameNodeEditLogRoller;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;

public class TestEditLogAutoroll {

  private Configuration conf;
  private MiniDFSCluster cluster;
  private NameNode nn0;
  private FileSystem fs;
  private FSEditLog editLog;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    // Stall the standby checkpointer in two ways
    conf.setLong(DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, Long.MAX_VALUE);
    conf.setLong(DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 20);
    // Make it autoroll after 10 edits
    conf.setFloat(DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD, 0.5f);
    conf.setInt(DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS, 100);

    MiniDFSNNTopology topology = new MiniDFSNNTopology()
    .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
      .addNN(new MiniDFSNNTopology.NNConf("nn1").setHttpPort(10061))
      .addNN(new MiniDFSNNTopology.NNConf("nn2").setHttpPort(10062)));

    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(topology)
      .numDataNodes(0)
      .build();
    cluster.waitActive();

    nn0 = cluster.getNameNode(0);
    fs = HATestUtil.configureFailoverFs(cluster, conf);

    cluster.transitionToActive(0);

    fs = cluster.getFileSystem(0);
    editLog = nn0.getNamesystem().getEditLog();
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testEditLogAutoroll() throws Exception {
    // Make some edits
    final long startTxId = editLog.getCurSegmentTxId();
    for (int i=0; i<11; i++) {
      fs.mkdirs(new Path("testEditLogAutoroll-" + i));
    }
    // Wait for the NN to autoroll
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return editLog.getCurSegmentTxId() > startTxId;
      }
    }, 1000, 5000);
    // Transition to standby and make sure the roller stopped
    nn0.transitionToStandby();
    GenericTestUtils.assertNoThreadsMatching(
        ".*" + NameNodeEditLogRoller.class.getSimpleName() + ".*");
  }
}

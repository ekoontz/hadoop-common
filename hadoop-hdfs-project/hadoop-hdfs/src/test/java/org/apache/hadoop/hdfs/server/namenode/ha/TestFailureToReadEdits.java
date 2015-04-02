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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster.Builder;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public class TestFailureToReadEdits {

  private static final String TEST_DIR1 = "/test1";
  private static final String TEST_DIR2 = "/test2";
  private static final String TEST_DIR3 = "/test3";
  
  private final TestType clusterType;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private MiniQJMHACluster miniQjmHaCluster; // for QJM case only
  private NameNode nn0;
  private NameNode nn1;
  private FileSystem fs;
  
  private static enum TestType {
    SHARED_DIR_HA,
    QJM_HA;
  };
  
  /**
   * Run this suite of tests both for QJM-based HA and for file-based
   * HA.
   */
  @Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { TestType.SHARED_DIR_HA },
        { TestType.QJM_HA } });
  }
  
  public TestFailureToReadEdits(TestType clusterType) {
    this.clusterType = clusterType;
  }

  @Before
  public void setUpCluster() throws Exception {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY, 10);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    HAUtil.setAllowStandbyReads(conf, true);
    
    if (clusterType == TestType.SHARED_DIR_HA) {
      MiniDFSNNTopology topology = MiniQJMHACluster.createDefaultTopology();
      cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(topology)
        .numDataNodes(0)
        .checkExitOnShutdown(false)
        .build();
    } else {
      Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder().numDataNodes(0).checkExitOnShutdown(false);
      miniQjmHaCluster = builder.build();
      cluster = miniQjmHaCluster.getDfsCluster();
    }
    cluster.waitActive();
    
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
    
    cluster.transitionToActive(0);
    fs = HATestUtil.configureFailoverFs(cluster, conf);
  }
  
  @After
  public void tearDownCluster() throws Exception {
    if (fs != null) {
      fs.close();
    }
    
    if (clusterType == TestType.SHARED_DIR_HA) {
      if (cluster != null) {
        cluster.shutdown();
      }
    } else {
      if (miniQjmHaCluster != null) {
        miniQjmHaCluster.shutdown();
      }
    }
  }

  /**
   * Test that the standby NN won't double-replay earlier edits if it encounters
   * a failure to read a later edit.
   */
  @Test
  public void testFailuretoReadEdits() throws Exception {
    assertTrue(fs.mkdirs(new Path(TEST_DIR1)));
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    
    // If these two ops are applied twice, the first op will throw an
    // exception the second time its replayed.
    fs.setOwner(new Path(TEST_DIR1), "foo", "bar");
    assertTrue(fs.delete(new Path(TEST_DIR1), true));
    
    // This op should get applied just fine.
    assertTrue(fs.mkdirs(new Path(TEST_DIR2)));
    
    // This is the op the mocking will cause to fail to be read.
    assertTrue(fs.mkdirs(new Path(TEST_DIR3)));
    
    LimitedEditLogAnswer answer = causeFailureOnEditLogRead();
    
    try {
      HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
      fail("Standby fully caught up, but should not have been able to");
    } catch (HATestUtil.CouldNotCatchUpException e) {
      // Expected. The NN did not exit.
    }
    
    // Null because it was deleted.
    assertNull(NameNodeAdapter.getFileInfo(nn1,
        TEST_DIR1, false));
    // Should have been successfully created.
    assertTrue(NameNodeAdapter.getFileInfo(nn1,
        TEST_DIR2, false).isDir());
    // Null because it hasn't been created yet.
    assertNull(NameNodeAdapter.getFileInfo(nn1,
        TEST_DIR3, false));
    
    // Now let the standby read ALL the edits.
    answer.setThrowExceptionOnRead(false);
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    
    // Null because it was deleted.
    assertNull(NameNodeAdapter.getFileInfo(nn1,
        TEST_DIR1, false));
    // Should have been successfully created.
    assertTrue(NameNodeAdapter.getFileInfo(nn1,
        TEST_DIR2, false).isDir());
    // Should now have been successfully created.
    assertTrue(NameNodeAdapter.getFileInfo(nn1,
        TEST_DIR3, false).isDir());
  }
  
  /**
   * Test the following case:
   * 1. SBN is reading a finalized edits file when NFS disappears halfway
   *    through (or some intermittent error happens)
   * 2. SBN performs a checkpoint and uploads it to the NN
   * 3. NN receives a checkpoint that doesn't correspond to the end of any log
   *    segment
   * 4. Both NN and SBN should be able to restart at this point.
   * 
   * This is a regression test for HDFS-2766.
   */
  @Test
  public void testCheckpointStartingMidEditsFile() throws Exception {
    assertTrue(fs.mkdirs(new Path(TEST_DIR1)));
    
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    
    // Once the standby catches up, it should notice that it needs to
    // do a checkpoint and save one to its local directories.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(0, 3));
    
    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(0, 3));
    
    causeFailureOnEditLogRead();
    
    assertTrue(fs.mkdirs(new Path(TEST_DIR2)));
    assertTrue(fs.mkdirs(new Path(TEST_DIR3)));
    
    try {
      HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
      fail("Standby fully caught up, but should not have been able to");
    } catch (HATestUtil.CouldNotCatchUpException e) {
      // Expected. The NN did not exit.
    }
    
    // 5 because we should get OP_START_LOG_SEGMENT and one successful OP_MKDIR
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(0, 3, 5));
    
    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(0, 3, 5));

    // Restart the active NN
    cluster.restartNameNode(0);
    
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(0, 3, 5));
    
    FileSystem fs0 = null;
    try {
      // Make sure that when the active restarts, it loads all the edits.
      fs0 = FileSystem.get(NameNode.getUri(nn0.getNameNodeAddress()),
          conf);
      
      assertTrue(fs0.exists(new Path(TEST_DIR1)));
      assertTrue(fs0.exists(new Path(TEST_DIR2)));
      assertTrue(fs0.exists(new Path(TEST_DIR3)));
    } finally {
      if (fs0 != null)
        fs0.close();
    }
  }

  /**
   * Ensure that the standby fails to become active if it cannot read all
   * available edits in the shared edits dir when it is transitioning to active
   * state.
   */
  @Test
  public void testFailureToReadEditsOnTransitionToActive() throws Exception {
    assertTrue(fs.mkdirs(new Path(TEST_DIR1)));
    
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    
    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(0, 3));
    
    causeFailureOnEditLogRead();
    
    assertTrue(fs.mkdirs(new Path(TEST_DIR2)));
    assertTrue(fs.mkdirs(new Path(TEST_DIR3)));
    
    try {
      HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
      fail("Standby fully caught up, but should not have been able to");
    } catch (HATestUtil.CouldNotCatchUpException e) {
      // Expected. The NN did not exit.
    }
    
    // Shutdown the active NN.
    cluster.shutdownNameNode(0);
    
    try {
      // Transition the standby to active.
      cluster.transitionToActive(1);
      fail("Standby transitioned to active, but should not have been able to");
    } catch (ExitException ee) {
      GenericTestUtils.assertExceptionContains("Error replaying edit log", ee);
    }
  }
  
  private LimitedEditLogAnswer causeFailureOnEditLogRead() throws IOException {
    FSEditLog spyEditLog = NameNodeAdapter.spyOnEditLog(nn1);
    LimitedEditLogAnswer answer = new LimitedEditLogAnswer(); 
    doAnswer(answer).when(spyEditLog).selectInputStreams(
        anyLong(), anyLong(), (MetaRecoveryContext)anyObject(), anyBoolean());
    return answer;
  }
  
  private static class LimitedEditLogAnswer
      implements Answer<Collection<EditLogInputStream>> {
    
    private boolean throwExceptionOnRead = true;

    @SuppressWarnings("unchecked")
    @Override
    public Collection<EditLogInputStream> answer(InvocationOnMock invocation)
        throws Throwable {
      Collection<EditLogInputStream> streams = (Collection<EditLogInputStream>)
          invocation.callRealMethod();
  
      if (!throwExceptionOnRead) {
        return streams;
      } else {
        Collection<EditLogInputStream> ret = new LinkedList<EditLogInputStream>();
        for (EditLogInputStream stream : streams) {
          EditLogInputStream spyStream = spy(stream);
          doAnswer(new Answer<FSEditLogOp>() {

            @Override
            public FSEditLogOp answer(InvocationOnMock invocation)
                throws Throwable {
              FSEditLogOp op = (FSEditLogOp) invocation.callRealMethod();
              if (throwExceptionOnRead &&
                  TEST_DIR3.equals(NameNodeAdapter.getMkdirOpPath(op))) {
                throw new IOException("failed to read op creating " + TEST_DIR3);
              } else {
                return op;
              }
            }
            
          }).when(spyStream).readOp();
          ret.add(spyStream);
        }
        return ret;
      }
    }
    
    public void setThrowExceptionOnRead(boolean throwExceptionOnRead) {
      this.throwExceptionOnRead = throwExceptionOnRead;
    }
  }
  
}

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
package org.apache.hadoop.hdfs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.TestInterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.Test;

public class TestLeaseRecovery {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short)3;
  private static final long LEASE_PERIOD = 300L;

  static void checkMetaInfo(ExtendedBlock b, DataNode dn
      ) throws IOException {
    TestInterDatanodeProtocol.checkMetaInfo(b, dn);
  }
  
  static int min(Integer... x) {
    int m = x[0];
    for(int i = 1; i < x.length; i++) {
      if (x[i] < m) {
        m = x[i];
      }
    }
    return m;
  }

  void waitLeaseRecovery(MiniDFSCluster cluster) {
    cluster.setLeasePeriod(LEASE_PERIOD, LEASE_PERIOD);
    // wait for the lease to expire
    try {
      Thread.sleep(2 * 3000);  // 2 heartbeat intervals
    } catch (InterruptedException e) {
    }
  }

  /**
   * The following test first creates a file with a few blocks.
   * It randomly truncates the replica of the last block stored in each datanode.
   * Finally, it triggers block synchronization to synchronize all stored block.
   */
  @Test
  public void testBlockSynchronization() throws Exception {
    final int ORG_FILE_SIZE = 3000; 
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, ORG_FILE_SIZE, REPLICATION_NUM, 0L);
      assertTrue(dfs.exists(filepath));
      DFSTestUtil.waitReplication(dfs, filepath, REPLICATION_NUM);

      //get block info for the last block
      LocatedBlock locatedblock = TestInterDatanodeProtocol.getLastLocatedBlock(
          dfs.dfs.getNamenode(), filestr);
      DatanodeInfo[] datanodeinfos = locatedblock.getLocations();
      assertEquals(REPLICATION_NUM, datanodeinfos.length);

      //connect to data nodes
      DataNode[] datanodes = new DataNode[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
        datanodes[i] = cluster.getDataNode(datanodeinfos[i].getIpcPort());
        assertTrue(datanodes[i] != null);
      }
      
      //verify Block Info
      ExtendedBlock lastblock = locatedblock.getBlock();
      DataNode.LOG.info("newblocks=" + lastblock);
      for(int i = 0; i < REPLICATION_NUM; i++) {
        checkMetaInfo(lastblock, datanodes[i]);
      }


      DataNode.LOG.info("dfs.dfs.clientName=" + dfs.dfs.clientName);
      cluster.getNameNodeRpc().append(filestr, dfs.dfs.clientName);

      // expire lease to trigger block recovery.
      waitLeaseRecovery(cluster);

      Block[] updatedmetainfo = new Block[REPLICATION_NUM];
      long oldSize = lastblock.getNumBytes();
      lastblock = TestInterDatanodeProtocol.getLastLocatedBlock(
          dfs.dfs.getNamenode(), filestr).getBlock();
      long currentGS = lastblock.getGenerationStamp();
      for(int i = 0; i < REPLICATION_NUM; i++) {
        updatedmetainfo[i] = DataNodeTestUtils.getFSDataset(datanodes[i]).getStoredBlock(
            lastblock.getBlockPoolId(), lastblock.getBlockId());
        assertEquals(lastblock.getBlockId(), updatedmetainfo[i].getBlockId());
        assertEquals(oldSize, updatedmetainfo[i].getNumBytes());
        assertEquals(currentGS, updatedmetainfo[i].getGenerationStamp());
      }

      // verify that lease recovery does not occur when namenode is in safemode
      System.out.println("Testing that lease recovery cannot happen during safemode.");
      filestr = "/foo.safemode";
      filepath = new Path(filestr);
      dfs.create(filepath, (short)1);
      cluster.getNameNodeRpc().setSafeMode(
          HdfsConstants.SafeModeAction.SAFEMODE_ENTER, false);
      assertTrue(dfs.dfs.exists(filestr));
      DFSTestUtil.waitReplication(dfs, filepath, (short)1);
      waitLeaseRecovery(cluster);
      // verify that we still cannot recover the lease
      LeaseManager lm = NameNodeAdapter.getLeaseManager(cluster.getNamesystem());
      assertTrue("Found " + lm.countLease() + " lease, expected 1", lm.countLease() == 1);
      cluster.getNameNodeRpc().setSafeMode(
          HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}

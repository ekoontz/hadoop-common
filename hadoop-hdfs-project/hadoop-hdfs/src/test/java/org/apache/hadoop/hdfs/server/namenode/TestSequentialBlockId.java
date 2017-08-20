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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests the sequential block ID generation mechanism and block ID
 * collision handling.
 */
public class TestSequentialBlockId {

  private static final Log LOG = LogFactory.getLog("TestSequentialBlockId");

  private static final DataChecksum DEFAULT_CHECKSUM =
      DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512);

  final int BLOCK_SIZE = 1024;
  final int IO_SIZE = BLOCK_SIZE;
  final short REPLICATION = 1;
  final long SEED = 0;

  DatanodeID datanode;
  InetSocketAddress dnAddr;

  /**
   * Test that block IDs are generated sequentially.
   *
   * @throws IOException
   */
  @Test
  public void testBlockIdGeneration() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // Create a file that is 10 blocks long.
      Path path = new Path("testBlockIdGeneration.dat");
      DFSTestUtil.createFile(
          fs, path, IO_SIZE, BLOCK_SIZE * 10, BLOCK_SIZE, REPLICATION, SEED);
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
      LOG.info("Block0 id is " + blocks.get(0).getBlock().getBlockId());
      long nextBlockExpectedId = blocks.get(0).getBlock().getBlockId() + 1;

      // Ensure that the block IDs are sequentially increasing.
      for (int i = 1; i < blocks.size(); ++i) {
        long nextBlockId = blocks.get(i).getBlock().getBlockId();
        LOG.info("Block" + i + " id is " + nextBlockId);
        assertThat(nextBlockId, is(nextBlockExpectedId));
        ++nextBlockExpectedId;
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test that collisions in the block ID space are handled gracefully.
   *
   * @throws IOException
   */
  @Test
  public void testTriggerBlockIdCollision() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      FSNamesystem fsn = cluster.getNamesystem();
      final int blockCount = 10;


      // Create a file with a few blocks to rev up the global block ID
      // counter.
      Path path1 = new Path("testBlockIdCollisionDetection_file1.dat");
      DFSTestUtil.createFile(
          fs, path1, IO_SIZE, BLOCK_SIZE * blockCount,
          BLOCK_SIZE, REPLICATION, SEED);
      List<LocatedBlock> blocks1 = DFSTestUtil.getAllBlocks(fs, path1);


      // Rewind the block ID counter in the name system object. This will result
      // in block ID collisions when we try to allocate new blocks.
      SequentialBlockIdGenerator blockIdGenerator = fsn.getBlockIdGenerator();
      blockIdGenerator.setCurrentValue(blockIdGenerator.getCurrentValue() - 5);

      // Trigger collisions by creating a new file.
      Path path2 = new Path("testBlockIdCollisionDetection_file2.dat");
      DFSTestUtil.createFile(
          fs, path2, IO_SIZE, BLOCK_SIZE * blockCount,
          BLOCK_SIZE, REPLICATION, SEED);
      List<LocatedBlock> blocks2 = DFSTestUtil.getAllBlocks(fs, path2);
      assertThat(blocks2.size(), is(blockCount));

      // Make sure that file2 block IDs start immediately after file1
      assertThat(blocks2.get(0).getBlock().getBlockId(),
                 is(blocks1.get(9).getBlock().getBlockId() + 1));

    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test that the block type (legacy or not) can be correctly detected
   * based on its generation stamp.
   *
   * @throws IOException
   */
  @Test
  public void testBlockTypeDetection() throws IOException {

    // Setup a mock object and stub out a few routines to
    // retrieve the generation stamp counters.
    FSNamesystem fsn = mock(FSNamesystem.class);
    final long maxGenStampForLegacyBlocks = 10000;

    when(fsn.getGenerationStampV1Limit())
        .thenReturn(maxGenStampForLegacyBlocks);

    Block legacyBlock = spy(new Block());
    when(legacyBlock.getGenerationStamp())
        .thenReturn(maxGenStampForLegacyBlocks/2);

    Block newBlock = spy(new Block());
    when(newBlock.getGenerationStamp())
        .thenReturn(maxGenStampForLegacyBlocks+1);

    // Make sure that isLegacyBlock() can correctly detect
    // legacy and new blocks.
    when(fsn.isLegacyBlock(any(Block.class))).thenCallRealMethod();
    assertThat(fsn.isLegacyBlock(legacyBlock), is(true));
    assertThat(fsn.isLegacyBlock(newBlock), is(false));
  }

  /**
   * Test that the generation stamp for legacy and new blocks is updated
   * as expected.
   *
   * @throws IOException
   */
  @Test
  public void testGenerationStampUpdate() throws IOException {

    // Setup a mock object and stub out a few routines to
    // retrieve the generation stamp counters.
    FSNamesystem fsn = mock(FSNamesystem.class);
    FSEditLog editLog = mock(FSEditLog.class);
    final long nextGenerationStampV1 = 5000;
    final long nextGenerationStampV2 = 20000;

    when(fsn.getNextGenerationStampV1())
        .thenReturn(nextGenerationStampV1);
    when(fsn.getNextGenerationStampV2())
        .thenReturn(nextGenerationStampV2);

    // Make sure that the generation stamp is set correctly for both
    // kinds of blocks.
    when(fsn.nextGenerationStamp(anyBoolean())).thenCallRealMethod();
    when(fsn.hasWriteLock()).thenReturn(true);
    when(fsn.getEditLog()).thenReturn(editLog);
    assertThat(fsn.nextGenerationStamp(true), is(nextGenerationStampV1));
    assertThat(fsn.nextGenerationStamp(false), is(nextGenerationStampV2));
  }
}

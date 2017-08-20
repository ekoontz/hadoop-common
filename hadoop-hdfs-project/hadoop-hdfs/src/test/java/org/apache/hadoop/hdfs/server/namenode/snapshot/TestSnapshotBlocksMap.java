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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for snapshot-related information in blocksMap.
 */
public class TestSnapshotBlocksMap {
  private static final long seed = 0;
  private static final short REPLICATION = 3;
  private static final int BLOCKSIZE = 1024;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  FSDirectory fsdir;
  BlockManager blockmanager;
  protected DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    blockmanager = fsn.getBlockManager();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  void assertAllNull(INodeFile inode, Path path, String[] snapshots) throws Exception { 
    Assert.assertNull(inode.getBlocks());
    assertINodeNull(path.toString());
    assertINodeNullInSnapshots(path, snapshots);
  }

  void assertINodeNull(String path) throws Exception {
    Assert.assertNull(fsdir.getINode(path));
  }

  void assertINodeNullInSnapshots(Path path, String... snapshots) throws Exception {
    for(String s : snapshots) {
      assertINodeNull(SnapshotTestHelper.getSnapshotPath(
          path.getParent(), s, path.getName()).toString());
    }
  }

  static INodeFile assertBlockCollection(String path, int numBlocks,
     final FSDirectory dir, final BlockManager blkManager) throws Exception {
    final INodeFile file = INodeFile.valueOf(dir.getINode(path), path);
    assertEquals(numBlocks, file.getBlocks().length);
    for(BlockInfo b : file.getBlocks()) {
      assertBlockCollection(blkManager, file, b);
    }
    return file;
  }

  static void assertBlockCollection(final BlockManager blkManager,
      final INodeFile file, final BlockInfo b) { 
    Assert.assertSame(b, blkManager.getStoredBlock(b));
    Assert.assertSame(file, blkManager.getBlockCollection(b));
    Assert.assertSame(file, b.getBlockCollection());
  }

  /**
   * Test deleting a file with snapshots. Need to check the blocksMap to make
   * sure the corresponding record is updated correctly.
   */
  @Test (timeout=60000)
  public void testDeletionWithSnapshots() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    
    Path sub2 = new Path(sub1, "sub2");
    Path file2 = new Path(sub2, "file2");

    Path file3 = new Path(sub1, "file3");
    Path file4 = new Path(sub1, "file4");
    Path file5 = new Path(sub1, "file5");
    
    // Create file under sub1
    DFSTestUtil.createFile(hdfs, file0, 4*BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, 2*BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, 3*BLOCKSIZE, REPLICATION, seed);
    
    // Normal deletion
    {
      final INodeFile f2 = assertBlockCollection(file2.toString(), 3, fsdir,
          blockmanager);
      BlockInfo[] blocks = f2.getBlocks();
      hdfs.delete(sub2, true);
      // The INode should have been removed from the blocksMap
      for(BlockInfo b : blocks) {
        assertNull(blockmanager.getBlockCollection(b));
      }
    }
    
    // Create snapshots for sub1
    final String[] snapshots = {"s0", "s1", "s2"};
    DFSTestUtil.createFile(hdfs, file3, 5*BLOCKSIZE, REPLICATION, seed);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, snapshots[0]);
    DFSTestUtil.createFile(hdfs, file4, 1*BLOCKSIZE, REPLICATION, seed);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, snapshots[1]);
    DFSTestUtil.createFile(hdfs, file5, 7*BLOCKSIZE, REPLICATION, seed);
    SnapshotTestHelper.createSnapshot(hdfs, sub1, snapshots[2]);

    // set replication so that the inode should be replaced for snapshots
    {
      INodeFile f1 = assertBlockCollection(file1.toString(), 2, fsdir,
          blockmanager);
      Assert.assertSame(INodeFile.class, f1.getClass());
      hdfs.setReplication(file1, (short)2);
      f1 = assertBlockCollection(file1.toString(), 2, fsdir, blockmanager);
      Assert.assertSame(INodeFileWithSnapshot.class, f1.getClass());
    }
    
    // Check the block information for file0
    final INodeFile f0 = assertBlockCollection(file0.toString(), 4, fsdir,
        blockmanager);
    BlockInfo[] blocks0 = f0.getBlocks();
    
    // Also check the block information for snapshot of file0
    Path snapshotFile0 = SnapshotTestHelper.getSnapshotPath(sub1, "s0",
        file0.getName());
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);
    
    // Delete file0
    hdfs.delete(file0, true);
    // Make sure the blocks of file0 is still in blocksMap
    for(BlockInfo b : blocks0) {
      assertNotNull(blockmanager.getBlockCollection(b));
    }
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);
    
    // Compare the INode in the blocksMap with INodes for snapshots
    String s1f0 = SnapshotTestHelper.getSnapshotPath(sub1, "s1",
        file0.getName()).toString();
    assertBlockCollection(s1f0, 4, fsdir, blockmanager);
    
    // Delete snapshot s1
    hdfs.deleteSnapshot(sub1, "s1");

    // Make sure the first block of file0 is still in blocksMap
    for(BlockInfo b : blocks0) {
      assertNotNull(blockmanager.getBlockCollection(b));
    }
    assertBlockCollection(snapshotFile0.toString(), 4, fsdir, blockmanager);

    try {
      INodeFile.valueOf(fsdir.getINode(s1f0), s1f0);
      fail("Expect FileNotFoundException when identifying the INode in a deleted Snapshot");
    } catch (IOException e) {
      assertExceptionContains("File does not exist: " + s1f0, e);
    }
  }
}

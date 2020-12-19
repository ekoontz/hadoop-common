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

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the creation and validation of metasave
 */
public class TestMetaSave {
  static final int NUM_DATA_NODES = 2;
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fileSys = null;

  @BeforeClass
  public static void setUp() throws IOException {
    // start a cluster
    Configuration conf = new HdfsConfiguration();

    // High value of replication interval
    // so that blocks remain under-replicated
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1000);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1L);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
  }

  /**
   * Tests metasave
   */
  @Test
  public void testMetaSave() throws IOException, InterruptedException {

    final FSNamesystem namesystem = cluster.getNamesystem();

    for (int i = 0; i < 2; i++) {
      Path file = new Path("/filestatus" + i);
      DFSTestUtil.createFile(fileSys, file, 1024, 1024, blockSize, (short) 2,
          seed);
    }

    cluster.stopDataNode(1);
    // wait for namenode to discover that a datanode is dead
    Thread.sleep(15000);
    namesystem.setReplication("/filestatus0", (short) 4);

    namesystem.metaSave("metasave.out.txt");

    // Verification
    String logFile = System.getProperty("hadoop.log.dir") + "/"
        + "metasave.out.txt";
    FileInputStream fstream = new FileInputStream(logFile);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String line = reader.readLine();
    assertTrue(line.equals(
      "3 files and directories, 2 blocks = 5 total filesystem objects"));
    line = reader.readLine();
    assertTrue(line.equals("Live Datanodes: 1"));
    line = reader.readLine();
    assertTrue(line.equals("Dead Datanodes: 1"));
    line = reader.readLine();
    line = reader.readLine();
    assertTrue(line.matches("^/filestatus[01]:.*"));
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (fileSys != null)
      fileSys.close();
    if (cluster != null)
      cluster.shutdown();
  }
}

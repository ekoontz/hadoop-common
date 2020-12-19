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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestSecurityTokenEditLog {
  static final int NUM_DATA_NODES = 1;

  // This test creates NUM_THREADS threads and each thread does
  // 2 * NUM_TRANSACTIONS Transactions concurrently.
  static final int NUM_TRANSACTIONS = 100;
  static final int NUM_THREADS = 100;
  static final int opsPerTrans = 3;

  static {
    // No need to fsync for the purposes of tests. This makes
    // the tests run much faster.
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    FSNamesystem namesystem;
    int numTransactions;
    short replication = 3;
    long blockSize = 64;

    Transactions(FSNamesystem ns, int num) {
      namesystem = ns;
      numTransactions = num;
    }

    // add a bunch of transactions.
    @Override
    public void run() {
      FSEditLog editLog = namesystem.getEditLog();

      for (int i = 0; i < numTransactions; i++) {
        try {
          String renewer = UserGroupInformation.getLoginUser().getUserName();
          Token<DelegationTokenIdentifier> token = namesystem
              .getDelegationToken(new Text(renewer));
          namesystem.renewDelegationToken(token);
          namesystem.cancelDelegationToken(token);
          editLog.logSync();
        } catch (IOException e) {
          System.out.println("Transaction " + i + " encountered exception " +
                             e);
        }
      }
    }
  }

  /**
   * Tests transaction logging in dfs.
   */
  @Test
  public void testEditLog() throws IOException {

    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;

    try {
      conf.setBoolean(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();
  
      for (Iterator<URI> it = cluster.getNameDirs(0).iterator(); it.hasNext(); ) {
        File dir = new File(it.next().getPath());
        System.out.println(dir);
      }
      
      FSImage fsimage = namesystem.getFSImage();
      FSEditLog editLog = fsimage.getEditLog();
  
      // set small size of flush buffer
      editLog.setOutputBufferCapacity(2048);
    
      // Create threads and make them run transactions concurrently.
      Thread threadId[] = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        Transactions trans = new Transactions(namesystem, NUM_TRANSACTIONS);
        threadId[i] = new Thread(trans, "TransactionThread-" + i);
        threadId[i].start();
      }
  
      // wait for all transactions to get over
      for (int i = 0; i < NUM_THREADS; i++) {
        try {
          threadId[i].join();
        } catch (InterruptedException e) {
          i--;      // retry 
        }
      } 
      
      editLog.close();
        
      // Verify that we can read in all the transactions that we have written.
      // If there were any corruptions, it is likely that the reading in
      // of these transactions will throw an exception.
      //
      namesystem.getDelegationTokenSecretManager().stopThreads();
      int numKeys = namesystem.getDelegationTokenSecretManager().getNumberOfKeys();
      int expectedTransactions = NUM_THREADS * opsPerTrans * NUM_TRANSACTIONS + numKeys
          + 2; // + 2 for BEGIN and END txns

      for (StorageDirectory sd : fsimage.getStorage().dirIterable(NameNodeDirType.EDITS)) {
        File editFile = NNStorage.getFinalizedEditsFile(sd, 1, 1 + expectedTransactions - 1);
        System.out.println("Verifying file: " + editFile);
        
        FSEditLogLoader loader = new FSEditLogLoader(namesystem, 0);        
        long numEdits = loader.loadFSEdits(
            new EditLogFileInputStream(editFile), 1, null);
        assertEquals("Verification for " + editFile, expectedTransactions, numEdits);
      }
    } finally {
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }
}

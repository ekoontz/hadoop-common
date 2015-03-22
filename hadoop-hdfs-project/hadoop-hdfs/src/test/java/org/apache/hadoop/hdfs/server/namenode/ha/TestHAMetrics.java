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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * Make sure HA-related metrics are updated and reported appropriately.
 */
public class TestHAMetrics {
  
  private static final Log LOG = LogFactory.getLog(TestHAMetrics.class);
  
  @Test
  public void testHAMetrics() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, Integer.MAX_VALUE);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(1)
        .build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      
      FSNamesystem nn0 = cluster.getNamesystem(0);
      FSNamesystem nn1 = cluster.getNamesystem(1);
      
      assertEquals(nn0.getHAState(), "standby");
      assertTrue(0 < nn0.getMillisSinceLastLoadedEdits());
      assertEquals(nn1.getHAState(), "standby");
      assertTrue(0 < nn1.getMillisSinceLastLoadedEdits());

      cluster.transitionToActive(0);
      
      assertEquals("active", nn0.getHAState());
      assertEquals(0, nn0.getMillisSinceLastLoadedEdits());
      assertEquals("standby", nn1.getHAState());
      assertTrue(0 < nn1.getMillisSinceLastLoadedEdits());
      
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      
      assertEquals("standby", nn0.getHAState());
      assertTrue(0 < nn0.getMillisSinceLastLoadedEdits());
      assertEquals("active", nn1.getHAState());
      assertEquals(0, nn1.getMillisSinceLastLoadedEdits());
      
      Thread.sleep(2000); // make sure standby gets a little out-of-date
      assertTrue(2000 <= nn0.getMillisSinceLastLoadedEdits());
      
      assertEquals(0, nn0.getPendingDataNodeMessageCount());
      assertEquals(0, nn1.getPendingDataNodeMessageCount());
      
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      DFSTestUtil.createFile(fs, new Path("/foo"),
          10, (short)1, 1L);
      
      assertTrue(0 < nn0.getPendingDataNodeMessageCount());
      assertEquals(0, nn1.getPendingDataNodeMessageCount());
      long millisSinceLastLoadedEdits = nn0.getMillisSinceLastLoadedEdits();
      
      HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(1),
          cluster.getNameNode(0));
      
      assertEquals(0, nn0.getPendingDataNodeMessageCount());
      assertEquals(0, nn1.getPendingDataNodeMessageCount());
      long newMillisSinceLastLoadedEdits = nn0.getMillisSinceLastLoadedEdits();
      // Since we just waited for the standby to catch up, the time since we
      // last loaded edits should be very low.
      assertTrue("expected " + millisSinceLastLoadedEdits + " > " +
          newMillisSinceLastLoadedEdits,
          millisSinceLastLoadedEdits > newMillisSinceLastLoadedEdits);
    } finally {
      IOUtils.cleanup(LOG, fs);
      cluster.shutdown();
    }
  }
}

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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDFSClientFailover {
  
  private static final Log LOG = LogFactory.getLog(TestDFSClientFailover.class);
  
  private static final Path TEST_FILE = new Path("/tmp/failover-test-file");
  private static final int FILE_LENGTH_TO_VERIFY = 100;
  
  private Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  
  @Before
  public void setUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .build();
    cluster.transitionToActive(0);
    cluster.waitActive();
  }
  
  @After
  public void tearDownCluster() throws IOException {
    cluster.shutdown();
  }

  /**
   * Make sure that client failover works when an active NN dies and the standby
   * takes over.
   */
  @Test
  public void testDfsClientFailover() throws IOException, URISyntaxException {
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    
    DFSTestUtil.createFile(fs, TEST_FILE,
        FILE_LENGTH_TO_VERIFY, (short)1, 1L);
    
    assertEquals(fs.getFileStatus(TEST_FILE).getLen(), FILE_LENGTH_TO_VERIFY);
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(1);
    assertEquals(fs.getFileStatus(TEST_FILE).getLen(), FILE_LENGTH_TO_VERIFY);
    
    // Check that it functions even if the URL becomes canonicalized
    // to include a port number.
    Path withPort = new Path("hdfs://" +
        HATestUtil.getLogicalHostname(cluster) + ":" +
        NameNode.DEFAULT_PORT + "/" + TEST_FILE.toUri().getPath());
    FileSystem fs2 = withPort.getFileSystem(fs.getConf());
    assertTrue(fs2.exists(withPort));

    fs.close();
  }
  
  /**
   * Regression test for HDFS-2683.
   */
  @Test
  public void testLogicalUriShouldNotHavePorts() {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + ".foo",
        ConfiguredFailoverProxyProvider.class.getName());
    Path p = new Path("hdfs://foo:12345/");
    try {
      p.getFileSystem(conf).exists(p);
      fail("Did not fail with fake FS");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "does not use port information", ioe);
    }
  }

  /**
   * Make sure that a helpful error message is shown if a proxy provider is
   * configured for a given URI, but no actual addresses are configured for that
   * URI.
   */
  @Test
  public void testFailureWithMisconfiguredHaNNs() throws Exception {
    String logicalHost = "misconfigured-ha-uri";
    Configuration conf = new Configuration();
    conf.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + logicalHost,
        ConfiguredFailoverProxyProvider.class.getName());
    
    URI uri = new URI("hdfs://" + logicalHost + "/test");
    try {
      FileSystem.get(uri, conf).exists(new Path("/test"));
      fail("Successfully got proxy provider for misconfigured FS");
    } catch (IOException ioe) {
      LOG.info("got expected exception", ioe);
      assertTrue("expected exception did not contain helpful message",
          StringUtils.stringifyException(ioe).contains(
          "Could not find any configured addresses for URI " + uri));
    }
  }
}

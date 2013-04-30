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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests JT HA delegation tokens.
 */
public class TestHADelegationToken {
  private static final String TMP_DIR = 
    new File("build/test/" + TestHADelegationToken.class.getSimpleName()).getAbsolutePath();

  private static MiniDFSCluster miniDFS = null;
  private MiniMRHACluster cluster;
  private JobTrackerHADaemon jt1;
  private JobTrackerHADaemon jt2;
  private JobTrackerHAServiceTarget target1;
  private JobTrackerHAServiceTarget target2;
  private Configuration conf;
  
  private void startCluster() throws Exception {
    startCluster(new Configuration());
  }
  
  private void startCluster(Configuration conf) throws Exception {
    ExitUtil.disableSystemExit();

    File tmpDir = new File(TMP_DIR);
    FileUtil.fullyDelete(tmpDir);
    tmpDir.mkdirs();
    this.conf = conf;

    conf.set("fs.hdfs.impl.disable.cache", "true");
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hadoop.tmp.dir", TMP_DIR);
    miniDFS = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fileSystem = miniDFS.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path(TMP_DIR));
    fileSystem.mkdirs(new Path(TMP_DIR, "mapred"));
    fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
    Path tmp = new Path(TMP_DIR, "mapred");
    while (tmp != null) {      
      fileSystem.setPermission(tmp, new FsPermission((short)0777));      
      tmp = tmp.getParent();
    }
    conf.set("fs.default.name", fileSystem.getUri().toString());
    
    conf.set(HAUtil.MR_HA_FENCING_METHODS_KEY, AlwaysSucceedFencer.class.getName());
    cluster = new MiniMRHACluster(conf);
    cluster.getJobTrackerHaDaemon(0).makeActive();
    cluster.startTaskTracker(0, 1);
    cluster.waitActive();

    jt1 = cluster.getJobTrackerHaDaemon(0);
    jt2 = cluster.getJobTrackerHaDaemon(1);
    target1 = new JobTrackerHAServiceTarget(jt1.getConf());
    target2 = new JobTrackerHAServiceTarget(jt2.getConf());
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      FileSystem.closeAll();
      miniDFS.shutdown();
    }
  }
  
  @Test
  public void tokenCloningServiceWithLogicalRealNames() throws Exception {
    UserGroupInformation user = UserGroupInformation.createUserForTesting("alice",
      new String[]{"users"});
    startCluster();

    user.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        JobClient jc = new JobClient(conf);
        assertFalse(conf.get("mapred.job.tracker").contains(":"));

        Token<DelegationTokenIdentifier> haToken =
          jc.getDelegationToken(
            new Text(UserGroupInformation.getCurrentUser().getShortUserName()));

        // verify the token has the correct service name with JT HA prefix
        assertEquals(HAUtil.buildTokenServiceForLogicalAddress(conf.get("mapred.job.tracker")),
          haToken.getService());

        // inject the token as credential in the UGI
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        ugi.addToken(haToken);

        // create a new jobclient so it gets the token from the UGI
        jc = new JobClient(conf);

        // collect the service from tokens in the UGI
        Collection<Token<? extends TokenIdentifier>> tokensInUGI = ugi.getTokens();
        Set<Text> found = new HashSet<Text>();
        for (Token<? extends TokenIdentifier> t : tokensInUGI) {
          found.add(t.getService());
        }

        // create a set with the expected service from tokens in the UGI, we are looking for
        // services from cloned tokens with logical and real names
        Set<Text> expected = new HashSet<Text>();
        Map<String, InetSocketAddress> map = HAUtil.getHaJtRpcAddresses(conf);
        for (InetSocketAddress address : map.values()) {
          expected.add(SecurityUtil.buildTokenService(address));
        }
        expected.add(HAUtil.buildTokenServiceForLogicalAddress(conf.get("mapred.job.tracker")));

        assertEquals(expected, found);
        return null;
      }
    });
  }

}

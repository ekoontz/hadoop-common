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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestAuthorizationProvider {
  private MiniDFSCluster miniDFS;
  private static final Set<String> CALLED = new HashSet<String>();
  
  public static class MyAuthorizationProvider extends AuthorizationProvider {
    private AuthorizationProvider defaultProvider;

    @Override
    public void start() {
      CALLED.add("start");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider = new DefaultAuthorizationProvider();
      defaultProvider.start();
    }

    @Override
    public void stop() {
      CALLED.add("stop");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider.stop();
      defaultProvider = null;
    }

    @Override
    public void setSnaphottableDirs(
        Map<INodeAuthorizationInfo, Integer> snapshotableDirs) {
      CALLED.add("setSnaphottableDirs");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider.setSnaphottableDirs(snapshotableDirs);
    }

    @Override
    public void addSnapshottable(INodeAuthorizationInfo dir) {
      CALLED.add("addSnapshottable");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider.addSnapshottable(dir);
    }

    @Override
    public void removeSnapshottable(INodeAuthorizationInfo dir) {
      CALLED.add("removeSnapshottable");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider.removeSnapshottable(dir);
    }

    @Override
    public void createSnapshot(INodeAuthorizationInfo dir, int snapshotId) 
        throws IOException {
      CALLED.add("createSnapshot");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider.createSnapshot(dir, snapshotId);
    }

    @Override
    public void removeSnapshot(INodeAuthorizationInfo dir, int snapshotId)
        throws IOException {
      CALLED.add("removeSnapshot");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider.removeSnapshot(dir, snapshotId);
    }

    public void checkPermission(String user, Set<String> groups,
        INodeAuthorizationInfo[] inodes, int snapshotId,
        boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
        FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
        throws AccessControlException, UnresolvedLinkException {
      CALLED.add("checkPermission");
      CALLED.add("isClientOp=" + isClientOp());
      defaultProvider.checkPermission(user, groups, inodes, snapshotId,
          doCheckOwner, ancestorAccess, parentAccess, access, subAccess,
          ignoreEmptyDir);
    }

    private boolean useDefault(INodeAuthorizationInfo iNode) {
      return !iNode.getFullPathName().startsWith("/user/authz");
    }

    @Override
    public void setUser(INodeAuthorizationInfo node, String user) {
      CALLED.add("setUser");
      CALLED.add("isClientOp=" + isClientOp());
      if (useDefault(node)) {
        defaultProvider.setUser(node, user);
      }
    }

    @Override
    public String getUser(INodeAuthorizationInfo node, int snapshotId) {
      CALLED.add("getUser");
      CALLED.add("isClientOp=" + isClientOp());
      String user;
      if (useDefault(node)) {
        user = defaultProvider.getUser(node, snapshotId);
      } else {
        user = "foo";
      }
      return user;
    }

    @Override
    public void setGroup(INodeAuthorizationInfo node, String group) {
      CALLED.add("setGroup");
      CALLED.add("isClientOp=" + isClientOp());
      if (useDefault(node)) {
        defaultProvider.setGroup(node, group);
      }
    }

    @Override
    public String getGroup(INodeAuthorizationInfo node, int snapshotId) {
      CALLED.add("getGroup");
      CALLED.add("isClientOp=" + isClientOp());
      String group;
      if (useDefault(node)) {
        group = defaultProvider.getGroup(node, snapshotId);
      } else {
        group = "bar";
      }
      return group;
    }

    @Override
    public void setPermission(INodeAuthorizationInfo node,
        FsPermission permission) {
      CALLED.add("setPermission");
      CALLED.add("isClientOp=" + isClientOp());
      if (useDefault(node)) {
        defaultProvider.setPermission(node, permission);
      }
    }

    @Override
    public FsPermission getFsPermission(
        INodeAuthorizationInfo node, int snapshotId) {
      CALLED.add("getFsPermission");
      CALLED.add("isClientOp=" + isClientOp());
      FsPermission permission;
      if (useDefault(node)) {
        permission = defaultProvider.getFsPermission(node, snapshotId);
      } else {
        permission = new FsPermission((short)0770);
      }
      return permission;
    }

    @Override
    public AclFeature getAclFeature(INodeAuthorizationInfo node,
        int snapshotId) {
      CALLED.add("getAclFeature");
      CALLED.add("isClientOp=" + isClientOp());
      AclFeature f;
      if (useDefault(node)) {
        f = defaultProvider.getAclFeature(node, snapshotId);
      } else {
        AclEntry acl = new AclEntry.Builder().setType(AclEntryType.GROUP).
            setPermission(FsAction.ALL).setName("xxx").build();
        f = new AclFeature(ImmutableList.of(acl));
      }
      return f;
    }

    @Override
    public void removeAclFeature(INodeAuthorizationInfo node) {
      CALLED.add("removeAclFeature");
      CALLED.add("isClientOp=" + isClientOp());
      if (useDefault(node)) {
        defaultProvider.removeAclFeature(node);
      }
    }

    @Override
    public void addAclFeature(INodeAuthorizationInfo node, AclFeature f) {
      CALLED.add("addAclFeature");
      CALLED.add("isClientOp=" + isClientOp());
      if (useDefault(node)) {
        defaultProvider.addAclFeature(node, f);
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    AuthorizationProvider.set(null);
    CALLED.clear();
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_AUTHORIZATION_PROVIDER_KEY, 
        MyAuthorizationProvider.class.getName());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();
  }

  @After
  public void cleanUp() throws IOException {
    CALLED.clear();
    if (miniDFS != null) {
      miniDFS.shutdown();
    }
    Assert.assertTrue(CALLED.contains("stop"));
    Assert.assertFalse(CALLED.contains("isClientOp=true"));
    Assert.assertTrue(CALLED.contains("isClientOp=false"));
    AuthorizationProvider.set(null);
  }

  @Test
  public void testDelegationToProvider() throws Exception {
    Assert.assertTrue(CALLED.contains("start"));
    Assert.assertTrue(CALLED.contains("setSnaphottableDirs"));
    Assert.assertFalse(CALLED.contains("isClientOp=true"));
    Assert.assertTrue(CALLED.contains("isClientOp=false"));
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    fs.mkdirs(new Path("/tmp"));
    fs.setPermission(new Path("/tmp"), new FsPermission((short) 0777));
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("u1", 
        new String[]{"g1"});
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
        CALLED.clear();
        fs.mkdirs(new Path("/tmp/foo"));
        Assert.assertTrue(CALLED.contains("checkPermission"));
        Assert.assertTrue(CALLED.contains("isClientOp=true"));
        Assert.assertFalse(CALLED.contains("isClientOp=false"));
        CALLED.clear();
        fs.listStatus(new Path("/tmp/foo"));
        Assert.assertTrue(CALLED.contains("getUser"));
        Assert.assertTrue(CALLED.contains("getGroup"));
        Assert.assertTrue(CALLED.contains("getFsPermission"));
        Assert.assertTrue(CALLED.contains("isClientOp=true"));
        Assert.assertFalse(CALLED.contains("isClientOp=false"));
        CALLED.clear();
        fs.setPermission(new Path("/tmp/foo"), new FsPermission((short) 0700));
        Assert.assertTrue(CALLED.contains("setPermission"));
        Assert.assertTrue(CALLED.contains("isClientOp=true"));
        Assert.assertFalse(CALLED.contains("isClientOp=false"));
        CALLED.clear();
        fs.getAclStatus(new Path("/tmp/foo"));
        Assert.assertTrue(CALLED.contains("getAclFeature"));
        Assert.assertTrue(CALLED.contains("isClientOp=true"));
        Assert.assertFalse(CALLED.contains("isClientOp=false"));
        CALLED.clear();
        fs.modifyAclEntries(new Path("/tmp/foo"), 
            Arrays.asList(new AclEntry.Builder().
            setName("u3").setType(AclEntryType.USER).
            setPermission(FsAction.ALL).build()));
        Assert.assertTrue(CALLED.contains("addAclFeature"));
        Assert.assertTrue(CALLED.contains("isClientOp=true"));
        Assert.assertFalse(CALLED.contains("isClientOp=false"));
        CALLED.clear();
        fs.removeAcl(new Path("/tmp/foo"));
        Assert.assertTrue(CALLED.contains("removeAclFeature"));
        Assert.assertTrue(CALLED.contains("isClientOp=true"));
        Assert.assertFalse(CALLED.contains("isClientOp=false"));
        return null;
      }
    });
    CALLED.clear();
    fs.setOwner(new Path("/tmp/foo"), "u2", "g2");
    Assert.assertTrue(CALLED.contains("setUser"));
    Assert.assertTrue(CALLED.contains("setGroup"));
    Assert.assertTrue(CALLED.contains("isClientOp=true"));
    Assert.assertFalse(CALLED.contains("isClientOp=false"));

    CALLED.clear();
    ((DistributedFileSystem)fs).allowSnapshot(new Path("/tmp/foo"));
    Assert.assertTrue(CALLED.contains("addSnapshottable"));
    Assert.assertTrue(CALLED.contains("isClientOp=true"));
    Assert.assertFalse(CALLED.contains("isClientOp=false"));

    CALLED.clear();
    fs.createSnapshot(new Path("/tmp/foo"), "foo");
    Assert.assertTrue(CALLED.contains("createSnapshot"));
    Assert.assertTrue(CALLED.contains("isClientOp=true"));
    Assert.assertFalse(CALLED.contains("isClientOp=false"));

    CALLED.clear();
    fs.deleteSnapshot(new Path("/tmp/foo"), "foo");
    Assert.assertTrue(CALLED.contains("removeSnapshot"));
    Assert.assertTrue(CALLED.contains("isClientOp=true"));
    Assert.assertFalse(CALLED.contains("isClientOp=false"));

    CALLED.clear();
    ((DistributedFileSystem) fs).disallowSnapshot(new Path("/tmp/foo"));
    Assert.assertTrue(CALLED.contains("removeSnapshottable"));
    Assert.assertTrue(CALLED.contains("isClientOp=true"));
    Assert.assertFalse(CALLED.contains("isClientOp=false"));

  }

  @Test
  public void testCustomProvider() throws Exception {
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    fs.mkdirs(new Path("/user/xxx"));
    FileStatus status = fs.getFileStatus(new Path("/user/xxx"));
    Assert.assertEquals(System.getProperty("user.name"), status.getOwner());
    Assert.assertEquals("supergroup", status.getGroup());
    Assert.assertEquals(new FsPermission((short)0755), status.getPermission());
    fs.mkdirs(new Path("/user/authz"));
    status = fs.getFileStatus(new Path("/user/authz"));
    Assert.assertEquals("foo", status.getOwner());
    Assert.assertEquals("bar", status.getGroup());
    Assert.assertEquals(new FsPermission((short) 0770), status.getPermission());
  }

}

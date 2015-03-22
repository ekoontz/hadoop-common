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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDirectoryCollection {

  private static final File testDir = new File("target",
      TestDirectoryCollection.class.getName()).getAbsoluteFile();
  private static final File testFile = new File(testDir, "testfile");

  @BeforeClass
  public static void setup() throws IOException {
    testDir.mkdirs();
    testFile.createNewFile();
  }

  @AfterClass
  public static void teardown() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testConcurrentAccess() throws IOException {
    // Initialize DirectoryCollection with a file instead of a directory
    String[] dirs = {testFile.getPath()};
    DirectoryCollection dc = new DirectoryCollection(dirs);

    // Create an iterator before checkDirs is called to reliable test case
    List<String> list = dc.getGoodDirs();
    ListIterator<String> li = list.listIterator();

    // DiskErrorException will invalidate iterator of non-concurrent
    // collections. ConcurrentModificationException will be thrown upon next
    // use of the iterator.
    Assert.assertTrue("checkDirs did not remove test file from directory list",
        dc.checkDirs());

    // Verify no ConcurrentModification is thrown
    li.next();
  }

  @Test
  public void testCreateDirectories() throws IOException {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext localFs = FileContext.getLocalFSFileContext(conf);

    String dirA = new File(testDir, "dirA").getPath();
    String dirB = new File(dirA, "dirB").getPath();
    String dirC = new File(testDir, "dirC").getPath();
    Path pathC = new Path(dirC);
    FsPermission permDirC = new FsPermission((short)0710);

    localFs.mkdir(pathC, null, true);
    localFs.setPermission(pathC, permDirC);

    String[] dirs = { dirA, dirB, dirC };
    DirectoryCollection dc = new DirectoryCollection(dirs);
    FsPermission defaultPerm = FsPermission.getDefault()
        .applyUMask(new FsPermission((short)FsPermission.DEFAULT_UMASK));
    boolean createResult = dc.createNonExistentDirs(localFs, defaultPerm);
    Assert.assertTrue(createResult);

    FileStatus status = localFs.getFileStatus(new Path(dirA));
    Assert.assertEquals("local dir parent not created with proper permissions",
        defaultPerm, status.getPermission());
    status = localFs.getFileStatus(new Path(dirB));
    Assert.assertEquals("local dir not created with proper permissions",
        defaultPerm, status.getPermission());
    status = localFs.getFileStatus(pathC);
    Assert.assertEquals("existing local directory permissions modified",
        permDirC, status.getPermission());
  }
}

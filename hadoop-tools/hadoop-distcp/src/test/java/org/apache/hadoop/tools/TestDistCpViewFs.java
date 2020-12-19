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

package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.viewfs.*;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.fs.FsConstants;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class TestDistCpViewFs {
  private static final Log LOG = LogFactory.getLog(TestDistCpViewFs.class);

  private static FileSystem fs;

  private static Path listFile;
  private static Path target;
  private static String root;

  private static Configuration getConf() throws URISyntaxException {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "local");
    conf.set("fs.default.name", "file:///");
    return conf;
  }

  @BeforeClass
  public static void setup() throws URISyntaxException{
    try {
      Path fswd = FileSystem.get(getConf()).getWorkingDirectory();
      Configuration vConf = ViewFileSystemTestSetup.createConfig();
      ConfigUtil.addLink(vConf, "/usr", new URI(fswd.toString()));
      fs = FileSystem.get(FsConstants.VIEWFS_URI, vConf);
      fs.setWorkingDirectory(new Path("/usr"));
      listFile = new Path("target/tmp/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      target = new Path("target/tmp/target").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      root = new Path("target/tmp").makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString();
      TestDistCpUtils.delete(fs, root);
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
    }
  }

  @Test
  public void testSingleFileMissingTarget() {
    caseSingleFileMissingTarget(false);
    caseSingleFileMissingTarget(true);
  }


  private void caseSingleFileMissingTarget(boolean sync) {

    try {
      addEntries(listFile, "singlefile1/file1");
      createFiles("singlefile1/file1");

      runTest(listFile, target, sync);

      checkResult(target, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleFileTargetFile() {
    caseSingleFileTargetFile(false);
    caseSingleFileTargetFile(true);
  }

  private void caseSingleFileTargetFile(boolean sync) {

    try {
      addEntries(listFile, "singlefile1/file1");
      createFiles("singlefile1/file1", target.toString());

      runTest(listFile, target, sync);

      checkResult(target, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleFileTargetDir() {
    caseSingleFileTargetDir(false);
    caseSingleFileTargetDir(true);
  }

  private void caseSingleFileTargetDir(boolean sync) {

    try {
      addEntries(listFile, "singlefile2/file2");
      createFiles("singlefile2/file2");
      mkdirs(target.toString());

      runTest(listFile, target, sync);

      checkResult(target, 1, "file2");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleDirTargetMissing() {
    caseSingleDirTargetMissing(false);
    caseSingleDirTargetMissing(true);
  }

  private void caseSingleDirTargetMissing(boolean sync) {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, sync);

      checkResult(target, 1, "dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleDirTargetPresent() {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");
      mkdirs(target.toString());

      runTest(listFile, target, false);

      checkResult(target, 1, "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testUpdateSingleDirTargetPresent() {

    try {
      addEntries(listFile, "Usingledir");
      mkdirs(root + "/Usingledir/Udir1");
      mkdirs(target.toString());

      runTest(listFile, target, true);

      checkResult(target, 1, "Udir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiFileTargetPresent() {
    caseMultiFileTargetPresent(false);
    caseMultiFileTargetPresent(true);
  }

  private void caseMultiFileTargetPresent(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(target.toString());

      runTest(listFile, target, sync);

      checkResult(target, 3, "file3", "file4", "file5");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiFileTargetMissing() {
    caseMultiFileTargetMissing(false);
    caseMultiFileTargetMissing(true);
  }

  private void caseMultiFileTargetMissing(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");

      runTest(listFile, target, sync);

      checkResult(target, 3, "file3", "file4", "file5");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiDirTargetPresent() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(target.toString(), root + "/singledir/dir1");

      runTest(listFile, target, false);

      checkResult(target, 2, "multifile/file3", "multifile/file4", "multifile/file5", "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testUpdateMultiDirTargetPresent() {

    try {
      addEntries(listFile, "Umultifile", "Usingledir");
      createFiles("Umultifile/Ufile3", "Umultifile/Ufile4", "Umultifile/Ufile5");
      mkdirs(target.toString(), root + "/Usingledir/Udir1");

      runTest(listFile, target, true);

      checkResult(target, 4, "Ufile3", "Ufile4", "Ufile5", "Udir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiDirTargetMissing() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, false);

      checkResult(target, 2, "multifile/file3", "multifile/file4",
          "multifile/file5", "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testUpdateMultiDirTargetMissing() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, true);

      checkResult(target, 4, "file3", "file4", "file5", "dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testGlobTargetMissingSingleLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
                                fs.getWorkingDirectory());
      addEntries(listFile, "*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir/dir2/file6");

      runTest(listFile, target, false);

      checkResult(target, 2, "multifile/file3", "multifile/file4", "multifile/file5",
          "singledir/dir2/file6");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test
  public void testUpdateGlobTargetMissingSingleLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
                                  fs.getWorkingDirectory());
      addEntries(listFile, "*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir/dir2/file6");

      runTest(listFile, target, true);

      checkResult(target, 4, "file3", "file4", "file5", "dir2/file6");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test
  public void testGlobTargetMissingMultiLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      addEntries(listFile, "*/*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir1/dir3/file7", "singledir1/dir3/file8",
          "singledir1/dir3/file9");

      runTest(listFile, target, false);

      checkResult(target, 4, "file3", "file4", "file5",
          "dir3/file7", "dir3/file8", "dir3/file9");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test
  public void testUpdateGlobTargetMissingMultiLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      addEntries(listFile, "*/*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir1/dir3/file7", "singledir1/dir3/file8",
          "singledir1/dir3/file9");

      runTest(listFile, target, true);

      checkResult(target, 6, "file3", "file4", "file5",
          "file7", "file8", "file9");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  private void addEntries(Path listFile, String... entries) throws IOException {
    OutputStream out = fs.create(listFile);
    try {
      for (String entry : entries){
        out.write((root + "/" + entry).getBytes());
        out.write("\n".getBytes());
      }
    } finally {
      out.close();
    }
  }

  private void createFiles(String... entries) throws IOException {
    String e;
    for (String entry : entries){
      if ((new Path(entry)).isAbsolute()) 
      {
        e = entry;
      } 
      else 
      { 
        e = root + "/" + entry;
      }
      OutputStream out = fs.create(new Path(e));
      try {
        out.write((e).getBytes());
        out.write("\n".getBytes());
      } finally {
        out.close();
      }
    }
  }

  private void mkdirs(String... entries) throws IOException {
    for (String entry : entries){
      fs.mkdirs(new Path(entry));
    }
  }

  private void runTest(Path listFile, Path target, boolean sync) throws IOException {
    DistCpOptions options = new DistCpOptions(listFile, target);
    options.setSyncFolder(sync);
    try {
      new DistCp(getConf(), options).execute();
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw new IOException(e);
    }
  }

  private void checkResult(Path target, int count, String... relPaths) throws IOException {
    Assert.assertEquals(count, fs.listStatus(target).length);
    if (relPaths == null || relPaths.length == 0) {
      Assert.assertTrue(target.toString(), fs.exists(target));
      return;
    }
    for (String relPath : relPaths) {
      Assert.assertTrue(new Path(target, relPath).toString(), fs.exists(new Path(target, relPath)));
    }
  }

}

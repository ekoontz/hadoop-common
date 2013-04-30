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

package org.apache.hadoop.mapreduce.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class TestEncryptedShuffle {

  private static final String BASEDIR =
    System.getProperty("test.build.dir", "build/test-dir") + "/" +
    TestEncryptedShuffle.class.getSimpleName();

  @BeforeClass
  public static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
  }

  @After
  public void cleanUpMiniClusterSpecialConfig() throws Exception {
    String classpathDir =
      KeyStoreTestUtil.getClasspathDir(TestEncryptedShuffle.class);
    String keystoresDir = new File(BASEDIR).getAbsolutePath();
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, classpathDir);
  }

  private MiniDFSCluster dfsCluster = null;
  private MiniMRCluster mrCluster = null;

  private void startCluster(Configuration  conf) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "build/test-dir");
    }
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");
    dfsCluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fileSystem = dfsCluster.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
    fileSystem.setPermission(
      new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(
      new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(
      new Path("/hadoop/mapred/system"), FsPermission.valueOf("-rwx------"));
    FileSystem.setDefaultUri(conf, fileSystem.getUri());
    mrCluster = new MiniMRCluster(1, fileSystem.getUri().toString(), 1, null, null, new JobConf(conf));
  }

  private void stopCluster() throws Exception {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  protected JobConf getJobConf(Configuration sslConf) throws IOException {
    JobConf conf = new JobConf(mrCluster.createJobConf());
    //doing this because Hadoop1 minicluster does not use all mr cluster confs for the created jobconf
    conf.set(JobTracker.SHUFFLE_SSL_ENABLED_KEY,
             sslConf.get(JobTracker.SHUFFLE_SSL_ENABLED_KEY));
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, sslConf.get(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY));
    conf.set(SSLFactory.SSL_CLIENT_CONF_KEY, sslConf.get(SSLFactory.SSL_CLIENT_CONF_KEY));
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConf.get(SSLFactory.SSL_SERVER_CONF_KEY));
    conf.set(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY,
             sslConf.get(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY));
    return conf;
  }

  private void encryptedShuffleWithCerts(boolean useClientCerts)
    throws Exception {
    try {
      Configuration conf = new Configuration();
      String keystoresDir = new File(BASEDIR).getAbsolutePath();
      String sslConfsDir =
        KeyStoreTestUtil.getClasspathDir(TestEncryptedShuffle.class);
      KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfsDir, conf,
                                      useClientCerts);
      conf.setBoolean(JobTracker.SHUFFLE_SSL_ENABLED_KEY, true);
      startCluster(conf);
      FileSystem fs = FileSystem.get(getJobConf(conf));
      Path inputDir = new Path("input");
      fs.mkdirs(inputDir);
      Writer writer =
        new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
      writer.write("hello");
      writer.close();

      Path outputDir = new Path("output", "output");

      JobConf jobConf = new JobConf(getJobConf(conf));
      jobConf.setInt("mapred.map.tasks", 1);
      jobConf.setInt("mapred.map.max.attempts", 1);
      jobConf.setInt("mapred.reduce.max.attempts", 1);
      jobConf.set("mapred.input.dir", inputDir.toString());
      jobConf.set("mapred.output.dir", outputDir.toString());
      JobClient jobClient = new JobClient(jobConf);
      RunningJob runJob = jobClient.submitJob(jobConf);
      runJob.waitForCompletion();
      Assert.assertTrue(runJob.isComplete());
      Assert.assertTrue(runJob.isSuccessful());
    } finally {
      stopCluster();
    }
  }

  @Test
  public void encryptedShuffleWithClientCerts() throws Exception {
    encryptedShuffleWithCerts(true);
  }

  @Test
  public void encryptedShuffleWithoutClientCerts() throws Exception {
    encryptedShuffleWithCerts(false);
  }

}


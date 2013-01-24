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
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestJobTrackerHAServiceProtocol {

  private static final Path TEST_DIR = new Path("/tmp/tst");

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    MiniMRHACluster.configureLogicalName(conf);
    conf.set(HAUtil.MR_HA_JOBTRACKER_ID_KEY, "jt1");
    conf.set("mapred.system.dir", TEST_DIR.toString());

    fs = FileSystem.getLocal(conf);
    fs.delete(TEST_DIR, true);
  }

  @Test
  public void testRollSystemDirectory() throws IOException {
    JobTrackerHAServiceProtocol jt = new JobTrackerHAServiceProtocol(conf, null);

    JobConf jobConf = new JobConf(conf);
    Path sysDir = jt.rollSystemDirectory(jobConf);
    Path sysDir0 = sysDirPath("000000000000");
    assertEquals(sysDir0.toString(), jobConf.get("mapred.system.dir"));
    assertEquals(sysDir0.toString(), sysDir.toString());
    assertFalse("New sys dir 0 should not exist", fs.exists(sysDir0));
    fs.mkdirs(sysDir0);
    assertTrue("New sys dir 0 should now exist", fs.exists(sysDir0));

    jobConf = new JobConf(conf);
    sysDir = jt.rollSystemDirectory(jobConf);
    Path sysDir1 = sysDirPath("000000000001");
    assertEquals(sysDir1.toString(), jobConf.get("mapred.system.dir"));
    assertEquals(sysDir1.toString(), sysDir.toString());
    assertFalse("Old sys dir 0 should no longer exist", fs.exists(sysDir0));
    assertTrue("New sys dir 1 should exist", fs.exists(sysDir1));

    // Create a new sys dir later in the sequence and check it is picked up
    Path sysDir10 = sysDirPath("000000000010");
    fs.mkdirs(sysDir10);
    jobConf = new JobConf(conf);
    sysDir = jt.rollSystemDirectory(jobConf);
    Path sysDir11 = sysDirPath("000000000011");
    assertEquals(sysDir11.toString(), jobConf.get("mapred.system.dir"));
    assertEquals(sysDir11.toString(), sysDir.toString());
    assertTrue("Old sys dir 1 should still exist", fs.exists(sysDir1));
    assertFalse("Old sys dir 10 should no longer exist", fs.exists(sysDir10));
    assertTrue("New sys dir 11 should exist", fs.exists(sysDir11));

    // Create junk and check it is ignored
    fs.mkdirs(new Path(TEST_DIR, "zzz"));
    jobConf = new JobConf(conf);
    sysDir = jt.rollSystemDirectory(jobConf);
    Path sysDir12 = sysDirPath("000000000012");
    assertEquals(sysDir12.toString(), jobConf.get("mapred.system.dir"));
    assertEquals(sysDir12.toString(), sysDir.toString());
    assertFalse("Old sys dir 11 should no longer exist", fs.exists(sysDir11));
    assertTrue("New sys dir 12 should exist", fs.exists(sysDir12));
  }

  private Path sysDirPath(String counter) {
    Path sysDir = new Path(TEST_DIR,
        JobTrackerHAServiceProtocol.SYSTEM_DIR_SEQUENCE_PREFIX + counter);
    sysDir = fs.makeQualified(sysDir);
    return sysDir;
  }
}

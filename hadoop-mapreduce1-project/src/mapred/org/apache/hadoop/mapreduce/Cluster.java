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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Provides a way to access information about the map/reduce cluster.
 */
public class Cluster {

  public static enum JobTrackerStatus {INITIALIZING, RUNNING};

  private UserGroupInformation ugi;
  private Configuration conf;
  private Path stagingAreaDir = null;
  final Random rand = new Random();

  public Cluster(Configuration conf) throws IOException {
    this(null, conf);
  }

  public Cluster(InetSocketAddress jobTrackAddr, Configuration conf)
      throws IOException {
    this.conf = conf;
    this.ugi = UserGroupInformation.getCurrentUser();
  }

  /**
   * Grab the jobtracker's view of the staging directory path where job-specific
   * files will be placed.
   *
   * @return the staging directory where job-specific files are to be placed.
   */
  public Path getStagingAreaDir() throws IOException, InterruptedException {
    if (stagingAreaDir == null) {
      Path stagingRootDir = new Path(conf
          .get("mapreduce.jobtracker.staging.root.dir",
              "/tmp/hadoop/mapred/staging"));
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String user;
      if (ugi != null) {
        user = ugi.getShortUserName() + rand.nextInt();
      } else {
        user = "dummy" + rand.nextInt();
      }
      return stagingRootDir.getFileSystem(conf).makeQualified(
          new Path(stagingRootDir, user + "/.staging"));
    }
    return stagingAreaDir;
  }

}

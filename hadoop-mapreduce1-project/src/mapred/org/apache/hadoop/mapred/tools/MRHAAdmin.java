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

package org.apache.hadoop.mapred.tools;

import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.JobTrackerHAServiceTarget;
import org.apache.hadoop.util.ToolRunner;

public class MRHAAdmin extends HAAdmin {
  
  private static final Log LOG = LogFactory.getLog(MRHAAdmin.class);

  protected void setErrOut(PrintStream errOut) {
    this.errOut = errOut;
  }
  
  protected void setOut(PrintStream out) {
    this.out = out;
  }
  
  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }
  
  /**
   * Add the requisite security principal settings to the given Configuration,
   * returning a copy.
   * @param conf the original config
   * @return a copy with the security settings added
   */
  public static Configuration addSecurityConfiguration(Configuration conf) {
    // Make a copy so we don't mutate it. Also use a JobConf to
    // force loading of mapred-site.xml.
    conf = new JobConf(conf);
    String jobTrackerPrincipal = conf.get(
        JobTracker.JT_USER_NAME, "");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using JT principal: " + jobTrackerPrincipal);
    }

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        jobTrackerPrincipal);
    return conf;
  }
  
  @Override
  protected HAServiceTarget resolveTarget(String jtId) {
    JobConf conf = new JobConf(getConf()); // force load mapred-site.xml
    return new JobTrackerHAServiceTarget(conf, jtId);
  }
  
  @Override
  protected String getUsageString() {
    return "Usage: MRHAAdmin";
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new MRHAAdmin(), argv);
    System.exit(res);
  }
}

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

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.FenceMethod;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.DFSZKFailoverController;
import org.apache.hadoop.mapred.tools.MRZKFailoverController;

import com.google.common.base.Preconditions;

public class JobTrackerHAServiceTarget extends HAServiceTarget {
  
  private static final Log LOG =
    LogFactory.getLog(JobTrackerHAServiceTarget.class);
  
  private final InetSocketAddress addr;
  private InetSocketAddress zkfcAddr;
  private NodeFencer fencer;
  private BadFencingConfigurationException fenceConfigError;
  private String logicalName;
  private String jtId;
  private final boolean autoFailoverEnabled;
  
  public JobTrackerHAServiceTarget(Configuration conf) {
    this(conf, HAUtil.getJobTrackerId(conf));
  }
  
  public JobTrackerHAServiceTarget(Configuration conf, String jtId) {
    this.logicalName = HAUtil.getLogicalName(conf);
    this.jtId = jtId;

    // Make a copy of the conf, and override configs based on the
    // target node -- not the node we happen to be running on.
    JobConf targetConf = new JobConf(conf);
    HAUtil.setGenericConf(targetConf, logicalName, jtId, HAUtil.JOB_TRACKER_SPECIFIC_KEYS);
    
    this.addr = HAUtil.getJtHaRpcAddress(targetConf, jtId);

    this.autoFailoverEnabled = targetConf.getBoolean(
        HAUtil.MR_HA_AUTO_FAILOVER_ENABLED_KEY,
        HAUtil.MR_HA_AUTO_FAILOVER_ENABLED_DEFAULT);
    if (autoFailoverEnabled) {
      int port = MRZKFailoverController.getZkfcPort(targetConf);
      if (port != 0) {
        setZkfcPort(port);
      }
    }
    try {
      fencer = NodeFencer.create(targetConf, HAUtil.MR_HA_FENCING_METHODS_KEY);
    } catch (BadFencingConfigurationException e) {
      this.fenceConfigError = e;
    }
  }

  @Override
  public void checkFencingConfigured() throws BadFencingConfigurationException {
    if (fenceConfigError != null) {
      throw fenceConfigError;
    }
    if (fencer == null) {
      throw new BadFencingConfigurationException(
          "No fencer configured for " + this);
    }
  }

  @Override
  public InetSocketAddress getAddress() {
    return addr;
  }

  @Override
  public NodeFencer getFencer() {
    return fencer;
  }

  @Override
  public InetSocketAddress getZKFCAddress() {
    Preconditions.checkState(autoFailoverEnabled,
        "ZKFC address not relevant when auto failover is off");
    assert zkfcAddr != null;

    return zkfcAddr;
  }
  
  public void setZkfcPort(int port) {
    assert autoFailoverEnabled;
    
    this.zkfcAddr = new InetSocketAddress(addr.getAddress(), port);
  }
  
  @Override
  public boolean isAutoFailoverEnabled() {
    return autoFailoverEnabled;
  }

  public String getLogicalName() {
    return logicalName;
  }
  
  public String getJobTrackerId() {
    return jtId;
  }
  
  @Override
  public String toString() {
    return "JobTrackerHAServiceTarget at " + addr;
  }
  

}

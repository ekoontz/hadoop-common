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

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;

@InterfaceAudience.Private
public class CheckpointConf {
  private static final Log LOG = LogFactory.getLog(CheckpointConf.class);
  
  /** How often to checkpoint regardless of number of txns */
  private final long checkpointPeriod;    // in seconds
  
  /** How often to poll the NN to check checkpointTxnCount */
  private final long checkpointCheckPeriod; // in seconds
  
  /** checkpoint once every this many transactions, regardless of time */
  private final long checkpointTxnCount;

  /** maxium number of retries when merge errors occur */
  private final int maxRetriesOnMergeError;
  
  public CheckpointConf(Configuration conf) {
    checkpointCheckPeriod = conf.getLong(
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT);
        
    checkpointPeriod = conf.getLong(DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
                                    DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    checkpointTxnCount = conf.getLong(DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 
                                  DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
    maxRetriesOnMergeError = conf.getInt(DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY,
                                  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT);
    warnForDeprecatedConfigs(conf);
  }
  
  private static void warnForDeprecatedConfigs(Configuration conf) {
    for (String key : ImmutableList.of(
          "fs.checkpoint.size",
          "dfs.namenode.checkpoint.size")) {
      if (conf.get(key) != null) {
        LOG.warn("Configuration key " + key + " is deprecated! Ignoring..." +
            " Instead please specify a value for " +
            DFS_NAMENODE_CHECKPOINT_TXNS_KEY);
      }
    }
  }

  public long getPeriod() {
    return checkpointPeriod;
  }

  public long getCheckPeriod() {
    return Math.min(checkpointCheckPeriod, checkpointPeriod);
  }

  public long getTxnCount() {
    return checkpointTxnCount;
  }

  public int getMaxRetriesOnMergeError() {
    return maxRetriesOnMergeError;
  }
}

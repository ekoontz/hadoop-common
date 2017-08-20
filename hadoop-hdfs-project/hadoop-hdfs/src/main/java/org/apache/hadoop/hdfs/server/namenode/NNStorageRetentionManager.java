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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.util.MD5FileUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The NNStorageRetentionManager is responsible for inspecting the storage
 * directories of the NN and enforcing a retention policy on checkpoints
 * and edit logs.
 * 
 * It delegates the actual removal of files to a StoragePurger
 * implementation, which might delete the files or instead copy them to
 * a filer or HDFS for later analysis.
 */
public class NNStorageRetentionManager {
  
  private final int numCheckpointsToRetain;
  private final long numExtraEditsToRetain;
  private final int maxExtraEditsSegmentsToRetain;
  private static final Log LOG = LogFactory.getLog(
      NNStorageRetentionManager.class);
  private final NNStorage storage;
  private final StoragePurger purger;
  private final LogsPurgeable purgeableLogs;
  
  public NNStorageRetentionManager(
      Configuration conf,
      NNStorage storage,
      LogsPurgeable purgeableLogs,
      StoragePurger purger) {
    this.numCheckpointsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT);
    this.numExtraEditsToRetain = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_DEFAULT);
    this.maxExtraEditsSegmentsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_DEFAULT);
    Preconditions.checkArgument(numCheckpointsToRetain > 0,
        "Must retain at least one checkpoint");
    Preconditions.checkArgument(numExtraEditsToRetain >= 0,
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY +
        " must not be negative");
    
    this.storage = storage;
    this.purgeableLogs = purgeableLogs;
    this.purger = purger;
  }
  
  public NNStorageRetentionManager(Configuration conf, NNStorage storage,
      LogsPurgeable purgeableLogs) {
    this(conf, storage, purgeableLogs, new DeletionStoragePurger());
  }

  public void purgeOldStorage() throws IOException {
    FSImageTransactionalStorageInspector inspector =
      new FSImageTransactionalStorageInspector();
    storage.inspectStorageDirs(inspector);

    long minImageTxId = getImageTxIdToRetain(inspector);
    purgeCheckpointsOlderThan(inspector, minImageTxId);
    // If fsimage_N is the image we want to keep, then we need to keep
    // all txns > N. We can remove anything < N+1, since fsimage_N
    // reflects the state up to and including N. However, we also
    // provide a "cushion" of older txns that we keep, which is
    // handy for HA, where a remote node may not have as many
    // new images.
    //
    // First, determine the target number of extra transactions to retain based
    // on the configured amount.
    long minimumRequiredTxId = minImageTxId + 1;
    long purgeLogsFrom = Math.max(0, minimumRequiredTxId - numExtraEditsToRetain);
    
    ArrayList<EditLogInputStream> editLogs = new ArrayList<EditLogInputStream>();
    purgeableLogs.selectInputStreams(editLogs, purgeLogsFrom, false, false);
    Collections.sort(editLogs, new Comparator<EditLogInputStream>() {
      @Override
      public int compare(EditLogInputStream a, EditLogInputStream b) {
        return ComparisonChain.start()
            .compare(a.getFirstTxId(), b.getFirstTxId())
            .compare(a.getLastTxId(), b.getLastTxId())
            .result();
      }
    });

    // Remove from consideration any edit logs that are in fact required.
    while (editLogs.size() > 0 &&
        editLogs.get(editLogs.size() - 1).getFirstTxId() >= minimumRequiredTxId) {
      editLogs.remove(editLogs.size() - 1);
    }
    
    // Next, adjust the number of transactions to retain if doing so would mean
    // keeping too many segments around.
    while (editLogs.size() > maxExtraEditsSegmentsToRetain) {
      purgeLogsFrom = editLogs.get(0).getLastTxId() + 1;
      editLogs.remove(0);
    }
    
    // Finally, ensure that we're not trying to purge any transactions that we
    // actually need.
    if (purgeLogsFrom > minimumRequiredTxId) {
      throw new AssertionError("Should not purge more edits than required to "
          + "restore: " + purgeLogsFrom + " should be <= "
          + minimumRequiredTxId);
    }
    
    purgeableLogs.purgeLogsOlderThan(purgeLogsFrom);
  }
  
  private void purgeCheckpointsOlderThan(
      FSImageTransactionalStorageInspector inspector,
      long minTxId) {
    for (FSImageFile image : inspector.getFoundImages()) {
      if (image.getCheckpointTxId() < minTxId) {
        purger.purgeImage(image);
      }
    }
  }

  /**
   * @param inspector inspector that has already inspected all storage dirs
   * @return the transaction ID corresponding to the oldest checkpoint
   * that should be retained. 
   */
  private long getImageTxIdToRetain(FSImageTransactionalStorageInspector inspector) {
      
    List<FSImageFile> images = inspector.getFoundImages();
    TreeSet<Long> imageTxIds = Sets.newTreeSet();
    for (FSImageFile image : images) {
      imageTxIds.add(image.getCheckpointTxId());
    }
    
    List<Long> imageTxIdsList = Lists.newArrayList(imageTxIds);
    if (imageTxIdsList.isEmpty()) {
      return 0;
    }
    
    Collections.reverse(imageTxIdsList);
    int toRetain = Math.min(numCheckpointsToRetain, imageTxIdsList.size());    
    long minTxId = imageTxIdsList.get(toRetain - 1);
    LOG.info("Going to retain " + toRetain + " images with txid >= " +
        minTxId);
    return minTxId;
  }
  
  /**
   * Interface responsible for disposing of old checkpoints and edit logs.
   */
  static interface StoragePurger {
    void purgeLog(EditLogFile log);
    void purgeImage(FSImageFile image);
  }
  
  static class DeletionStoragePurger implements StoragePurger {
    @Override
    public void purgeLog(EditLogFile log) {
      LOG.info("Purging old edit log " + log);
      deleteOrWarn(log.getFile());
    }

    @Override
    public void purgeImage(FSImageFile image) {
      LOG.info("Purging old image " + image);
      deleteOrWarn(image.getFile());
      deleteOrWarn(MD5FileUtils.getDigestFileForFile(image.getFile()));
    }

    private static void deleteOrWarn(File file) {
      if (!file.delete()) {
        // It's OK if we fail to delete something -- we'll catch it
        // next time we swing through this directory.
        LOG.warn("Could not delete " + file);
      }      
    }
  }
}

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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

/**
 * Manage the heartbeats received from datanodes.
 * The datanode list and statistics are synchronized
 * by the heartbeat manager lock.
 */
class HeartbeatManager implements DatanodeStatistics {
  static final Log LOG = LogFactory.getLog(HeartbeatManager.class);

  /**
   * Stores a subset of the datanodeMap in DatanodeManager,
   * containing nodes that are considered alive.
   * The HeartbeatMonitor periodically checks for out-dated entries,
   * and removes them from the list.
   * It is synchronized by the heartbeat manager lock.
   */
  private final List<DatanodeDescriptor> datanodes = new ArrayList<DatanodeDescriptor>();

  /** Statistics, which are synchronized by the heartbeat manager lock. */
  private final Stats stats = new Stats();

  /** The time period to check for expired datanodes */
  private final long heartbeatRecheckInterval;
  /** Heartbeat monitor thread */
  private final Daemon heartbeatThread = new Daemon(new Monitor());
  /**
   * The initial setting of end user which indicates whether or not to avoid
   * writing to stale datanodes.
   */
  private final boolean initialAvoidWriteStaleNodes;
  /**
   * When the ratio of stale datanodes reaches this number, stop avoiding 
   * writing to stale datanodes, i.e., continue using stale nodes for writing.
   */
  private final float ratioUseStaleDataNodesForWrite;
    
  final Namesystem namesystem;
  final BlockManager blockManager;

  HeartbeatManager(final Namesystem namesystem,
      final BlockManager blockManager, final Configuration conf) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    boolean checkStaleNodes = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_CHECK_STALE_DATANODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECK_STALE_DATANODE_DEFAULT);
    long recheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 min
    long staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);// 30s
    this.initialAvoidWriteStaleNodes = DatanodeManager
        .getAvoidStaleForWriteFromConf(conf, checkStaleNodes);
    this.ratioUseStaleDataNodesForWrite = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT);
    Preconditions.checkArgument(
        (ratioUseStaleDataNodesForWrite > 0 && 
            ratioUseStaleDataNodesForWrite <= 1.0f),
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY +
        " = '" + ratioUseStaleDataNodesForWrite + "' is invalid. " +
        "It should be a positive non-zero float value, not greater than 1.0f.");
    
    this.heartbeatRecheckInterval = (checkStaleNodes 
        && initialAvoidWriteStaleNodes 
        && staleInterval < recheckInterval) ? staleInterval : recheckInterval;
  }

  void activate(Configuration conf) {
    heartbeatThread.start();
  }

  void close() {
    heartbeatThread.interrupt();
    try {
      // This will no effect if the thread hasn't yet been started.
      heartbeatThread.join(3000);
    } catch (InterruptedException e) {
    }
  }
  
  synchronized int getLiveDatanodeCount() {
    return datanodes.size();
  }

  @Override
  public synchronized long getCapacityTotal() {
    return stats.capacityTotal;
  }

  @Override
  public synchronized long getCapacityUsed() {
    return stats.capacityUsed;
  }

  @Override
  public synchronized float getCapacityUsedPercent() {
    return DFSUtil.getPercentUsed(stats.capacityUsed, stats.capacityTotal);
  }

  @Override
  public synchronized long getCapacityRemaining() {
    return stats.capacityRemaining;
  }

  @Override
  public synchronized float getCapacityRemainingPercent() {
    return DFSUtil.getPercentRemaining(
        stats.capacityRemaining, stats.capacityTotal);
  }

  @Override
  public synchronized long getBlockPoolUsed() {
    return stats.blockPoolUsed;
  }

  @Override
  public synchronized float getPercentBlockPoolUsed() {
    return DFSUtil.getPercentUsed(stats.blockPoolUsed, stats.capacityTotal);
  }

  @Override
  public synchronized long getCapacityUsedNonDFS() {
    final long nonDFSUsed = stats.capacityTotal
        - stats.capacityRemaining - stats.capacityUsed;
    return nonDFSUsed < 0L? 0L : nonDFSUsed;
  }

  @Override
  public synchronized int getXceiverCount() {
    return stats.xceiverCount;
  }

  @Override
  public synchronized long[] getStats() {
    return new long[] {getCapacityTotal(),
                       getCapacityUsed(),
                       getCapacityRemaining(),
                       -1L,
                       -1L,
                       -1L,
                       getBlockPoolUsed()};
  }

  @Override
  public synchronized int getExpiredHeartbeats() {
    return stats.expiredHeartbeats;
  }

  synchronized void register(final DatanodeDescriptor d) {
    if (!datanodes.contains(d)) {
      addDatanode(d);

      //update its timestamp
      d.updateHeartbeat(0L, 0L, 0L, 0L, 0, 0);
    }
  }

  synchronized DatanodeDescriptor[] getDatanodes() {
    return datanodes.toArray(new DatanodeDescriptor[datanodes.size()]);
  }

  synchronized void addDatanode(final DatanodeDescriptor d) {
    datanodes.add(d);
    d.isAlive = true;
  }

  synchronized void removeDatanode(DatanodeDescriptor node) {
    if (node.isAlive) {
      stats.subtract(node);
      datanodes.remove(node);
      node.isAlive = false;
    }
  }

  synchronized void updateHeartbeat(final DatanodeDescriptor node,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      int xceiverCount, int failedVolumes) {
    stats.subtract(node);
    node.updateHeartbeat(capacity, dfsUsed, remaining, blockPoolUsed,
        xceiverCount, failedVolumes);
    stats.add(node);
  }

  synchronized void startDecommission(final DatanodeDescriptor node) {
    stats.subtract(node);
    node.startDecommission();
    stats.add(node);
  }

  synchronized void stopDecommission(final DatanodeDescriptor node) {
    stats.subtract(node);
    node.stopDecommission();
    stats.add(node);
  }
  
  /**
   * Check if there are any expired heartbeats, and if so,
   * whether any blocks have to be re-replicated.
   * While removing dead datanodes, make sure that only one datanode is marked
   * dead at a time within the synchronized section. Otherwise, a cascading
   * effect causes more datanodes to be declared dead.
   */
  void heartbeatCheck() {
    final DatanodeManager dm = blockManager.getDatanodeManager();
    // It's OK to check safe mode w/o taking the lock here, we re-check
    // for safe mode after taking the lock before removing a datanode.
    if (namesystem.isInSafeMode()) {
      return;
    }
    boolean checkStaleNodes = dm.isCheckingForStaleDataNodes();
    boolean allAlive = false;
    while (!allAlive) {
      // locate the first dead node.
      DatanodeID dead = null;
      // check the number of stale nodes
      int numOfStaleNodes = 0;
      synchronized(this) {
        for (DatanodeDescriptor d : datanodes) {
          if (dead == null && dm.isDatanodeDead(d)) {
            stats.incrExpiredHeartbeats();
            dead = d;
            if (!checkStaleNodes) {
              break;
            }
          }
          if (checkStaleNodes && 
              d.isStale(dm.getStaleInterval())) {
            numOfStaleNodes++;
          }
        }
        
        // Change whether to avoid using stale datanodes for writing
        // based on proportion of stale datanodes
        if (checkStaleNodes) {
          dm.setNumStaleNodes(numOfStaleNodes);
          if (numOfStaleNodes > 
                datanodes.size() * ratioUseStaleDataNodesForWrite) {
            dm.setAvoidStaleDataNodesForWrite(false);
          } else {
            if (this.initialAvoidWriteStaleNodes) {
              dm.setAvoidStaleDataNodesForWrite(true);
            }
          }
        }
      }

      allAlive = dead == null;
      if (!allAlive) {
        // acquire the fsnamesystem lock, and then remove the dead node.
        namesystem.writeLock();
        try {
          if (namesystem.isInSafeMode()) {
            return;
          }
          synchronized(this) {
            dm.removeDeadDatanode(dead);
          }
        } finally {
          namesystem.writeUnlock();
        }
      }
    }
  }


  /** Periodically check heartbeat and update block key */
  private class Monitor implements Runnable {
    private long lastHeartbeatCheck;
    private long lastBlockKeyUpdate;

    @Override
    public void run() {
      while(namesystem.isRunning()) {
        try {
          final long now = Time.now();
          if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
            heartbeatCheck();
            lastHeartbeatCheck = now;
          }
          if (blockManager.shouldUpdateBlockKey(now - lastBlockKeyUpdate)) {
            synchronized(HeartbeatManager.this) {
              for(DatanodeDescriptor d : datanodes) {
                d.needKeyUpdate = true;
              }
            }
            lastBlockKeyUpdate = now;
          }
        } catch (Exception e) {
          LOG.error("Exception while checking heartbeat", e);
        }
        try {
          Thread.sleep(5000);  // 5 seconds
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /** Datanode statistics.
   * For decommissioning/decommissioned nodes, only used capacity is counted.
   */
  private static class Stats {
    private long capacityTotal = 0L;
    private long capacityUsed = 0L;
    private long capacityRemaining = 0L;
    private long blockPoolUsed = 0L;
    private int xceiverCount = 0;

    private int expiredHeartbeats = 0;

    private void add(final DatanodeDescriptor node) {
      capacityUsed += node.getDfsUsed();
      blockPoolUsed += node.getBlockPoolUsed();
      xceiverCount += node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        capacityTotal += node.getCapacity();
        capacityRemaining += node.getRemaining();
      } else {
        capacityTotal += node.getDfsUsed();
      }
    }

    private void subtract(final DatanodeDescriptor node) {
      capacityUsed -= node.getDfsUsed();
      blockPoolUsed -= node.getBlockPoolUsed();
      xceiverCount -= node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        capacityTotal -= node.getCapacity();
        capacityRemaining -= node.getRemaining();
      } else {
        capacityTotal -= node.getDfsUsed();
      }
    }
    
    /** Increment expired heartbeat counter. */
    private void incrExpiredHeartbeats() {
      expiredHeartbeats++;
    }
  }
}

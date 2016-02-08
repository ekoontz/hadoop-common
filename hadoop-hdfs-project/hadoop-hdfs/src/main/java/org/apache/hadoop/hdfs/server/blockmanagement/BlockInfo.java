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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

/**
 * BlockInfo class maintains for a given block
 * the {@link INodeFile} it is part of and datanodes where the replicas of 
 * the block are stored.
 * BlockInfo class maintains for a given block
 * the {@link BlockCollection} it is part of and datanodes where the replicas of 
 * the block are stored.
 */
@InterfaceAudience.Private
public class BlockInfo extends Block implements LightWeightGSet.LinkedElement {
  public static final BlockInfo[] EMPTY_ARRAY = {}; 

  private BlockCollection bc;

  /** For implementing {@link LightWeightGSet.LinkedElement} interface */
  private LightWeightGSet.LinkedElement nextLinkedElement;

  // Storages this block is replicated on
  private DatanodeStorageInfo[] storages;

  /**
   * Construct an entry for blocksmap
   * @param replication the block's replication factor
   */
  public BlockInfo(short replication) {
    this.storages = new DatanodeStorageInfo[replication];
    this.bc = null;
  }
  
  public BlockInfo(Block blk, short replication) {
    super(blk);
    this.storages = new DatanodeStorageInfo[replication];
    this.bc = null;
  }

  /**
   * Copy construction.
   * This is used to convert BlockInfoUnderConstruction
   * @param from BlockInfo to copy from.
   */
  protected BlockInfo(BlockInfo from) {
    this(from, from.bc.getBlockReplication());
    this.bc = from.bc;
  }

  public BlockCollection getBlockCollection() {
    return bc;
  }

  public void setBlockCollection(BlockCollection bc) {
    this.bc = bc;
  }

  public boolean isDeleted() {
    return (bc == null);
  }

  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }

  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.storages != null : "BlockInfo is not initialized";
    return storages[index];
  }

  public Iterator<DatanodeStorageInfo> getStorageInfos() {
    return new Iterator<DatanodeStorageInfo>() {

      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < storages.length && storages[index] != null;
      }

      @Override
      public DatanodeStorageInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return storages[index++];
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Sorry. can't remove.");
      }
    };
  }

  private void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.storages != null : "BlockInfo is not initialized";
    this.storages[index] = storage;
  }

  public int getCapacity() {
    assert this.storages != null : "BlockInfo is not initialized";
    return storages.length;
  }

  /**
   * Ensure that there is enough  space to include num more entries.
   * @return first free storage index.
   */
  private int ensureCapacity(int num) {
    assert this.storages != null : "BlockInfo is not initialized";
    int last = numNodes();
    if(storages.length >= (last+num))
      return last;
    /* Not enough space left. Create a new array. Should normally 
     * happen only when replication is manually increased by the user. */
    DatanodeStorageInfo[] old = storages;
    storages = new DatanodeStorageInfo[(last+num)];
    System.arraycopy(old, 0, storages, 0, last);
    return last;
  }

  /**
   * Count the number of data-nodes the block belongs to.
   */
  public int numNodes() {
    assert this.storages != null : "BlockInfo is not initialized";
    for(int idx = getCapacity()-1; idx >= 0; idx--) {
      if(getDatanode(idx) != null)
        return idx+1;
    }
    return 0;
  }

  /**
   * Add a {@link DatanodeStorageInfo} location for a block
   */
  boolean addStorage(DatanodeStorageInfo storage) {
    // find the last null node
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    return true;
  }

  /**
   * Remove {@link DatanodeStorageInfo} location for a block
   */
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if(dnIndex < 0) // the node is not found
      return false;
    // find the last not null node
    int lastNode = numNodes()-1; 
    // replace current node entry by the lastNode one
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    // set the last entry to null
    setStorageInfo(lastNode, null);
    return true;
  }

  /**
   * Find specified DatanodeStorageInfo.
   * @return DatanodeStorageInfo or null if not found.
   */
  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur == null)
        break;
      if(cur.getDatanodeDescriptor() == dn)
        return cur;
    }
    return null;
  }
  
  /**
   * Find specified DatanodeStorageInfo.
   * @return index or -1 if not found.
   */
  int findStorageInfo(DatanodeStorageInfo storageInfo) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur == storageInfo)
        return idx;
      if(cur == null)
        break;
    }
    return -1;
  }

  /**
   * BlockInfo represents a block that is not being constructed.
   * In order to start modifying the block, the BlockInfo should be converted
   * to {@link BlockInfoUnderConstruction}.
   * @return {@link BlockUCState#COMPLETE}
   */
  public BlockUCState getBlockUCState() {
    return BlockUCState.COMPLETE;
  }

  /**
   * Is this block complete?
   * 
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  /**
   * Convert a complete block to an under construction block.
   * @return BlockInfoUnderConstruction -  an under construction block.
   */
  public BlockInfoUnderConstruction convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets) {
    if(isComplete()) {
      return new BlockInfoUnderConstruction(this,
          getBlockCollection().getBlockReplication(), s, targets);
    }
    // the block is already under construction
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction)this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    return ucBlock;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }
}

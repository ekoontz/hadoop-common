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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;

/** Namesystem operations. */
@InterfaceAudience.Private
public interface Namesystem extends RwLock, SafeMode {
  /** Is this name system running? */
  public boolean isRunning();

  /** Check if the user has superuser privilege. */
  public void checkSuperuserPrivilege() throws AccessControlException;

  /** @return the block pool ID */
  public String getBlockPoolId();

  public boolean isInStandbyState();

  public boolean isGenStampInFuture(Block block);

  public void adjustSafeModeBlockTotals(int deltaSafe, int deltaTotal);

  public void checkOperation(OperationCategory read) throws StandbyException;

  public boolean isInSnapshot(BlockInfoUnderConstruction blockUC);
}
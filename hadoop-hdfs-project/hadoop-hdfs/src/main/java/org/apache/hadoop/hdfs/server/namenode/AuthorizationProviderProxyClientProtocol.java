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

import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

public class AuthorizationProviderProxyClientProtocol implements ClientProtocol {
  private ClientProtocol server;
  
  public AuthorizationProviderProxyClientProtocol(ClientProtocol server) {
    this.server = server;
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getBlockLocations(src, offset, length);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return server.getServerDefaults();
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize, CryptoProtocolVersion[] supportedVersions)
      throws AccessControlException, AlreadyBeingCreatedException,
             DSQuotaExceededException, FileAlreadyExistsException,
             FileNotFoundException, NSQuotaExceededException,
             ParentNotDirectoryException, SafeModeException,
             UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.create(src, masked, clientName, flag, createParent,
          replication, blockSize, supportedVersions);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public LocatedBlock append(String src, String clientName)
      throws AccessControlException, DSQuotaExceededException,
             FileNotFoundException, SafeModeException, UnresolvedLinkException,
             SnapshotAccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.append(src, clientName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws AccessControlException, DSQuotaExceededException,
             FileNotFoundException, SafeModeException, UnresolvedLinkException,
             SnapshotAccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.setReplication(src, replication);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getStoragePolicies();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
     throws SnapshotAccessControlException, UnresolvedLinkException,
     FileNotFoundException, QuotaExceededException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setStoragePolicy(src, policyName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
             UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setPermission(src, permission);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws AccessControlException, FileNotFoundException, SafeModeException,
             UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setOwner(src, username, groupname);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
      String holder) throws AccessControlException, FileNotFoundException,
                            UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.abandonBlock(b, fileId, src, holder);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
      String[] favoredNodes)
      throws AccessControlException, FileNotFoundException,
             NotReplicatedYetException, SafeModeException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.addBlock(src, clientName, previous, excludeNodes, fileId, 
          favoredNodes);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, long fileId,
      ExtendedBlock blk, DatanodeInfo[] existings,
      String[] existingStorageIDs, DatanodeInfo[] excludes,
      int numAdditionalNodes, String clientName)
      throws AccessControlException, FileNotFoundException, SafeModeException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getAdditionalDatanode(src, fileId, blk, existings, 
          existingStorageIDs, excludes, numAdditionalNodes, clientName);
    } finally {
      AuthorizationProvider.endClientOp();
    }

  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last,
      long fileId)
      throws AccessControlException, FileNotFoundException, SafeModeException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.complete(src, clientName, last, fileId);
    } finally {
      AuthorizationProvider.endClientOp();
    }

  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.reportBadBlocks(blocks);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean rename(String src, String dst)
      throws UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.rename(src, dst);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void concat(String trg, String[] srcs)
      throws IOException, UnresolvedLinkException,
             SnapshotAccessControlException {
    try {
      AuthorizationProvider.beginClientOp();
      server.concat(trg, srcs);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void rename2(String src, String dst, Options.Rename... options)
      throws AccessControlException, DSQuotaExceededException,
             FileAlreadyExistsException, FileNotFoundException,
             NSQuotaExceededException, ParentNotDirectoryException,
             SafeModeException, UnresolvedLinkException,
             SnapshotAccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.rename2(src, dst, options);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean delete(String src, boolean recursive)
      throws AccessControlException, FileNotFoundException, SafeModeException,
             UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.delete(src, recursive);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
             FileNotFoundException, NSQuotaExceededException,
             ParentNotDirectoryException, SafeModeException,
             UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.mkdirs(src, masked, createParent);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getListing(src, startAfter, needLocation);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getSnapshottableDirListing();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void renewLease(String clientName)
      throws AccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.renewLease(clientName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.recoverLease(src, clientName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public long[] getStats() throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getStats();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getDatanodeReport(type);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      HdfsConstants.DatanodeReportType type) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getDatanodeStorageReport(type);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public long getPreferredBlockSize(String filename)
      throws IOException, UnresolvedLinkException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getPreferredBlockSize(filename);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action,
      boolean isChecked) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.setSafeMode(action, isChecked);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void saveNamespace() throws AccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.saveNamespace();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public long rollEdits() throws AccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.rollEdits();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean restoreFailedStorage(String arg)
      throws AccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.restoreFailedStorage(arg);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void refreshNodes() throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.refreshNodes();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.finalizeUpgrade();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(
      HdfsConstants.RollingUpgradeAction action) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.rollingUpgrade(action);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.listCorruptFileBlocks(path, cookie);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void metaSave(String filename) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.metaSave(filename);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setBalancerBandwidth(bandwidth);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public HdfsFileStatus getFileInfo(String src)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getFileInfo(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public boolean isFileClosed(String src)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.isFileClosed(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src)
      throws AccessControlException, UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getFileLinkInfo(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public ContentSummary getContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getContentSummary(path);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setQuota(path, namespaceQuota, diskspaceQuota);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void fsync(String src, long inodeId, String client,
      long lastBlockLength)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.fsync(src, inodeId, client, lastBlockLength);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setTimes(String src, long mtime, long atime)
      throws AccessControlException, FileNotFoundException,
             UnresolvedLinkException, SnapshotAccessControlException,
             IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setTimes(src, mtime, atime);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerm,
      boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
             FileNotFoundException, ParentNotDirectoryException,
             SafeModeException, UnresolvedLinkException,
             SnapshotAccessControlException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.createSymlink(target, link, dirPerm, createParent);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public String getLinkTarget(String path)
      throws AccessControlException, FileNotFoundException, IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getLinkTarget(path);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.updateBlockForPipeline(block, clientName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.updatePipeline(clientName, oldBlock, newBlock, newNodes, 
          newStorageIDs);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getDelegationToken(renewer);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.renewDelegationToken(token);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.cancelDelegationToken(token);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getDataEncryptionKey();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.createSnapshot(snapshotRoot, snapshotName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.deleteSnapshot(snapshotRoot, snapshotName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.renameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.allowSnapshot(snapshotRoot);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.disallowSnapshot(snapshotRoot);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String fromSnapshot, String toSnapshot) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getSnapshotDiffReport(snapshotRoot, fromSnapshot, 
          toSnapshot);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.addCacheDirective(directive, flags);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.modifyCacheDirective(directive, flags);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.removeCacheDirective(id);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> 
      listCacheDirectives(long prevId, CacheDirectiveInfo filter) 
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.listCacheDirectives(prevId, filter);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.addCachePool(info);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void modifyCachePool(CachePoolInfo req) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.modifyCachePool(req);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void removeCachePool(String pool) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.removeCachePool(pool);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(
      String prevPool) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.listCachePools(prevPool);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.modifyAclEntries(src, aclSpec);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.removeAclEntries(src, aclSpec);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.removeDefaultAcl(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void removeAcl(String src) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.removeAcl(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setAcl(src, aclSpec);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getAclStatus(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.createEncryptionZone(src, keyName);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public EncryptionZone getEZForPath(String src) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getEZForPath(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<EncryptionZone> 
      listEncryptionZones(long prevId) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.listEncryptionZones(prevId);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.setXAttr(src, xAttr, flag);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getXAttrs(src, xAttrs);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.listXAttrs(src);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.removeXAttr(src, xAttr);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      server.checkAccess(path, mode);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public long getCurrentEditLogTxid() throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getCurrentEditLogTxid();
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

  @Override
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    try {
      AuthorizationProvider.beginClientOp();
      return server.getEditsFromTxid(txid);
    } finally {
      AuthorizationProvider.endClientOp();
    }
  }

}

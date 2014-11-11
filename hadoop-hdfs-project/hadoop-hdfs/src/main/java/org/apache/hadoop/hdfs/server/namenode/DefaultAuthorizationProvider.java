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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.Stack;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DefaultAuthorizationProvider
    extends AuthorizationProvider {

  @Override
  public void setUser(INodeAuthorizationInfo node, String user) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    inode.updatePermissionStatus(
        INodeWithAdditionalFields.PermissionStatusFormat.USER, n);
  }

  @Override
  public String getUser(INodeAuthorizationInfo node, int snapshotId) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return inode.getSnapshotINode(snapshotId).getUserName();
    }
    return INodeWithAdditionalFields.PermissionStatusFormat.
        getUser(inode.getPermissionLong());
  }

  @Override
  public void setGroup(INodeAuthorizationInfo node, String group) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    inode.updatePermissionStatus(
        INodeWithAdditionalFields.PermissionStatusFormat.GROUP, n);
  }

  @Override
  public String getGroup(INodeAuthorizationInfo node, int snapshotId) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return inode.getSnapshotINode(snapshotId).getGroupName();
    }
    return INodeWithAdditionalFields.PermissionStatusFormat.
        getGroup(inode.getPermissionLong());
  }

  @Override
  public void setPermission(INodeAuthorizationInfo node, 
      FsPermission permission) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    final short mode = permission.toShort();
    inode.updatePermissionStatus(INodeWithAdditionalFields.
        PermissionStatusFormat.MODE, mode);
  }

  @Override
  public FsPermission getFsPermission(INodeAuthorizationInfo node, 
      int snapshotId) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return inode.getSnapshotINode(snapshotId).getFsPermission();
    }
    return new FsPermission(inode.getFsPermissionShort());
  }

  @Override
  public AclFeature getAclFeature(INodeAuthorizationInfo node,
      int snapshotId) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return inode.getSnapshotINode(snapshotId).getAclFeature();
    }
    return inode.getFeature(AclFeature.class);
  }

  @Override
  public void removeAclFeature(INodeAuthorizationInfo node) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    AclFeature f = inode.getAclFeature();
    Preconditions.checkNotNull(f);
    inode.removeFeature(f);
  }

  @Override
  public void addAclFeature(INodeAuthorizationInfo node, AclFeature f) {
    INodeWithAdditionalFields inode = (INodeWithAdditionalFields) node;
    AclFeature f1 = inode.getAclFeature();
    if (f1 != null) {
      throw new IllegalStateException("Duplicated ACLFeature");
    }
    inode.addFeature(f);
  }

  @Override
  public void checkPermission(String user, Set<String> groups,
      INodeAuthorizationInfo[] nodes, int snapshotId,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException, UnresolvedLinkException {
    INode[] inodes = (INode[]) nodes;
    int ancestorIndex = inodes.length - 2;
    for (; ancestorIndex >= 0 && inodes[ancestorIndex] == null;
         ancestorIndex--)
      ;
    checkTraverse(user, groups, inodes, ancestorIndex, snapshotId);

    final INode last = inodes[inodes.length - 1];
    if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
        && inodes.length > 1 && last != null) {
      checkStickyBit(user, inodes[inodes.length - 2], last, snapshotId);
    }
    if (ancestorAccess != null && inodes.length > 1) {
      check(user, groups, inodes, ancestorIndex, snapshotId, ancestorAccess);
    }
    if (parentAccess != null && inodes.length > 1) {
      check(user, groups, inodes, inodes.length - 2, snapshotId, parentAccess);
    }
    if (access != null) {
      check(user, groups, last, snapshotId, access);
    }
    if (subAccess != null) {
      checkSubAccess(user, groups, last, snapshotId, subAccess, ignoreEmptyDir);
    }
    if (doCheckOwner) {
      checkOwner(user, last, snapshotId);
    }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkOwner(String user, INode inode, int snapshotId
  ) throws AccessControlException {
    if (inode != null && user.equals(inode.getUserName(snapshotId))) {
      return;
    }
    throw new AccessControlException(
       "Permission denied. user="
       + user + " is not the owner of inode=" + inode);
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkTraverse(String user, Set<String> groups, INode[] inodes,
      int last, int snapshotId) throws AccessControlException {
    for (int j = 0; j <= last; j++) {
      check(user, groups, inodes[j], snapshotId, FsAction.EXECUTE);
    }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkSubAccess(String user, Set<String> groups, INode inode,
      int snapshotId, FsAction access, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (inode == null || !inode.isDirectory()) {
      return;
    }

    Stack<INodeDirectory> directories = new Stack<INodeDirectory>();
    for (directories.push(inode.asDirectory()); !directories.isEmpty(); ) {
      INodeDirectory d = directories.pop();
      ReadOnlyList<INode> cList = d.getChildrenList(snapshotId);
      if (!(cList.isEmpty() && ignoreEmptyDir)) {
        check(user, groups, d, snapshotId, access);
      }

      for (INode child : cList) {
        if (child.isDirectory()) {
          directories.push(child.asDirectory());
        }
      }
    }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void check(String user, Set<String> groups, INode[] inodes, int i,
      int snapshotId, FsAction access
  ) throws AccessControlException {
    check(user, groups, i >= 0 ? inodes[i] : null, snapshotId, access);
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void check(String user, Set<String> groups, INode inode,
      int snapshotId, FsAction access) throws AccessControlException {
    if (inode == null) {
      return;
    }
    FsPermission mode = inode.getFsPermission(snapshotId);
    AclFeature aclFeature = inode.getAclFeature(snapshotId);
    if (aclFeature != null) {
      List<AclEntry> featureEntries = aclFeature.getEntries();
      // It's possible that the inode has a default ACL but no access ACL.
      if (featureEntries.get(0).getScope() == AclEntryScope.ACCESS) {
        checkAccessAcl(user, groups, inode, snapshotId, access, mode,
            featureEntries);
        return;
      }
    }
    checkFsPermission(user, groups, inode, snapshotId, access, mode);
  }

  private void checkFsPermission(String user, Set<String> groups, INode inode,
      int snapshotId, FsAction access, FsPermission mode)
      throws AccessControlException {
    if (user.equals(inode.getUserName(snapshotId))) { //user class
      if (mode.getUserAction().implies(access)) {
        return;
      }
    } else if (groups.contains(inode.getGroupName(snapshotId))) { //group class
      if (mode.getGroupAction().implies(access)) {
        return;
      }
    } else { //other class
      if (mode.getOtherAction().implies(access)) {
        return;
      }
    }
    throw new AccessControlException(
        toAccessControlString(user, inode, snapshotId, access, mode));
  }

  /**
   * Checks requested access against an Access Control List.  This method relies
   * on finding the ACL data in the relevant portions of {@link FsPermission} 
   * and {@link AclFeature} as implemented in the logic of {@link AclStorage}. 
   * This method also relies on receiving the ACL entries in sorted order.  This
   * is assumed to be true, because the ACL modification methods in
   * {@link AclTransformation} sort the resulting entries.
   * <p/>
   * More specifically, this method depends on these invariants in an ACL:
   * - The list must be sorted.
   * - Each entry in the list must be unique by scope + type + name.
   * - There is exactly one each of the unnamed user/group/other entries.
   * - The mask entry must not have a name.
   * - The other entry must not have a name.
   * - Default entries may be present, but they are ignored during enforcement.
   *
   * @param inode INode accessed inode
   * @param snapshotId int snapshot ID
   * @param access FsAction requested permission
   * @param mode FsPermission mode from inode
   * @param featureEntries List<AclEntry> ACL entries from AclFeature of inode
   * @throws AccessControlException if the ACL denies permission
   */
  private void checkAccessAcl(String user, Set<String> groups, INode inode,
      int snapshotId,  FsAction access, FsPermission mode,
      List<AclEntry> featureEntries) throws AccessControlException {
    boolean foundMatch = false;

    // Use owner entry from permission bits if user is owner.
    if (user.equals(inode.getUserName(snapshotId))) {
      if (mode.getUserAction().implies(access)) {
        return;
      }
      foundMatch = true;
    }

    // Check named user and group entries if user was not denied by owner entry.
    if (!foundMatch) {
      for (AclEntry entry : featureEntries) {
        if (entry.getScope() == AclEntryScope.DEFAULT) {
          break;
        }
        AclEntryType type = entry.getType();
        String name = entry.getName();
        if (type == AclEntryType.USER) {
          // Use named user entry with mask from permission bits applied if user
          // matches name.
          if (user.equals(name)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
            break;
          }
        } else if (type == AclEntryType.GROUP) {
          // Use group entry (unnamed or named) with mask from permission bits
          // applied if user is a member and entry grants access.  If user is a
          // member of multiple groups that have entries that grant access, then
          // it doesn't matter which is chosen, so exit early after first match.
          String group = name == null ? inode.getGroupName(snapshotId) : name;
          if (groups.contains(group)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
          }
        }
      }
    }

    // Use other entry if user was not denied by an earlier match.
    if (!foundMatch && mode.getOtherAction().implies(access)) {
      return;
    }

    throw new AccessControlException(
        toAccessControlString(user, inode, snapshotId, access, mode,
            featureEntries));
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkStickyBit(String user, INode parent, INode inode,
      int snapshotId) throws AccessControlException {
    if (!parent.getFsPermission(snapshotId).getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if (parent.getUserName(snapshotId).equals(user)) {
      return;
    }

    // if this user is the file owner, return
    if (inode.getUserName(snapshotId).equals(user)) {
      return;
    }

    throw new AccessControlException("Permission denied by sticky bit setting:" 
        + " user=" + user + ", inode=" + inode);
  }

  /**
   * @return a string for throwing {@link AccessControlException}
   */
  private String toAccessControlString(String user, INode inode, int snapshotId,
      FsAction access, FsPermission mode) {
    return toAccessControlString(user, inode, snapshotId, access, mode, null);
  }

  /**
   * @return a string for throwing {@link AccessControlException}
   */
  private String toAccessControlString(String user, INode inode, int snapshotId,
      FsAction access, FsPermission mode, List<AclEntry> featureEntries) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
        .append("user=").append(user).append(", ")
        .append("access=").append(access).append(", ")
        .append("inode=\"").append(inode.getFullPathName()).append("\":")
        .append(inode.getUserName(snapshotId)).append(':')
        .append(inode.getGroupName(snapshotId)).append(':')
        .append(inode.isDirectory() ? 'd' : '-')
        .append(mode);
    if (featureEntries != null) {
      sb.append(':').append(StringUtils.join(",", featureEntries));
    }
    return sb.toString();
  }

}

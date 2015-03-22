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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import com.google.common.base.Preconditions;

/**
 * Contains INodes information resolved from a given path.
 */
public class INodesInPath {
  public static final Log LOG = LogFactory.getLog(INodesInPath.class);

  /**
   * @return true if path component is {@link HdfsConstants#DOT_SNAPSHOT_DIR}
   */
  private static boolean isDotSnapshotDir(byte[] pathComponent) {
    return pathComponent == null ? false
        : Arrays.equals(HdfsConstants.DOT_SNAPSHOT_DIR_BYTES, pathComponent);
  }

  /**
   * Given some components, create a path name.
   * @param components The path components
   * @param start index
   * @param end index
   * @return concatenated path
   */
  private static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components) throws UnresolvedLinkException {
    return resolve(startingDir, components, components.length, false);
  }

  /**
   * Retrieve existing INodes from a path. If existing is big enough to store
   * all path components (existing and non-existing), then existing INodes
   * will be stored starting from the root INode into existing[0]; if
   * existing is not big enough to store all path components, then only the
   * last existing and non existing INodes will be stored so that
   * existing[existing.length-1] refers to the INode of the final component.
   * 
   * An UnresolvedPathException is always thrown when an intermediate path 
   * component refers to a symbolic link. If the final path component refers 
   * to a symbolic link then an UnresolvedPathException is only thrown if
   * resolveLink is true.  
   * 
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"],
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?])</code> should fill the
   * array with [c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?])</code> should fill the
   * array with [null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?])</code> should fill the
   * array with [c1,c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?])</code> should fill
   * the array with [c2,null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?,?,?])</code> should fill
   * the array with [rootINode,c1,c2,null], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
   * fill the array with [rootINode,c1,c2,null]
   * 
   * @param startingDir the starting directory
   * @param components array of path component name
   * @param numOfINodes number of INodes to return
   * @param resolveLink indicates whether UnresolvedLinkException should
   *        be thrown when the path refers to a symbolic link.
   * @return the specified number of existing INodes in the path
   */
  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components, final int numOfINodes, 
      final boolean resolveLink) throws UnresolvedLinkException {
    Preconditions.checkArgument(startingDir.compareTo(components[0]) == 0);

    INode curNode = startingDir;
    final INodesInPath existing = new INodesInPath(components, numOfINodes);
    int count = 0;
    int index = numOfINodes - components.length;
    if (index > 0) {
      index = 0;
    }
    while (count < components.length && curNode != null) {
      final boolean lastComp = (count == components.length - 1);      
      if (index >= 0) {
        existing.addNode(curNode);
      }
      final boolean isRef = curNode.isReference();
      final boolean isDir = curNode.isDirectory();
      final INodeDirectory dir = isDir? curNode.asDirectory(): null;  
      if (!isRef && isDir && dir instanceof INodeDirectoryWithSnapshot) {
        //if the path is a non-snapshot path, update the latest snapshot.
        if (!existing.isSnapshot()) {
          existing.updateLatestSnapshot(
              ((INodeDirectoryWithSnapshot)dir).getLastSnapshot());
        }
      } else if (isRef && isDir && !lastComp) {
        // If the curNode is a reference node, need to check its dstSnapshot:
        // 1. if the existing snapshot is no later than the dstSnapshot (which
        // is the latest snapshot in dst before the rename), the changes 
        // should be recorded in previous snapshots (belonging to src).
        // 2. however, if the ref node is already the last component, we still 
        // need to know the latest snapshot among the ref node's ancestors, 
        // in case of processing a deletion operation. Thus we do not overwrite
        // the latest snapshot if lastComp is true. In case of the operation is
        // a modification operation, we do a similar check in corresponding 
        // recordModification method.
        if (!existing.isSnapshot()) {
          int dstSnapshotId = curNode.asReference().getDstSnapshotId();
          Snapshot latest = existing.getLatestSnapshot();
          if (latest == null ||  // no snapshot in dst tree of rename
              dstSnapshotId >= latest.getId()) { // the above scenario 
            Snapshot lastSnapshot = null;
            if (curNode.isDirectory()
                && curNode.asDirectory() instanceof INodeDirectoryWithSnapshot) {
              lastSnapshot = ((INodeDirectoryWithSnapshot) curNode
                  .asDirectory()).getLastSnapshot();
            }
            existing.setSnapshot(lastSnapshot);
          }
        }
      }
      if (curNode.isSymlink() && (!lastComp || (lastComp && resolveLink))) {
        final String path = constructPath(components, 0, components.length);
        final String preceding = constructPath(components, 0, count);
        final String remainder =
          constructPath(components, count + 1, components.length);
        final String link = DFSUtil.bytes2String(components[count]);
        final String target = curNode.asSymlink().getSymlinkString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("UnresolvedPathException " +
            " path: " + path + " preceding: " + preceding +
            " count: " + count + " link: " + link + " target: " + target +
            " remainder: " + remainder);
        }
        throw new UnresolvedPathException(path, preceding, remainder, target);
      }
      if (lastComp || !isDir) {
        break;
      }
      final byte[] childName = components[count + 1];
      
      // check if the next byte[] in components is for ".snapshot"
      if (isDotSnapshotDir(childName)
          && isDir && dir instanceof INodeDirectorySnapshottable) {
        // skip the ".snapshot" in components
        count++;
        index++;
        existing.isSnapshot = true;
        if (index >= 0) { // decrease the capacity by 1 to account for .snapshot
          existing.capacity--;
        }
        // check if ".snapshot" is the last element of components
        if (count == components.length - 1) {
          break;
        }
        // Resolve snapshot root
        final Snapshot s = ((INodeDirectorySnapshottable)dir).getSnapshot(
            components[count + 1]);
        if (s == null) {
          //snapshot not found
          curNode = null;
        } else {
          curNode = s.getRoot();
          existing.setSnapshot(s);
        }
        if (index >= -1) {
          existing.snapshotRootIndex = existing.numNonNull;
        }
      } else {
        // normal case, and also for resolving file/dir under snapshot root
        curNode = dir.getChild(childName, existing.getPathSnapshot());
      }
      count++;
      index++;
    }
    return existing;
  }

  private final byte[][] path;
  /**
   * Array with the specified number of INodes resolved for a given path.
   */
  private INode[] inodes;
  /**
   * Indicate the number of non-null elements in {@link #inodes}
   */
  private int numNonNull;
  /**
   * The path for a snapshot file/dir contains the .snapshot thus makes the
   * length of the path components larger the number of inodes. We use
   * the capacity to control this special case.
   */
  private int capacity;
  /**
   * true if this path corresponds to a snapshot
   */
  private boolean isSnapshot;
  /**
   * Index of {@link INodeDirectoryWithSnapshot} for snapshot path, else -1
   */
  private int snapshotRootIndex;
  /**
   * For snapshot paths, it is the reference to the snapshot; or null if the
   * snapshot does not exist. For non-snapshot paths, it is the reference to
   * the latest snapshot found in the path; or null if no snapshot is found.
   */
  private Snapshot snapshot = null; 

  private INodesInPath(byte[][] path, int number) {
    this.path = path;
    assert (number >= 0);
    inodes = new INode[number];
    capacity = number;
    numNonNull = 0;
    isSnapshot = false;
    snapshotRootIndex = -1;
  }

  /**
   * For non-snapshot paths, return the latest snapshot found in the path.
   * For snapshot paths, return null.
   */
  public Snapshot getLatestSnapshot() {
    return isSnapshot? null: snapshot;
  }
  
  /**
   * For snapshot paths, return the snapshot specified in the path.
   * For non-snapshot paths, return null.
   */
  public Snapshot getPathSnapshot() {
    return isSnapshot? snapshot: null;
  }

  private void setSnapshot(Snapshot s) {
    snapshot = s;
  }
  
  private void updateLatestSnapshot(Snapshot s) {
    if (snapshot == null
        || (s != null && Snapshot.ID_COMPARATOR.compare(snapshot, s) < 0)) {
      snapshot = s;
    }
  }

  /**
   * @return the whole inodes array including the null elements.
   */
  INode[] getINodes() {
    if (capacity < inodes.length) {
      INode[] newNodes = new INode[capacity];
      System.arraycopy(inodes, 0, newNodes, 0, capacity);
      inodes = newNodes;
    }
    return inodes;
  }
  
  /**
   * @return the i-th inode if i >= 0;
   *         otherwise, i < 0, return the (length + i)-th inode.
   */
  public INode getINode(int i) {
    return inodes[i >= 0? i: inodes.length + i];
  }
  
  /** @return the last inode. */
  public INode getLastINode() {
    return inodes[inodes.length - 1];
  }

  byte[] getLastLocalName() {
    return path[path.length - 1];
  }
  
  /**
   * @return index of the {@link INodeDirectoryWithSnapshot} in
   *         {@link #inodes} for snapshot path, else -1.
   */
  int getSnapshotRootIndex() {
    return this.snapshotRootIndex;
  }
  
  /**
   * @return isSnapshot true for a snapshot path
   */
  boolean isSnapshot() {
    return this.isSnapshot;
  }
  
  /**
   * Add an INode at the end of the array
   */
  private void addNode(INode node) {
    inodes[numNonNull++] = node;
  }
  
  void setINode(int i, INode inode) {
    inodes[i >= 0? i: inodes.length + i] = inode;
  }
  
  void setLastINode(INode last) {
    inodes[inodes.length - 1] = last;
  }
  
  /**
   * @return The number of non-null elements
   */
  int getNumNonNull() {
    return numNonNull;
  }
  
  private static String toString(INode inode) {
    return inode == null? null: inode.getLocalName();
  }

  @Override
  public String toString() {
    return toString(true);
  }

  private String toString(boolean vaildateObject) {
    if (vaildateObject) {
      vaildate();
    }

    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append(": path = ").append(DFSUtil.byteArray2PathString(path))
        .append("\n  inodes = ");
    if (inodes == null) {
      b.append("null");
    } else if (inodes.length == 0) {
      b.append("[]");
    } else {
      b.append("[").append(toString(inodes[0]));
      for(int i = 1; i < inodes.length; i++) {
        b.append(", ").append(toString(inodes[i]));
      }
      b.append("], length=").append(inodes.length);
    }
    b.append("\n  numNonNull = ").append(numNonNull)
     .append("\n  capacity   = ").append(capacity)
     .append("\n  isSnapshot        = ").append(isSnapshot)
     .append("\n  snapshotRootIndex = ").append(snapshotRootIndex)
     .append("\n  snapshot          = ").append(snapshot);
    return b.toString();
  }

  void vaildate() {
    // check parent up to snapshotRootIndex or numNonNull
    final int n = snapshotRootIndex >= 0? snapshotRootIndex + 1: numNonNull;  
    int i = 0;
    if (inodes[i] != null) {
      for(i++; i < n && inodes[i] != null; i++) {
        final INodeDirectory parent_i = inodes[i].getParent();
        final INodeDirectory parent_i_1 = inodes[i-1].getParent();
        if (parent_i != inodes[i-1] &&
            (parent_i_1 == null || !parent_i_1.isSnapshottable()
                || parent_i != parent_i_1)) {
          throw new AssertionError(
              "inodes[" + i + "].getParent() != inodes[" + (i-1)
              + "]\n  inodes[" + i + "]=" + inodes[i].toDetailString()
              + "\n  inodes[" + (i-1) + "]=" + inodes[i-1].toDetailString()
              + "\n this=" + toString(false));
        }
      }
    }
    if (i != n) {
      throw new AssertionError("i = " + i + " != " + n
          + ", this=" + toString(false));
    }
  }
}
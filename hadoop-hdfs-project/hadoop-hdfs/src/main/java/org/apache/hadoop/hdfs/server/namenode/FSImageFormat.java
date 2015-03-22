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

import static org.apache.hadoop.util.Time.now;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileUnderConstructionWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;

/**
 * Contains inner classes for reading or writing the on-disk format for
 * FSImages.
 * 
 * In particular, the format of the FSImage looks like:
 * <pre>
 * FSImage {
 *   layoutVersion: int, namespaceID: int, numberItemsInFSDirectoryTree: long,
 *   namesystemGenerationStampV1: long, namesystemGenerationStampV2: long,
 *   generationStampAtBlockIdSwitch:long, lastAllocatedBlockId:
 *   long transactionID: long, snapshotCounter: int, numberOfSnapshots: int,
 *   numOfSnapshottableDirs: int,
 *   {FSDirectoryTree, FilesUnderConstruction, SecretManagerState} (can be compressed)
 * }
 * 
 * FSDirectoryTree (if {@link Feature#FSIMAGE_NAME_OPTIMIZATION} is supported) {
 *   INodeInfo of root, numberOfChildren of root: int
 *   [list of INodeInfo of root's children],
 *   [list of INodeDirectoryInfo of root's directory children]
 * }
 * 
 * FSDirectoryTree (if {@link Feature#FSIMAGE_NAME_OPTIMIZATION} not supported){
 *   [list of INodeInfo of INodes in topological order]
 * }
 * 
 * INodeInfo {
 *   {
 *     localName: short + byte[]
 *   } when {@link Feature#FSIMAGE_NAME_OPTIMIZATION} is supported
 *   or 
 *   {
 *     fullPath: byte[]
 *   } when {@link Feature#FSIMAGE_NAME_OPTIMIZATION} is not supported
 *   replicationFactor: short, modificationTime: long,
 *   accessTime: long, preferredBlockSize: long,
 *   numberOfBlocks: int (-1 for INodeDirectory, -2 for INodeSymLink),
 *   { 
 *     nsQuota: long, dsQuota: long, 
 *     {
 *       isINodeSnapshottable: byte,
 *       isINodeWithSnapshot: byte (if isINodeSnapshottable is false)
 *     } (when {@link Feature#SNAPSHOT} is supported), 
 *     fsPermission: short, PermissionStatus
 *   } for INodeDirectory
 *   or 
 *   {
 *     symlinkString, fsPermission: short, PermissionStatus
 *   } for INodeSymlink
 *   or
 *   {
 *     [list of BlockInfo]
 *     [list of FileDiff]
 *     {
 *       isINodeFileUnderConstructionSnapshot: byte, 
 *       {clientName: short + byte[], clientMachine: short + byte[]} (when 
 *       isINodeFileUnderConstructionSnapshot is true),
 *     } (when {@link Feature#SNAPSHOT} is supported and writing snapshotINode), 
 *     fsPermission: short, PermissionStatus
 *   } for INodeFile
 * }
 * 
 * INodeDirectoryInfo {
 *   fullPath of the directory: short + byte[],
 *   numberOfChildren: int, [list of INodeInfo of children INode],
 *   {
 *     numberOfSnapshots: int,
 *     [list of Snapshot] (when NumberOfSnapshots is positive),
 *     numberOfDirectoryDiffs: int,
 *     [list of DirectoryDiff] (NumberOfDirectoryDiffs is positive),
 *     number of children that are directories,
 *     [list of INodeDirectoryInfo of the directory children] (includes
 *     snapshot copies of deleted sub-directories)
 *   } (when {@link Feature#SNAPSHOT} is supported), 
 * }
 * 
 * Snapshot {
 *   snapshotID: int, root of Snapshot: INodeDirectoryInfo (its local name is 
 *   the name of the snapshot)
 * }
 * 
 * DirectoryDiff {
 *   full path of the root of the associated Snapshot: short + byte[], 
 *   childrenSize: int, 
 *   isSnapshotRoot: byte, 
 *   snapshotINodeIsNotNull: byte (when isSnapshotRoot is false),
 *   snapshotINode: INodeDirectory (when SnapshotINodeIsNotNull is true), Diff 
 * }
 * 
 * Diff {
 *   createdListSize: int, [Local name of INode in created list],
 *   deletedListSize: int, [INode in deleted list: INodeInfo]
 * }
 *
 * FileDiff {
 *   full path of the root of the associated Snapshot: short + byte[], 
 *   fileSize: long, 
 *   snapshotINodeIsNotNull: byte,
 *   snapshotINode: INodeFile (when SnapshotINodeIsNotNull is true), Diff 
 * }
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageFormat {
  private static final Log LOG = FSImage.LOG;
  
  // Static-only class
  private FSImageFormat() {}
  
  /**
   * A one-shot class responsible for loading an image. The load() function
   * should be called once, after which the getter methods may be used to retrieve
   * information about the image that was loaded, if loading was successful.
   */
  public static class Loader {
    private final Configuration conf;
    /** which namesystem this loader is working for */
    private final FSNamesystem namesystem;

    /** Set to true once a file has been loaded using this loader. */
    private boolean loaded = false;

    /** The transaction ID of the last edit represented by the loaded file */
    private long imgTxId;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;
    
    private Map<Integer, Snapshot> snapshotMap = null;
    private final ReferenceMap referenceMap = new ReferenceMap();

    Loader(Configuration conf, FSNamesystem namesystem) {
      this.conf = conf;
      this.namesystem = namesystem;
    }

    /**
     * Return the MD5 checksum of the image that has been loaded.
     * @throws IllegalStateException if load() has not yet been called.
     */
    MD5Hash getLoadedImageMd5() {
      checkLoaded();
      return imgDigest;
    }

    long getLoadedImageTxId() {
      checkLoaded();
      return imgTxId;
    }

    /**
     * Throw IllegalStateException if load() has not yet been called.
     */
    private void checkLoaded() {
      if (!loaded) {
        throw new IllegalStateException("Image not yet loaded!");
      }
    }

    /**
     * Throw IllegalStateException if load() has already been called.
     */
    private void checkNotLoaded() {
      if (loaded) {
        throw new IllegalStateException("Image already loaded!");
      }
    }

    void load(File curFile) throws IOException {
      checkNotLoaded();
      assert curFile != null : "curFile is null";

      StartupProgress prog = NameNode.getStartupProgress();
      Step step = new Step(StepType.INODES);
      prog.beginStep(Phase.LOADING_FSIMAGE, step);
      long startTime = now();

      //
      // Load in bits
      //
      MessageDigest digester = MD5Hash.getDigester();
      DigestInputStream fin = new DigestInputStream(
           new FileInputStream(curFile), digester);

      DataInputStream in = new DataInputStream(fin);
      try {
        // read image version: first appeared in version -1
        int imgVersion = in.readInt();
        if (getLayoutVersion() != imgVersion) {
          throw new InconsistentFSStateException(curFile, 
              "imgVersion " + imgVersion +
              " expected to be " + getLayoutVersion());
        }
        boolean supportSnapshot = LayoutVersion.supports(Feature.SNAPSHOT,
            imgVersion);

        // read namespaceID: first appeared in version -2
        in.readInt();

        long numFiles = in.readLong();

        // read in the last generation stamp for legacy blocks.
        long genstamp = in.readLong();
        namesystem.setGenerationStampV1(genstamp);
        
        if (LayoutVersion.supports(Feature.SEQUENTIAL_BLOCK_ID, imgVersion)) {
          // read the starting generation stamp for sequential block IDs
          genstamp = in.readLong();
          namesystem.setGenerationStampV2(genstamp);

          // read the last generation stamp for blocks created after
          // the switch to sequential block IDs.
          long stampAtIdSwitch = in.readLong();
          namesystem.setGenerationStampV1Limit(stampAtIdSwitch);

          // read the max sequential block ID.
          long maxSequentialBlockId = in.readLong();
          namesystem.setLastAllocatedBlockId(maxSequentialBlockId);
        } else {
          long startingGenStamp = namesystem.upgradeGenerationStampToV2();
          // This is an upgrade.
          LOG.info("Upgrading to sequential block IDs. Generation stamp " +
                   "for new blocks set to " + startingGenStamp);
        }

        // read the transaction ID of the last edit represented by
        // this image
        if (LayoutVersion.supports(Feature.STORED_TXIDS, imgVersion)) {
          imgTxId = in.readLong();
        } else {
          imgTxId = 0;
        }

        // read the last allocated inode id in the fsimage
        if (LayoutVersion.supports(Feature.ADD_INODE_ID, imgVersion)) {
          long lastInodeId = in.readLong();
          namesystem.resetLastInodeId(lastInodeId);
          if (LOG.isDebugEnabled()) {
            LOG.debug("load last allocated InodeId from fsimage:" + lastInodeId);
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Old layout version doesn't have inode id."
                + " Will assign new id for each inode.");
          }
        }
        
        if (supportSnapshot) {
          snapshotMap = namesystem.getSnapshotManager().read(in, this);
        }

        // read compression related info
        FSImageCompression compression;
        if (LayoutVersion.supports(Feature.FSIMAGE_COMPRESSION, imgVersion)) {
          compression = FSImageCompression.readCompressionHeader(conf, in);
        } else {
          compression = FSImageCompression.createNoopCompression();
        }
        in = compression.unwrapInputStream(fin);

        LOG.info("Loading image file " + curFile + " using " + compression);
        
        // load all inodes
        LOG.info("Number of files = " + numFiles);
        prog.setTotal(Phase.LOADING_FSIMAGE, step, numFiles);
        Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
        if (LayoutVersion.supports(Feature.FSIMAGE_NAME_OPTIMIZATION,
            imgVersion)) {
          if (supportSnapshot) {
            loadLocalNameINodesWithSnapshot(numFiles, in, counter);
          } else {
            loadLocalNameINodes(numFiles, in, counter);
          }
        } else {
          loadFullNameINodes(numFiles, in, counter);
        }

        loadFilesUnderConstruction(in, supportSnapshot, counter);
        prog.endStep(Phase.LOADING_FSIMAGE, step);
        // Now that the step is finished, set counter equal to total to adjust
        // for possible under-counting due to reference inodes.
        prog.setCount(Phase.LOADING_FSIMAGE, step, numFiles);

        loadSecretManagerState(in);

        // make sure to read to the end of file
        boolean eof = (in.read() == -1);
        assert eof : "Should have reached the end of image file " + curFile;
      } finally {
        in.close();
      }

      imgDigest = new MD5Hash(digester.digest());
      loaded = true;
      
      LOG.info("Image file " + curFile + " of size " + curFile.length() +
          " bytes loaded in " + (now() - startTime)/1000 + " seconds.");
    }

  /** Update the root node's attributes */
  private void updateRootAttr(INodeWithAdditionalFields root) {                                                           
    long nsQuota = root.getNsQuota();
    long dsQuota = root.getDsQuota();
    FSDirectory fsDir = namesystem.dir;
    if (nsQuota != -1 || dsQuota != -1) {
      fsDir.rootDir.setQuota(nsQuota, dsQuota);
    }
    fsDir.rootDir.cloneModificationTime(root);
    fsDir.rootDir.clonePermissionStatus(root);    
  }
  
    /**
     * Load fsimage files when 1) only local names are stored, 
     * and 2) snapshot is supported.
     * 
     * @param numFiles number of files expected to be read
     * @param in Image input stream
     * @param counter Counter to increment for namenode startup progress
     */
    private void loadLocalNameINodesWithSnapshot(long numFiles, DataInput in,
        Counter counter) throws IOException {
      assert LayoutVersion.supports(Feature.FSIMAGE_NAME_OPTIMIZATION,
          getLayoutVersion());
      assert LayoutVersion.supports(Feature.SNAPSHOT, getLayoutVersion());
      
      // load root
      loadRoot(in, counter);
      // load rest of the nodes recursively
      loadDirectoryWithSnapshot(in, counter);
    }
    
  /** 
   * load fsimage files assuming only local names are stored
   *   
   * @param numFiles number of files expected to be read
   * @param in image input stream
   * @param counter Counter to increment for namenode startup progress
   * @throws IOException
   */  
   private void loadLocalNameINodes(long numFiles, DataInput in, Counter counter)
       throws IOException {
     assert LayoutVersion.supports(Feature.FSIMAGE_NAME_OPTIMIZATION,
         getLayoutVersion());
     assert numFiles > 0;

     // load root
     loadRoot(in, counter);
     // have loaded the first file (the root)
     numFiles--; 

     // load rest of the nodes directory by directory
     while (numFiles > 0) {
       numFiles -= loadDirectory(in, counter);
     }
     if (numFiles != 0) {
       throw new IOException("Read unexpect number of files: " + -numFiles);
     }
   }
   
    /**
     * Load information about root, and use the information to update the root
     * directory of NameSystem.
     * @param in The {@link DataInput} instance to read.
     * @param counter Counter to increment for namenode startup progress
     */
    private void loadRoot(DataInput in, Counter counter)
        throws IOException {
      // load root
      if (in.readShort() != 0) {
        throw new IOException("First node is not root");
      }
      final INodeDirectory root = loadINode(null, false, in, counter)
        .asDirectory();
      // update the root's attributes
      updateRootAttr(root);
    }
   
    /** Load children nodes for the parent directory. */
    private int loadChildren(INodeDirectory parent, DataInput in,
        Counter counter) throws IOException {
      int numChildren = in.readInt();
      for (int i = 0; i < numChildren; i++) {
        // load single inode
        INode newNode = loadINodeWithLocalName(false, in, true, counter);
        addToParent(parent, newNode);
      }
      return numChildren;
    }
    
    /**
     * Load a directory when snapshot is supported.
     * @param in The {@link DataInput} instance to read.
     * @param counter Counter to increment for namenode startup progress
     */
    private void loadDirectoryWithSnapshot(DataInput in, Counter counter)
        throws IOException {
      // Step 1. Identify the parent INode
      long inodeId = in.readLong();
      final INodeDirectory parent = this.namesystem.dir.getInode(inodeId)
          .asDirectory();
      
      // Check if the whole subtree has been saved (for reference nodes)
      boolean toLoadSubtree = referenceMap.toProcessSubtree(parent.getId());
      if (!toLoadSubtree) {
        return;
      }
      
      // Step 2. Load snapshots if parent is snapshottable
      int numSnapshots = in.readInt();
      if (numSnapshots >= 0) {
        final INodeDirectorySnapshottable snapshottableParent
            = INodeDirectorySnapshottable.valueOf(parent, parent.getLocalName());
        // load snapshots and snapshotQuota
        SnapshotFSImageFormat.loadSnapshotList(snapshottableParent,
            numSnapshots, in, this);
        if (snapshottableParent.getSnapshotQuota() > 0) {
          // add the directory to the snapshottable directory list in 
          // SnapshotManager. Note that we only add root when its snapshot quota
          // is positive.
          this.namesystem.getSnapshotManager().addSnapshottable(
              snapshottableParent);
        }
      }

      // Step 3. Load children nodes under parent
      loadChildren(parent, in, counter);
      
      // Step 4. load Directory Diff List
      SnapshotFSImageFormat.loadDirectoryDiffList(parent, in, this);
      
      // Recursively load sub-directories, including snapshot copies of deleted
      // directories
      int numSubTree = in.readInt();
      for (int i = 0; i < numSubTree; i++) {
        loadDirectoryWithSnapshot(in, counter);
      }
    }
    
   /**
    * Load all children of a directory
    * 
    * @param in
    * @param counter Counter to increment for namenode startup progress
    * @return number of child inodes read
    * @throws IOException
    */
   private int loadDirectory(DataInput in, Counter counter) throws IOException {
     String parentPath = FSImageSerialization.readString(in);
     final INodeDirectory parent = INodeDirectory.valueOf(
         namesystem.dir.rootDir.getNode(parentPath, true), parentPath);
     return loadChildren(parent, in, counter);
   }

  /**
   * load fsimage files assuming full path names are stored
   * 
   * @param numFiles total number of files to load
   * @param in data input stream
   * @param counter Counter to increment for namenode startup progress
   * @throws IOException if any error occurs
   */
  private void loadFullNameINodes(long numFiles, DataInput in, Counter counter)
      throws IOException {
    byte[][] pathComponents;
    byte[][] parentPath = {{}};      
    FSDirectory fsDir = namesystem.dir;
    INodeDirectory parentINode = fsDir.rootDir;
    for (long i = 0; i < numFiles; i++) {
      pathComponents = FSImageSerialization.readPathComponents(in);
      final INode newNode = loadINode(
          pathComponents[pathComponents.length-1], false, in, counter);

      if (isRoot(pathComponents)) { // it is the root
        // update the root's attributes
        updateRootAttr(newNode.asDirectory());
        continue;
      }
      // check if the new inode belongs to the same parent
      if(!isParent(pathComponents, parentPath)) {
        parentINode = getParentINodeDirectory(pathComponents);
        parentPath = getParent(pathComponents);
      }

      // add new inode
      addToParent(parentINode, newNode);
    }
  }

  private INodeDirectory getParentINodeDirectory(byte[][] pathComponents
      ) throws FileNotFoundException, PathIsNotDirectoryException,
      UnresolvedLinkException {
    if (pathComponents.length < 2) { // root
      return null;
    }
    // Gets the parent INode
    final INodesInPath inodes = namesystem.dir.getExistingPathINodes(
        pathComponents);
    return INodeDirectory.valueOf(inodes.getINode(-2), pathComponents);
  }

  /**
   * Add the child node to parent and, if child is a file, update block map.
   * This method is only used for image loading so that synchronization,
   * modification time update and space count update are not needed.
   */
  private void addToParent(INodeDirectory parent, INode child) {
    FSDirectory fsDir = namesystem.dir;
    if (parent == fsDir.rootDir && FSDirectory.isReservedName(child)) {
        throw new HadoopIllegalArgumentException("File name \""
            + child.getLocalName() + "\" is reserved. Please "
            + " change the name of the existing file or directory to another "
            + "name before upgrading to this release.");
    }
    // NOTE: This does not update space counts for parents
    if (!parent.addChild(child)) {
      return;
    }
    namesystem.dir.cacheName(child);

    if (child.isFile()) {
      // Add file->block mapping
      final INodeFile file = child.asFile();
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        final BlockManager bm = namesystem.getBlockManager();
        for (int i = 0; i < blocks.length; i++) {
          file.setBlock(i, bm.addBlockCollection(blocks[i], file));
        } 
      }
    }
  }

    /** @return The FSDirectory of the namesystem where the fsimage is loaded */
    public FSDirectory getFSDirectoryInLoading() {
      return namesystem.dir;
    }

    public INode loadINodeWithLocalName(boolean isSnapshotINode, DataInput in,
        boolean updateINodeMap) throws IOException {
      return loadINodeWithLocalName(isSnapshotINode, in, updateINodeMap, null);
    }

    public INode loadINodeWithLocalName(boolean isSnapshotINode,
        DataInput in, boolean updateINodeMap, Counter counter)
        throws IOException {
      final byte[] localName = FSImageSerialization.readLocalName(in);
      INode inode = loadINode(localName, isSnapshotINode, in, counter);
      if (updateINodeMap
          && LayoutVersion.supports(Feature.ADD_INODE_ID, getLayoutVersion())) {
        namesystem.dir.addToInodeMap(inode);
      }
      return inode;
    }
  
  /**
   * load an inode from fsimage except for its name
   * 
   * @param in data input stream from which image is read
   * @param counter Counter to increment for namenode startup progress
   * @return an inode
   */
  INode loadINode(final byte[] localName, boolean isSnapshotINode,
      DataInput in, Counter counter) throws IOException {
    final int imgVersion = getLayoutVersion();
    if (LayoutVersion.supports(Feature.SNAPSHOT, imgVersion)) {
      namesystem.getFSDirectory().verifyINodeName(localName);
    }

    long inodeId = LayoutVersion.supports(Feature.ADD_INODE_ID, imgVersion) ? 
           in.readLong() : namesystem.allocateNewInodeId();
    
    final short replication = namesystem.getBlockManager().adjustReplication(
        in.readShort());
    final long modificationTime = in.readLong();
    long atime = 0;
    if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, imgVersion)) {
      atime = in.readLong();
    }
    final long blockSize = in.readLong();
    final int numBlocks = in.readInt();

    if (numBlocks >= 0) {
      // file
      
      // read blocks
      BlockInfo[] blocks = null;
      if (numBlocks >= 0) {
        blocks = new BlockInfo[numBlocks];
        for (int j = 0; j < numBlocks; j++) {
          blocks[j] = new BlockInfo(replication);
          blocks[j].readFields(in);
        }
      }

      String clientName = "";
      String clientMachine = "";
      boolean underConstruction = false;
      FileDiffList fileDiffs = null;
      if (LayoutVersion.supports(Feature.SNAPSHOT, imgVersion)) {
        // read diffs
        fileDiffs = SnapshotFSImageFormat.loadFileDiffList(in, this);

        if (isSnapshotINode) {
          underConstruction = in.readBoolean();
          if (underConstruction) {
            clientName = FSImageSerialization.readString(in);
            clientMachine = FSImageSerialization.readString(in);
          }
        }
      }

      final PermissionStatus permissions = PermissionStatus.read(in);

      // return
      if (counter != null) {
        counter.increment();
      }
      final INodeFile file = new INodeFile(inodeId, localName, permissions,
          modificationTime, atime, blocks, replication, blockSize);
      return fileDiffs != null? new INodeFileWithSnapshot(file, fileDiffs)
          : underConstruction? new INodeFileUnderConstruction(
              file, clientName, clientMachine, null)
          : file;
    } else if (numBlocks == -1) {
      //directory
      
      //read quotas
      final long nsQuota = in.readLong();
      long dsQuota = -1L;
      if (LayoutVersion.supports(Feature.DISKSPACE_QUOTA, imgVersion)) {
        dsQuota = in.readLong();
      }

      //read snapshot info
      boolean snapshottable = false;
      boolean withSnapshot = false;
      if (LayoutVersion.supports(Feature.SNAPSHOT, imgVersion)) {
        snapshottable = in.readBoolean();
        if (!snapshottable) {
          withSnapshot = in.readBoolean();
        }
      }

      final PermissionStatus permissions = PermissionStatus.read(in);

      //return
      if (counter != null) {
        counter.increment();
      }
      final INodeDirectory dir = nsQuota >= 0 || dsQuota >= 0?
          new INodeDirectoryWithQuota(inodeId, localName, permissions,
              modificationTime, nsQuota, dsQuota)
          : new INodeDirectory(inodeId, localName, permissions, modificationTime);
      return snapshottable ? new INodeDirectorySnapshottable(dir)
          : withSnapshot ? new INodeDirectoryWithSnapshot(dir)
          : dir;
    } else if (numBlocks == -2) {
      //symlink

      final String symlink = Text.readString(in);
      final PermissionStatus permissions = PermissionStatus.read(in);
      if (counter != null) {
        counter.increment();
      }
      return new INodeSymlink(inodeId, localName, permissions,
          modificationTime, atime, symlink);
    } else if (numBlocks == -3) {
      //reference
      // Intentionally do not increment counter, because it is too difficult at
      // this point to assess whether or not this is a reference that counts
      // toward quota.
      
      final boolean isWithName = in.readBoolean();
      // lastSnapshotId for WithName node, dstSnapshotId for DstReference node
      int snapshotId = in.readInt();
      
      final INodeReference.WithCount withCount
          = referenceMap.loadINodeReferenceWithCount(isSnapshotINode, in, this);

      if (isWithName) {
          return new INodeReference.WithName(null, withCount, localName,
              snapshotId);
      } else {
        final INodeReference ref = new INodeReference.DstReference(null,
            withCount, snapshotId);
        return ref;
      }
    }
    
    throw new IOException("Unknown inode type: numBlocks=" + numBlocks);
  }

    /** Load {@link INodeFileAttributes}. */
    public INodeFileAttributes loadINodeFileAttributes(DataInput in)
        throws IOException {
      final int layoutVersion = getLayoutVersion();
      
      if (!LayoutVersion.supports(Feature.OPTIMIZE_SNAPSHOT_INODES, layoutVersion)) {
        return loadINodeWithLocalName(true, in, false).asFile();
      }
  
      final byte[] name = FSImageSerialization.readLocalName(in);
      final PermissionStatus permissions = PermissionStatus.read(in);
      final long modificationTime = in.readLong();
      final long accessTime = in.readLong();
  
      final short replication = namesystem.getBlockManager().adjustReplication(
          in.readShort());
      final long preferredBlockSize = in.readLong();
      
      return new INodeFileAttributes.SnapshotCopy(name, permissions, modificationTime,
          accessTime, replication, preferredBlockSize);
    }

    public INodeDirectoryAttributes loadINodeDirectoryAttributes(DataInput in)
        throws IOException {
      final int layoutVersion = getLayoutVersion();
      
      if (!LayoutVersion.supports(Feature.OPTIMIZE_SNAPSHOT_INODES, layoutVersion)) {
        return loadINodeWithLocalName(true, in, false).asDirectory();
      }
  
      final byte[] name = FSImageSerialization.readLocalName(in);
      final PermissionStatus permissions = PermissionStatus.read(in);
      final long modificationTime = in.readLong();
      
      //read quotas
      final long nsQuota = in.readLong();
      final long dsQuota = in.readLong();
  
      return nsQuota == -1L && dsQuota == -1L?
          new INodeDirectoryAttributes.SnapshotCopy(name, permissions, modificationTime)
        : new INodeDirectoryAttributes.CopyWithQuota(name, permissions,
            modificationTime, nsQuota, dsQuota);
    }
  
    private void loadFilesUnderConstruction(DataInput in,
        boolean supportSnapshot, Counter counter) throws IOException {
      FSDirectory fsDir = namesystem.dir;
      int size = in.readInt();

      LOG.info("Number of files under construction = " + size);

      for (int i = 0; i < size; i++) {
        INodeFileUnderConstruction cons = FSImageSerialization
            .readINodeUnderConstruction(in, namesystem, getLayoutVersion());
        counter.increment();

        // verify that file exists in namespace
        String path = cons.getLocalName();
        final INodesInPath iip = fsDir.getLastINodeInPath(path);
        INodeFile oldnode = INodeFile.valueOf(iip.getINode(0), path);
        cons.setLocalName(oldnode.getLocalNameBytes());
        cons.setParent(oldnode.getParent());

        if (oldnode instanceof INodeFileWithSnapshot) {
          cons = new INodeFileUnderConstructionWithSnapshot(cons,
              ((INodeFileWithSnapshot)oldnode).getDiffs());
        }

        fsDir.replaceINodeFile(path, oldnode, cons);
        namesystem.leaseManager.addLease(cons.getClientName(), path); 
      }
    }

    private void loadSecretManagerState(DataInput in)
        throws IOException {
      int imgVersion = getLayoutVersion();

      if (!LayoutVersion.supports(Feature.DELEGATION_TOKEN, imgVersion)) {
        //SecretManagerState is not available.
        //This must not happen if security is turned on.
        return; 
      }
      namesystem.loadSecretManagerState(in);
    }

    private int getLayoutVersion() {
      return namesystem.getFSImage().getStorage().getLayoutVersion();
    }

    private boolean isRoot(byte[][] path) {
      return path.length == 1 &&
        path[0] == null;    
    }

    private boolean isParent(byte[][] path, byte[][] parent) {
      if (path == null || parent == null)
        return false;
      if (parent.length == 0 || path.length != parent.length + 1)
        return false;
      boolean isParent = true;
      for (int i = 0; i < parent.length; i++) {
        isParent = isParent && Arrays.equals(path[i], parent[i]); 
      }
      return isParent;
    }

    /**
     * Return string representing the parent of the given path.
     */
    String getParent(String path) {
      return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
    }
    
    byte[][] getParent(byte[][] path) {
      byte[][] result = new byte[path.length - 1][];
      for (int i = 0; i < result.length; i++) {
        result[i] = new byte[path[i].length];
        System.arraycopy(path[i], 0, result[i], 0, path[i].length);
      }
      return result;
    }
    
    public Snapshot getSnapshot(DataInput in) throws IOException {
      return snapshotMap.get(in.readInt());
    }
  }
  
  /**
   * A one-shot class responsible for writing an image file.
   * The write() function should be called once, after which the getter
   * functions may be used to retrieve information about the file that was written.
   */
  static class Saver {
    private final SaveNamespaceContext context;
    /** Set to true once an image has been written */
    private boolean saved = false;
    
    /** The MD5 checksum of the file that was written */
    private MD5Hash savedDigest;
    private final ReferenceMap referenceMap = new ReferenceMap();

    /** @throws IllegalStateException if the instance has not yet saved an image */
    private void checkSaved() {
      if (!saved) {
        throw new IllegalStateException("FSImageSaver has not saved an image");
      }
    }
    
    /** @throws IllegalStateException if the instance has already saved an image */
    private void checkNotSaved() {
      if (saved) {
        throw new IllegalStateException("FSImageSaver has already saved an image");
      }
    }
    

    Saver(SaveNamespaceContext context) {
      this.context = context;
    }

    /**
     * Return the MD5 checksum of the image file that was saved.
     */
    MD5Hash getSavedDigest() {
      checkSaved();
      return savedDigest;
    }

    void save(File newFile, FSImageCompression compression) throws IOException {
      checkNotSaved();

      final FSNamesystem sourceNamesystem = context.getSourceNamesystem();
      FSDirectory fsDir = sourceNamesystem.dir;
      String sdPath = newFile.getParentFile().getParentFile().getAbsolutePath();
      Step step = new Step(StepType.INODES, sdPath);
      StartupProgress prog = NameNode.getStartupProgress();
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      prog.setTotal(Phase.SAVING_CHECKPOINT, step,
        fsDir.rootDir.numItemsInTree());
      Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
      long startTime = now();
      //
      // Write out data
      //
      MessageDigest digester = MD5Hash.getDigester();
      FileOutputStream fout = new FileOutputStream(newFile);
      DigestOutputStream fos = new DigestOutputStream(fout, digester);
      DataOutputStream out = new DataOutputStream(fos);
      try {
        out.writeInt(HdfsConstants.LAYOUT_VERSION);
        // We use the non-locked version of getNamespaceInfo here since
        // the coordinating thread of saveNamespace already has read-locked
        // the namespace for us. If we attempt to take another readlock
        // from the actual saver thread, there's a potential of a
        // fairness-related deadlock. See the comments on HDFS-2223.
        out.writeInt(sourceNamesystem.unprotectedGetNamespaceInfo()
            .getNamespaceID());
        out.writeLong(fsDir.rootDir.numItemsInTree());
        out.writeLong(sourceNamesystem.getGenerationStampV1());
        out.writeLong(sourceNamesystem.getGenerationStampV2());
        out.writeLong(sourceNamesystem.getGenerationStampAtblockIdSwitch());
        out.writeLong(sourceNamesystem.getLastAllocatedBlockId());
        out.writeLong(context.getTxId());
        out.writeLong(sourceNamesystem.getLastInodeId());

        
        sourceNamesystem.getSnapshotManager().write(out);
        
        // write compression info and set up compressed stream
        out = compression.writeHeaderAndWrapStream(fos);
        LOG.info("Saving image file " + newFile +
                 " using " + compression);

        // save the root
        saveINode2Image(fsDir.rootDir, out, false, referenceMap, counter);
        // save the rest of the nodes
        saveImage(fsDir.rootDir, out, true, counter);
        prog.endStep(Phase.SAVING_CHECKPOINT, step);
        // Now that the step is finished, set counter equal to total to adjust
        // for possible under-counting due to reference inodes.
        prog.setCount(Phase.SAVING_CHECKPOINT, step,
          fsDir.rootDir.numItemsInTree());
        // save files under construction
        sourceNamesystem.saveFilesUnderConstruction(out);
        context.checkCancelled();
        sourceNamesystem.saveSecretManagerState(out, sdPath);
        context.checkCancelled();
        out.flush();
        context.checkCancelled();
        fout.getChannel().force(true);
      } finally {
        out.close();
      }

      saved = true;
      // set md5 of the saved image
      savedDigest = new MD5Hash(digester.digest());

      LOG.info("Image file " + newFile + " of size " + newFile.length() +
          " bytes saved in " + (now() - startTime)/1000 + " seconds.");
    }

    /**
     * Save children INodes.
     * @param children The list of children INodes
     * @param out The DataOutputStream to write
     * @param counter Counter to increment for namenode startup progress
     * @return Number of children that are directory
     */
    private int saveChildren(ReadOnlyList<INode> children, DataOutputStream out,
        Counter counter) throws IOException {
      // Write normal children INode. 
      out.writeInt(children.size());
      int dirNum = 0;
      int i = 0;
      for(INode child : children) {
        // print all children first
        saveINode2Image(child, out, false, referenceMap, counter);
        if (child.isDirectory()) {
          dirNum++;
        }
        if (i++ % 50 == 0) {
          context.checkCancelled();
        }
      }
      return dirNum;
    }
    
    /**
     * Save file tree image starting from the given root.
     * This is a recursive procedure, which first saves all children and 
     * snapshot diffs of a current directory and then moves inside the 
     * sub-directories.
     * 
     * @param current The current node
     * @param out The DataoutputStream to write the image
     * @param snapshot The possible snapshot associated with the current node
     * @param toSaveSubtree Whether or not to save the subtree to fsimage. For
     *                      reference node, its subtree may already have been
     *                      saved before.
     * @param counter Counter to increment for namenode startup progress
     */
    private void saveImage(INodeDirectory current, DataOutputStream out,
        boolean toSaveSubtree, Counter counter) throws IOException {
      // write the inode id of the directory
      out.writeLong(current.getId());
      
      if (!toSaveSubtree) {
        return;
      }
      
      final ReadOnlyList<INode> children = current.getChildrenList(null);
      int dirNum = 0;
      List<INodeDirectory> snapshotDirs = null;
      if (current instanceof INodeDirectoryWithSnapshot) {
        snapshotDirs = new ArrayList<INodeDirectory>();
        ((INodeDirectoryWithSnapshot) current).getSnapshotDirectory(
            snapshotDirs);
        dirNum += snapshotDirs.size();
      }
      
      // 2. Write INodeDirectorySnapshottable#snapshotsByNames to record all
      // Snapshots
      if (current instanceof INodeDirectorySnapshottable) {
        INodeDirectorySnapshottable snapshottableNode = 
            (INodeDirectorySnapshottable) current;
        SnapshotFSImageFormat.saveSnapshots(snapshottableNode, out);
      } else {
        out.writeInt(-1); // # of snapshots
      }

      // 3. Write children INode 
      dirNum += saveChildren(children, out, counter);
      
      // 4. Write DirectoryDiff lists, if there is any.
      SnapshotFSImageFormat.saveDirectoryDiffList(current, out, referenceMap);
      
      // Write sub-tree of sub-directories, including possible snapshots of 
      // deleted sub-directories
      out.writeInt(dirNum); // the number of sub-directories
      for(INode child : children) {
        if(!child.isDirectory()) {
          continue;
        }
        // make sure we only save the subtree under a reference node once
        boolean toSave = child.isReference() ? 
            referenceMap.toProcessSubtree(child.getId()) : true;
        saveImage(child.asDirectory(), out, toSave, counter);
      }
      if (snapshotDirs != null) {
        for (INodeDirectory subDir : snapshotDirs) {
          // make sure we only save the subtree under a reference node once
          boolean toSave = subDir.getParentReference() != null ? 
              referenceMap.toProcessSubtree(subDir.getId()) : true;
          saveImage(subDir, out, toSave, counter);
        }
      }
    }

    /**
     * Saves inode and increments progress counter.
     * 
     * @param inode INode to save
     * @param out DataOutputStream to receive inode
     * @param writeUnderConstruction boolean true if this is under construction
     * @param referenceMap ReferenceMap containing reference inodes
     * @param counter Counter to increment for namenode startup progress
     * @throws IOException thrown if there is an I/O error
     */
    private void saveINode2Image(INode inode, DataOutputStream out,
        boolean writeUnderConstruction, ReferenceMap referenceMap,
        Counter counter) throws IOException {
      FSImageSerialization.saveINode2Image(inode, out, writeUnderConstruction,
        referenceMap);
      // Intentionally do not increment counter for reference inodes, because it
      // is too difficult at this point to assess whether or not this is a
      // reference that counts toward quota.
      if (!(inode instanceof INodeReference)) {
        counter.increment();
      }
    }
  }
}

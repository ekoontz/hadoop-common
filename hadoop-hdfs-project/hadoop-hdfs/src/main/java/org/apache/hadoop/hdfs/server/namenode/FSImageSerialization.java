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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

/**
 * Static utility functions for serializing various pieces of data in the correct
 * format for the FSImage file.
 *
 * Some members are currently public for the benefit of the Offline Image Viewer
 * which is located outside of this package. These members should be made
 * package-protected when the OIV is refactored.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageSerialization {

  // Static-only class
  private FSImageSerialization() {}
  
  /**
   * In order to reduce allocation, we reuse some static objects. However, the methods
   * in this class should be thread-safe since image-saving is multithreaded, so 
   * we need to keep the static objects in a thread-local.
   */
  static private final ThreadLocal<TLData> TL_DATA =
    new ThreadLocal<TLData>() {
    @Override
    protected TLData initialValue() {
      return new TLData();
    }
  };

  /**
   * Simple container "struct" for threadlocal data.
   */
  static private final class TLData {
    final DeprecatedUTF8 U_STR = new DeprecatedUTF8();
    final ShortWritable U_SHORT = new ShortWritable();
    final IntWritable U_INT = new IntWritable();
    final LongWritable U_LONG = new LongWritable();
    final FsPermission FILE_PERM = new FsPermission((short) 0);
  }

  private static void writePermissionStatus(INodeAttributes inode,
      DataOutput out) throws IOException {
    final FsPermission p = TL_DATA.get().FILE_PERM;
    p.fromShort(inode.getFsPermissionShort());
    PermissionStatus.write(out, inode.getUserName(), inode.getGroupName(), p);
  }

  private static void writeBlocks(final Block[] blocks,
      final DataOutput out) throws IOException {
    if (blocks == null) {
      out.writeInt(0);
    } else {
      out.writeInt(blocks.length);
      for (Block blk : blocks) {
        blk.write(out);
      }
    }
  }

  // Helper function that reads in an INodeUnderConstruction
  // from the input stream
  //
  static INodeFileUnderConstruction readINodeUnderConstruction(
      DataInput in, FSNamesystem fsNamesys, int imgVersion)
      throws IOException {
    byte[] name = readBytes(in);
    long inodeId = LayoutVersion.supports(Feature.ADD_INODE_ID, imgVersion) ? in
        .readLong() : fsNamesys.allocateNewInodeId();
    short blockReplication = in.readShort();
    long modificationTime = in.readLong();
    long preferredBlockSize = in.readLong();
  
    int numBlocks = in.readInt();
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    Block blk = new Block();
    int i = 0;
    for (; i < numBlocks-1; i++) {
      blk.readFields(in);
      blocks[i] = new BlockInfo(blk, blockReplication);
    }
    // last block is UNDER_CONSTRUCTION
    if(numBlocks > 0) {
      blk.readFields(in);
      blocks[i] = new BlockInfoUnderConstruction(
        blk, blockReplication, BlockUCState.UNDER_CONSTRUCTION, null);
    }
    PermissionStatus perm = PermissionStatus.read(in);
    String clientName = readString(in);
    String clientMachine = readString(in);

    // We previously stored locations for the last block, now we
    // just record that there are none
    int numLocs = in.readInt();
    assert numLocs == 0 : "Unexpected block locations";

    return new INodeFileUnderConstruction(inodeId,
                                          name,
                                          blockReplication, 
                                          modificationTime,
                                          preferredBlockSize,
                                          blocks,
                                          perm,
                                          clientName,
                                          clientMachine,
                                          null);
  }

  // Helper function that writes an INodeUnderConstruction
  // into the input stream
  //
  static void writeINodeUnderConstruction(DataOutputStream out,
                                           INodeFileUnderConstruction cons,
                                           String path) 
                                           throws IOException {
    writeString(path, out);
    out.writeLong(cons.getId());
    out.writeShort(cons.getFileReplication());
    out.writeLong(cons.getModificationTime());
    out.writeLong(cons.getPreferredBlockSize());

    writeBlocks(cons.getBlocks(), out);
    cons.getPermissionStatus().write(out);

    writeString(cons.getClientName(), out);
    writeString(cons.getClientMachine(), out);

    out.writeInt(0); //  do not store locations of last block
  }

  /**
   * Serialize a {@link INodeFile} node
   * @param node The node to write
   * @param out The {@link DataOutputStream} where the fields are written
   * @param writeBlock Whether to write block information
   */
  public static void writeINodeFile(INodeFile file, DataOutput out,
      boolean writeUnderConstruction) throws IOException {
    writeLocalName(file, out);
    out.writeLong(file.getId());
    out.writeShort(file.getFileReplication());
    out.writeLong(file.getModificationTime());
    out.writeLong(file.getAccessTime());
    out.writeLong(file.getPreferredBlockSize());

    writeBlocks(file.getBlocks(), out);
    SnapshotFSImageFormat.saveFileDiffList(file, out);

    if (writeUnderConstruction) {
      if (file instanceof INodeFileUnderConstruction) {
        out.writeBoolean(true);
        final INodeFileUnderConstruction uc = (INodeFileUnderConstruction)file;
        writeString(uc.getClientName(), out);
        writeString(uc.getClientMachine(), out);
      } else {
        out.writeBoolean(false);
      }
    }

    writePermissionStatus(file, out);
  }

  /** Serialize an {@link INodeFileAttributes}. */
  public static void writeINodeFileAttributes(INodeFileAttributes file,
      DataOutput out) throws IOException {
    writeLocalName(file, out);
    writePermissionStatus(file, out);
    out.writeLong(file.getModificationTime());
    out.writeLong(file.getAccessTime());

    out.writeShort(file.getFileReplication());
    out.writeLong(file.getPreferredBlockSize());
  }

  /**
   * Serialize a {@link INodeDirectory}
   * @param node The node to write
   * @param out The {@link DataOutput} where the fields are written 
   */
  public static void writeINodeDirectory(INodeDirectory node, DataOutput out)
      throws IOException {
    writeLocalName(node, out);
    out.writeLong(node.getId());
    out.writeShort(0);  // replication
    out.writeLong(node.getModificationTime());
    out.writeLong(0);   // access time
    out.writeLong(0);   // preferred block size
    out.writeInt(-1);   // # of blocks

    out.writeLong(node.getNsQuota());
    out.writeLong(node.getDsQuota());
    if (node instanceof INodeDirectorySnapshottable) {
      out.writeBoolean(true);
    } else {
      out.writeBoolean(false);
      out.writeBoolean(node instanceof INodeDirectoryWithSnapshot);
    }
    
    writePermissionStatus(node, out);
  }
  
  /**
   * Serialize a {@link INodeDirectory}
   * @param a The node to write
   * @param out The {@link DataOutput} where the fields are written 
   */
  public static void writeINodeDirectoryAttributes(
      INodeDirectoryAttributes a, DataOutput out) throws IOException {
    writeLocalName(a, out);
    writePermissionStatus(a, out);
    out.writeLong(a.getModificationTime());

    out.writeLong(a.getNsQuota());
    out.writeLong(a.getDsQuota());
  }

  /**
   * Serialize a {@link INodeSymlink} node
   * @param node The node to write
   * @param out The {@link DataOutput} where the fields are written
   */
  private static void writeINodeSymlink(INodeSymlink node, DataOutput out)
      throws IOException {
    writeLocalName(node, out);
    out.writeLong(node.getId());
    out.writeShort(0);  // replication
    out.writeLong(0);   // modification time
    out.writeLong(0);   // access time
    out.writeLong(0);   // preferred block size
    out.writeInt(-2);   // # of blocks

    Text.writeString(out, node.getSymlinkString());
    writePermissionStatus(node, out);
  }
  
  /** Serialize a {@link INodeReference} node */
  private static void writeINodeReference(INodeReference ref, DataOutput out,
      boolean writeUnderConstruction, ReferenceMap referenceMap
      ) throws IOException {
    writeLocalName(ref, out);
    out.writeLong(ref.getId());
    out.writeShort(0);  // replication
    out.writeLong(0);   // modification time
    out.writeLong(0);   // access time
    out.writeLong(0);   // preferred block size
    out.writeInt(-3);   // # of blocks

    final boolean isWithName = ref instanceof INodeReference.WithName;
    out.writeBoolean(isWithName);
    
    if (!isWithName) {
      Preconditions.checkState(ref instanceof INodeReference.DstReference);
      // dst snapshot id
      out.writeInt(((INodeReference.DstReference) ref).getDstSnapshotId());
    } else {
      out.writeInt(((INodeReference.WithName) ref).getLastSnapshotId());
    }
    
    final INodeReference.WithCount withCount
        = (INodeReference.WithCount)ref.getReferredINode();
    referenceMap.writeINodeReferenceWithCount(withCount, out,
        writeUnderConstruction);
  }

  /**
   * Save one inode's attributes to the image.
   */
  public static void saveINode2Image(INode node, DataOutput out,
      boolean writeUnderConstruction, ReferenceMap referenceMap)
      throws IOException {
    if (node.isReference()) {
      writeINodeReference(node.asReference(), out, writeUnderConstruction,
          referenceMap);
    } else if (node.isDirectory()) {
      writeINodeDirectory(node.asDirectory(), out);
    } else if (node.isSymlink()) {
      writeINodeSymlink(node.asSymlink(), out);      
    } else if (node.isFile()) {
      writeINodeFile(node.asFile(), out, writeUnderConstruction);
    }
  }

  // This should be reverted to package private once the ImageLoader
  // code is moved into this package. This method should not be called
  // by other code.
  @SuppressWarnings("deprecation")
  public static String readString(DataInput in) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    return ustr.toStringChecked();
  }

  static String readString_EmptyAsNull(DataInput in) throws IOException {
    final String s = readString(in);
    return s.isEmpty()? null: s;
  }

  @SuppressWarnings("deprecation")
  public static void writeString(String str, DataOutput out) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.set(str);
    ustr.write(out);
  }

  
  /** read the long value */
  static long readLong(DataInput in) throws IOException {
    LongWritable uLong = TL_DATA.get().U_LONG;
    uLong.readFields(in);
    return uLong.get();
  }

  /** write the long value */
  static void writeLong(long value, DataOutputStream out) throws IOException {
    LongWritable uLong = TL_DATA.get().U_LONG;
    uLong.set(value);
    uLong.write(out);
  }
  
  /** read the int value */
  static int readInt(DataInput in) throws IOException {
    IntWritable uInt = TL_DATA.get().U_INT;
    uInt.readFields(in);
    return uInt.get();
  }
  
  /** write the int value */
  static void writeInt(int value, DataOutputStream out) throws IOException {
    IntWritable uInt = TL_DATA.get().U_INT;
    uInt.set(value);
    uInt.write(out);
  }

  /** read short value */
  static short readShort(DataInput in) throws IOException {
    ShortWritable uShort = TL_DATA.get().U_SHORT;
    uShort.readFields(in);
    return uShort.get();
  }

  /** write short value */
  static void writeShort(short value, DataOutputStream out) throws IOException {
    ShortWritable uShort = TL_DATA.get().U_SHORT;
    uShort.set(value);
    uShort.write(out);
  }
  
  // Same comments apply for this method as for readString()
  @SuppressWarnings("deprecation")
  public static byte[] readBytes(DataInput in) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    int len = ustr.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(ustr.getBytes(), 0, bytes, 0, len);
    return bytes;
  }

  /**
   * Reading the path from the image and converting it to byte[][] directly
   * this saves us an array copy and conversions to and from String
   * @param in
   * @return the array each element of which is a byte[] representation 
   *            of a path component
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public static byte[][] readPathComponents(DataInput in)
      throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    
    ustr.readFields(in);
    return DFSUtil.bytes2byteArray(ustr.getBytes(),
      ustr.getLength(), (byte) Path.SEPARATOR_CHAR);
  }
  
  public static byte[] readLocalName(DataInput in) throws IOException {
    byte[] createdNodeName = new byte[in.readShort()];
    in.readFully(createdNodeName);
    return createdNodeName;
  }

  private static void writeLocalName(INodeAttributes inode, DataOutput out)
      throws IOException {
    final byte[] name = inode.getLocalNameBytes();
    writeBytes(name, out);
  }
  
  public static void writeBytes(byte[] data, DataOutput out)
      throws IOException {
    out.writeShort(data.length);
    out.write(data);
  }

  /**
   * Write an array of blocks as compactly as possible. This uses
   * delta-encoding for the generation stamp and size, following
   * the principle that genstamp increases relatively slowly,
   * and size is equal for all but the last block of a file.
   */
  public static void writeCompactBlockArray(
      Block[] blocks, DataOutputStream out) throws IOException {
    WritableUtils.writeVInt(out, blocks.length);
    Block prev = null;
    for (Block b : blocks) {
      long szDelta = b.getNumBytes() -
          (prev != null ? prev.getNumBytes() : 0);
      long gsDelta = b.getGenerationStamp() -
          (prev != null ? prev.getGenerationStamp() : 0);
      out.writeLong(b.getBlockId()); // blockid is random
      WritableUtils.writeVLong(out, szDelta);
      WritableUtils.writeVLong(out, gsDelta);
      prev = b;
    }
  }
  
  public static Block[] readCompactBlockArray(
      DataInput in, int logVersion) throws IOException {
    int num = WritableUtils.readVInt(in);
    if (num < 0) {
      throw new IOException("Invalid block array length: " + num);
    }
    Block prev = null;
    Block[] ret = new Block[num];
    for (int i = 0; i < num; i++) {
      long id = in.readLong();
      long sz = WritableUtils.readVLong(in) +
          ((prev != null) ? prev.getNumBytes() : 0);
      long gs = WritableUtils.readVLong(in) +
          ((prev != null) ? prev.getGenerationStamp() : 0);
      ret[i] = new Block(id, sz, gs);
      prev = ret[i];
    }
    return ret;
  }
}

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
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INodeFile.HeaderFormat;

/**
 * The attributes of a file.
 */
@InterfaceAudience.Private
public interface INodeFileAttributes extends INodeAttributes {
  /** @return the file replication. */
  public short getFileReplication();

  /** @return preferred block size in bytes */
  public long getPreferredBlockSize();
  
  /** @return the header as a long. */
  public long getHeaderLong();

  /** A copy of the inode file attributes */
  public static class SnapshotCopy extends INodeAttributes.SnapshotCopy
      implements INodeFileAttributes {
    private final long header;

    public SnapshotCopy(byte[] name, PermissionStatus permissions,
        long modificationTime, long accessTime,
        short replication, long preferredBlockSize) {
      super(name, permissions, modificationTime, accessTime);

      final long h = HeaderFormat.combineReplication(0L, replication);
      header = HeaderFormat.combinePreferredBlockSize(h, preferredBlockSize);
    }

    public SnapshotCopy(INodeFile file) {
      super(file);
      this.header = file.getHeaderLong();
    }

    @Override
    public short getFileReplication() {
      return HeaderFormat.getReplication(header);
    }

    @Override
    public long getPreferredBlockSize() {
      return HeaderFormat.getPreferredBlockSize(header);
    }

    @Override
    public long getHeaderLong() {
      return header;
    }
  }
}

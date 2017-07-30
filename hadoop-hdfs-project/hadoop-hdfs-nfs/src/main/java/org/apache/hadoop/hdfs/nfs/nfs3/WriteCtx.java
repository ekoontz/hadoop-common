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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.jboss.netty.channel.Channel;

/**
 * WriteCtx saves the context of one write request, such as request, channel,
 * xid and reply status.
 */
class WriteCtx {
  public static final Log LOG = LogFactory.getLog(WriteCtx.class);
  
  /**
   * In memory write data has 3 states. ALLOW_DUMP: not sequential write, still
   * wait for prerequisit writes. NO_DUMP: sequential write, no need to dump
   * since it will be written to HDFS soon. DUMPED: already dumped to a file.
   */
  public static enum DataState {
    ALLOW_DUMP,
    NO_DUMP,
    DUMPED;
  }

  private final FileHandle handle;
  private final long offset;
  private final int count;
  private final WriteStableHow stableHow;
  private byte[] data;
  
  private final Channel channel;
  private final int xid;
  private boolean replied;

  private DataState dataState;

  public DataState getDataState() {
    return dataState;
  }

  public void setDataState(DataState dataState) {
    this.dataState = dataState;
  }

  private RandomAccessFile raf;
  private long dumpFileOffset;
  
  // Return the dumped data size
  public long dumpData(FileOutputStream dumpOut, RandomAccessFile raf)
      throws IOException {
    if (dataState != DataState.ALLOW_DUMP) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No need to dump with status(replied,dataState):" + "("
            + replied + "," + dataState + ")");
      }
      return 0;
    }
    this.raf = raf;
    dumpFileOffset = dumpOut.getChannel().position();
    dumpOut.write(data, 0, count);
    if (LOG.isDebugEnabled()) {
      LOG.debug("After dump, new dumpFileOffset:" + dumpFileOffset);
    }
    data = null;
    dataState = DataState.DUMPED;
    return count;
  }

  public FileHandle getHandle() {
    return handle;
  }
  
  public long getOffset() {
    return offset;
  }

  public int getCount() {
    return count;
  }

  public WriteStableHow getStableHow() {
    return stableHow;
  }

  public byte[] getData() throws IOException {
    if (dataState != DataState.DUMPED) {
      if (data == null) {
        throw new IOException("Data is not dumpted but has null:" + this);
      }
    } else {
      // read back
      if (data != null) {
        throw new IOException("Data is dumpted but not null");
      }
      data = new byte[count];
      raf.seek(dumpFileOffset);
      int size = raf.read(data, 0, count);
      if (size != count) {
        throw new IOException("Data count is " + count + ", but read back "
            + size + "bytes");
      }
    }
    return data;
  }

  Channel getChannel() {
    return channel;
  }

  int getXid() {
    return xid;
  }

  boolean getReplied() {
    return replied;
  }
  
  void setReplied(boolean replied) {
    this.replied = replied;
  }
  
  WriteCtx(FileHandle handle, long offset, int count, WriteStableHow stableHow,
      byte[] data, Channel channel, int xid, boolean replied, DataState dataState) {
    this.handle = handle;
    this.offset = offset;
    this.count = count;
    this.stableHow = stableHow;
    this.data = data;
    this.channel = channel;
    this.xid = xid;
    this.replied = replied;
    this.dataState = dataState;
    raf = null;
  }
  
  @Override
  public String toString() {
    return "Id:" + handle.getFileId() + " offset:" + offset + " count:" + count
        + " stableHow:" + stableHow + " replied:" + replied + " dataState:"
        + dataState + " xid:" + xid;
  }
}
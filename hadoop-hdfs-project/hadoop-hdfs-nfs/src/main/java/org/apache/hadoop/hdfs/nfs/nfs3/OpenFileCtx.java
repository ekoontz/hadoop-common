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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.InvalidParameterException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.nfs.nfs3.WriteCtx.DataState;
import org.apache.hadoop.io.BytesWritable.Comparator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.util.Daemon;
import org.jboss.netty.channel.Channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * OpenFileCtx saves the context of one HDFS file output stream. Access to it is
 * synchronized by its member lock.
 */
class OpenFileCtx {
  public static final Log LOG = LogFactory.getLog(OpenFileCtx.class);
  
  // Pending writes water mark for dump, 1MB
  private static long DUMP_WRITE_WATER_MARK = 1024 * 1024;

  public final static int COMMIT_FINISHED = 0;
  public final static int COMMIT_WAIT = 1;
  public final static int COMMIT_INACTIVE_CTX = 2;
  public final static int COMMIT_INACTIVE_WITH_PENDING_WRITE = 3;
  public final static int COMMIT_ERROR = 4;

  // The stream status. False means the stream is closed.
  private volatile boolean activeState;
  // The stream write-back status. True means one thread is doing write back.
  private volatile boolean asyncStatus;

  /**
   * The current offset of the file in HDFS. All the content before this offset
   * has been written back to HDFS.
   */
  private AtomicLong nextOffset;
  private final HdfsDataOutputStream fos;
  
  // TODO: make it mutable and update it after each writing back to HDFS
  private final Nfs3FileAttributes latestAttr;

  private final ConcurrentNavigableMap<OffsetRange, WriteCtx> pendingWrites;
  
  // The last write, commit request or write-back event. Updating time to keep
  // output steam alive.
  private long lastAccessTime;
  
  private volatile boolean enabledDump;
  private FileOutputStream dumpOut;
  private AtomicLong nonSequentialWriteInMemory;
  private RandomAccessFile raf;
  private final String dumpFilePath;
  private Daemon dumpThread;
  
  private void updateLastAccessTime() {
    lastAccessTime = System.currentTimeMillis();
  }

  private boolean checkStreamTimeout(long streamTimeout) {
    return System.currentTimeMillis() - lastAccessTime > streamTimeout;
  }
  
  public long getNextOffset() {
    return nextOffset.get();
  }
  
  // Increase or decrease the memory occupation of non-sequential writes
  private long updateNonSequentialWriteInMemory(long count) {
    long newValue = nonSequentialWriteInMemory.addAndGet(count);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Update nonSequentialWriteInMemory by " + count + " new value:"
          + newValue);
    }

    Preconditions.checkState(newValue >= 0,
        "nonSequentialWriteInMemory is negative after update with count "
            + count);
    return newValue;
  }
  
  OpenFileCtx(HdfsDataOutputStream fos, Nfs3FileAttributes latestAttr,
      String dumpFilePath) {
    this.fos = fos;
    this.latestAttr = latestAttr;
    // We use the ReverseComparatorOnMin as the comparator of the map. In this
    // way, we first dump the data with larger offset. In the meanwhile, we
    // retrieve the last element to write back to HDFS.
    pendingWrites = new ConcurrentSkipListMap<OffsetRange, WriteCtx>(
        OffsetRange.ReverseComparatorOnMin);
    updateLastAccessTime();
    activeState = true;
    asyncStatus = false;
    dumpOut = null;
    raf = null;
    nonSequentialWriteInMemory = new AtomicLong(0);
  
    this.dumpFilePath = dumpFilePath;  
    enabledDump = dumpFilePath == null ? false: true;
    nextOffset = new AtomicLong();
    nextOffset.set(latestAttr.getSize());
    try {	
      assert(nextOffset.get() == this.fos.getPos());
    } catch (IOException e) {}
    dumpThread = null;
  }

  public Nfs3FileAttributes getLatestAttr() {
    return latestAttr;
  }
  
  // Get flushed offset. Note that flushed data may not be persisted.
  private long getFlushedOffset() throws IOException {
    return fos.getPos();
  }
  
  // Check if need to dump the new writes
  private void checkDump() {
    if (!enabledDump) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Do nothing, dump is disabled.");
      }
      return;
    }

    if (nonSequentialWriteInMemory.get() < DUMP_WRITE_WATER_MARK) {
      return;
    }

    // wake up the dumper thread to dump the data
    synchronized (this) {
      if (nonSequentialWriteInMemory.get() >= DUMP_WRITE_WATER_MARK) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Asking dumper to dump...");
        }
        if (dumpThread == null) {
          dumpThread = new Daemon(new Dumper());
          dumpThread.start();
        } else {
          this.notifyAll();          
        }
      }
    }
  }

  class Dumper implements Runnable {
    /** Dump data into a file */
    private void dump() {
      // Create dump outputstream for the first time
      if (dumpOut == null) {
        LOG.info("Create dump file:" + dumpFilePath);
        File dumpFile = new File(dumpFilePath);
        try {
          synchronized (this) {
            // check if alive again
            Preconditions.checkState(dumpFile.createNewFile(),
                "The dump file should not exist: %s", dumpFilePath);
            dumpOut = new FileOutputStream(dumpFile);
          }
        } catch (IOException e) {
          LOG.error("Got failure when creating dump stream " + dumpFilePath, e);
          enabledDump = false;
          if (dumpOut != null) {
            try {
              dumpOut.close();
            } catch (IOException e1) {
              LOG.error("Can't close dump stream " + dumpFilePath, e);
            }
          }
          return;
        }
      }

      // Get raf for the first dump
      if (raf == null) {
        try {
          raf = new RandomAccessFile(dumpFilePath, "r");
        } catch (FileNotFoundException e) {
          LOG.error("Can't get random access to file " + dumpFilePath);
          // Disable dump
          enabledDump = false;
          return;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Start dump. Before dump, nonSequentialWriteInMemory == "
            + nonSequentialWriteInMemory.get());
      }

      Iterator<OffsetRange> it = pendingWrites.keySet().iterator();
      while (activeState && it.hasNext()
          && nonSequentialWriteInMemory.get() > 0) {
        OffsetRange key = it.next();
        WriteCtx writeCtx = pendingWrites.get(key);
        if (writeCtx == null) {
          // This write was just deleted
          continue;
        }
        try {
          long dumpedDataSize = writeCtx.dumpData(dumpOut, raf);
          if (dumpedDataSize > 0) {
            updateNonSequentialWriteInMemory(-dumpedDataSize);
          }
        } catch (IOException e) {
          LOG.error("Dump data failed:" + writeCtx + " with error:" + e
              + " OpenFileCtx state:" + activeState);
          // Disable dump
          enabledDump = false;
          return;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("After dump, nonSequentialWriteInMemory == "
            + nonSequentialWriteInMemory.get());
      }
    }

    @Override
    public void run() {
      while (activeState && enabledDump) {
        try {
          if (nonSequentialWriteInMemory.get() >= DUMP_WRITE_WATER_MARK) {
            dump();
          }
          synchronized (OpenFileCtx.this) {
            if (nonSequentialWriteInMemory.get() < DUMP_WRITE_WATER_MARK) {
              try {
                OpenFileCtx.this.wait();
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Dumper woke up");
                }
              } catch (InterruptedException e) {
                LOG.info("Dumper is interrupted, dumpFilePath= "
                    + OpenFileCtx.this.dumpFilePath);
              }
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Dumper checking OpenFileCtx activeState: " + activeState
                + " enabledDump: " + enabledDump);
          }
        } catch (Throwable t) {
          LOG.info("Dumper get Throwable: " + t + ". dumpFilePath: "
              + OpenFileCtx.this.dumpFilePath);
        }
      }
    }
  }
  
  private WriteCtx checkRepeatedWriteRequest(WRITE3Request request,
      Channel channel, int xid) {
    OffsetRange range = new OffsetRange(request.getOffset(),
        request.getOffset() + request.getCount());
    WriteCtx writeCtx = pendingWrites.get(range);
    if (writeCtx== null) {
      return null;
    } else {
      if (xid != writeCtx.getXid()) {
        LOG.warn("Got a repeated request, same range, with a different xid:"
            + xid + " xid in old request:" + writeCtx.getXid());
        //TODO: better handling.
      }
      return writeCtx;  
    }
  }
  
  public void receivedNewWrite(DFSClient dfsClient, WRITE3Request request,
      Channel channel, int xid, AsyncDataService asyncDataService,
      IdUserGroup iug) {
    
    if (!activeState) {
      LOG.info("OpenFileCtx is inactive, fileId:"
          + request.getHandle().getFileId());
      WccData fileWcc = new WccData(latestAttr.getWccAttr(), latestAttr);
      WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
          fileWcc, 0, request.getStableHow(), Nfs3Constant.WRITE_COMMIT_VERF);
      Nfs3Utils.writeChannel(channel,
          response.writeHeaderAndResponse(new XDR(), xid, new VerifierNone()),
          xid);
    } else {
      // Update the write time first
      updateLastAccessTime();
      
      // Handle repeated write requests (same xid or not).
      // If already replied, send reply again. If not replied, drop the
      // repeated request.
      WriteCtx existantWriteCtx = checkRepeatedWriteRequest(request, channel,
          xid);
      if (existantWriteCtx != null) {
        if (!existantWriteCtx.getReplied()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Repeated write request which hasn't be served: xid="
                + xid + ", drop it.");
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Repeated write request which is already served: xid="
                + xid + ", resend response.");
          }
          WccData fileWcc = new WccData(latestAttr.getWccAttr(), latestAttr);
          WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
              fileWcc, request.getCount(), request.getStableHow(),
              Nfs3Constant.WRITE_COMMIT_VERF);
          Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
              new XDR(), xid, new VerifierNone()), xid);
        }
      } else {
        // not a repeated write request
        receivedNewWriteInternal(dfsClient, request, channel, xid,
            asyncDataService, iug);
      }
    }
  }

  @VisibleForTesting
  public static void alterWriteRequest(WRITE3Request request, long cachedOffset) {
    long offset = request.getOffset();
    int count = request.getCount();
    long smallerCount = offset + count - cachedOffset;
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Got overwrite with appended data (%d-%d),"
          + " current offset %d," + " drop the overlapped section (%d-%d)"
          + " and append new data (%d-%d).", offset, (offset + count - 1),
          cachedOffset, offset, (cachedOffset - 1), cachedOffset, (offset
              + count - 1)));
    }
    
    ByteBuffer data = request.getData();
    Preconditions.checkState(data.position() == 0,
        "The write request data has non-zero position");
    data.position((int) (cachedOffset - offset));
    Preconditions.checkState(data.limit() - data.position() == smallerCount,
        "The write request buffer has wrong limit/position regarding count");
    
    request.setOffset(cachedOffset);
    request.setCount((int) smallerCount);
  }
  
  /**
   * Creates and adds a WriteCtx into the pendingWrites map. This is a
   * synchronized method to handle concurrent writes.
   * 
   * @return A non-null {@link WriteCtx} instance if the incoming write
   *         request's offset >= nextOffset. Otherwise null.
   */
  private synchronized WriteCtx addWritesToCache(WRITE3Request request,
      Channel channel, int xid) {
    long offset = request.getOffset();
    int count = request.getCount();
    long cachedOffset = nextOffset.get();
    int originalCount = WriteCtx.INVALID_ORIGINAL_COUNT;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("requesed offset=" + offset + " and current offset="
          + cachedOffset);
    }

    // Handle a special case first
    if ((offset < cachedOffset) && (offset + count > cachedOffset)) {
      // One Linux client behavior: after a file is closed and reopened to
      // write, the client sometimes combines previous written data(could still
      // be in kernel buffer) with newly appended data in one write. This is
      // usually the first write after file reopened. In this
      // case, we log the event and drop the overlapped section.
      LOG.warn(String.format("Got overwrite with appended data (%d-%d),"
          + " current offset %d," + " drop the overlapped section (%d-%d)"
          + " and append new data (%d-%d).", offset, (offset + count - 1),
          cachedOffset, offset, (cachedOffset - 1), cachedOffset, (offset
              + count - 1)));

      if (!pendingWrites.isEmpty()) {
        LOG.warn("There are other pending writes, fail this jumbo write");
        return null;
      }
      
      LOG.warn("Modify this write to write only the appended data");
      alterWriteRequest(request, cachedOffset);

      // Update local variable
      originalCount = count;
      offset = request.getOffset();
      count = request.getCount();
    }
    
    // Fail non-append call
    if (offset < cachedOffset) {
      LOG.warn("(offset,count,nextOffset):" + "(" + offset + "," + count + ","
          + nextOffset + ")");
      return null;
    } else {
      DataState dataState = offset == cachedOffset ? WriteCtx.DataState.NO_DUMP
          : WriteCtx.DataState.ALLOW_DUMP;
      WriteCtx writeCtx = new WriteCtx(request.getHandle(),
          request.getOffset(), request.getCount(), originalCount,
          request.getStableHow(), request.getData(), channel, xid, false,
          dataState);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Add new write to the list with nextOffset " + cachedOffset
            + " and requesed offset=" + offset);
      }
      if (writeCtx.getDataState() == WriteCtx.DataState.ALLOW_DUMP) {
        // update the memory size
        updateNonSequentialWriteInMemory(count);
      }
      // check if there is a WriteCtx with the same range in pendingWrites
      WriteCtx oldWriteCtx = checkRepeatedWriteRequest(request, channel, xid);
      if (oldWriteCtx == null) {
        addWrite(writeCtx);
      } else {
        LOG.warn("Got a repeated request, same range, with xid:"
            + writeCtx.getXid());
      }
      return writeCtx;
    }
  }
  
  /** Process an overwrite write request */
  private void processOverWrite(DFSClient dfsClient, WRITE3Request request,
      Channel channel, int xid, IdUserGroup iug) {
    WccData wccData = new WccData(latestAttr.getWccAttr(), null);
    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();
    WRITE3Response response;
    long cachedOffset = nextOffset.get();
    if (offset + count > cachedOffset) {
      LOG.warn("Treat this jumbo write as a real random write, no support.");
      response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL, wccData, 0,
          WriteStableHow.UNSTABLE, Nfs3Constant.WRITE_COMMIT_VERF);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Process perfectOverWrite");
      }
      // TODO: let executor handle perfect overwrite
      response = processPerfectOverWrite(dfsClient, offset, count, stableHow,
          request.getData().array(),
          Nfs3Utils.getFileIdPath(request.getHandle()), wccData, iug);
    }
    updateLastAccessTime();
    Nfs3Utils.writeChannel(channel,
        response.writeHeaderAndResponse(new XDR(), xid, new VerifierNone()),
        xid);
  }
  
  /**
   * Check if we can start the write (back to HDFS) now. If there is no hole for
   * writing, and there is no other threads writing (i.e., asyncStatus is
   * false), start the writing and set asyncStatus to true.
   * 
   * @return True if the new write is sequencial and we can start writing
   *         (including the case that there is already a thread writing).
   */
  private synchronized boolean checkAndStartWrite(
      AsyncDataService asyncDataService, WriteCtx writeCtx) {
    
    if (writeCtx.getOffset() == nextOffset.get()) {
      if (!asyncStatus) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Trigger the write back task. Current nextOffset: "
              + nextOffset.get());
        }
        asyncStatus = true;
        asyncDataService.execute(new AsyncDataService.WriteBackTask(this));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The write back thread is working.");
        }
      }
      return true;
    } else {
      return false;
    }
  }

  private void receivedNewWriteInternal(DFSClient dfsClient,
      WRITE3Request request, Channel channel, int xid,
      AsyncDataService asyncDataService, IdUserGroup iug) {
    WriteStableHow stableHow = request.getStableHow();
    WccAttr preOpAttr = latestAttr.getWccAttr();
    int count = request.getCount();

    WriteCtx writeCtx = addWritesToCache(request, channel, xid);
    if (writeCtx == null) {
      // offset < nextOffset
      processOverWrite(dfsClient, request, channel, xid, iug);
    } else {
      // The writes is added to pendingWrites.
      // Check and start writing back if necessary
      boolean startWriting = checkAndStartWrite(asyncDataService, writeCtx);
      if (!startWriting) {
        // offset > nextOffset. check if we need to dump data
        checkDump();
        
        // In test, noticed some Linux client sends a batch (e.g., 1MB)
        // of reordered writes and won't send more writes until it gets
        // responses of the previous batch. So here send response immediately
        // for unstable non-sequential write
        if (request.getStableHow() == WriteStableHow.UNSTABLE) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("UNSTABLE write request, send response for offset: "
                + writeCtx.getOffset());
          }
          WccData fileWcc = new WccData(preOpAttr, latestAttr);
          WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
              fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
          Nfs3Utils
              .writeChannel(channel, response.writeHeaderAndResponse(new XDR(),
                  xid, new VerifierNone()), xid);
          writeCtx.setReplied(true);
        }
      }
    }
  }
  
  /**
   * Honor 2 kinds of overwrites: 1). support some application like touch(write
   * the same content back to change mtime), 2) client somehow sends the same
   * write again in a different RPC.
   */
  private WRITE3Response processPerfectOverWrite(DFSClient dfsClient,
      long offset, int count, WriteStableHow stableHow, byte[] data,
      String path, WccData wccData, IdUserGroup iug) {
    WRITE3Response response = null;

    // Read the content back
    byte[] readbuffer = new byte[count];

    int readCount = 0;
    FSDataInputStream fis = null;
    try {
      // Sync file data and length to avoid partial read failure
      fos.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    } catch (ClosedChannelException closedException) {
      LOG.info("The FSDataOutputStream has been closed. " +
      		"Continue processing the perfect overwrite.");
    } catch (IOException e) {
      LOG.info("hsync failed when processing possible perfect overwrite, path="
          + path + " error:" + e);
      return new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0, stableHow,
          Nfs3Constant.WRITE_COMMIT_VERF);
    }
    
    try {
      fis = new FSDataInputStream(dfsClient.open(path));
      readCount = fis.read(offset, readbuffer, 0, count);
      if (readCount < count) {
        LOG.error("Can't read back " + count + " bytes, partial read size:"
            + readCount);
        return new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0, stableHow,
            Nfs3Constant.WRITE_COMMIT_VERF);
      }
    } catch (IOException e) {
      LOG.info("Read failed when processing possible perfect overwrite, path="
          + path + " error:" + e);
      return new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0, stableHow,
          Nfs3Constant.WRITE_COMMIT_VERF);
    } finally {
      IOUtils.cleanup(LOG, fis);
    }

    // Compare with the request
    Comparator comparator = new Comparator();
    if (comparator.compare(readbuffer, 0, readCount, data, 0, count) != 0) {
      LOG.info("Perfect overwrite has different content");
      response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL, wccData, 0,
          stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
    } else {
      LOG.info("Perfect overwrite has same content,"
          + " updating the mtime, then return success");
      Nfs3FileAttributes postOpAttr = null;
      try {
        dfsClient.setTimes(path, System.currentTimeMillis(), -1);
        postOpAttr = Nfs3Utils.getFileAttr(dfsClient, path, iug);
      } catch (IOException e) {
        LOG.info("Got error when processing perfect overwrite, path=" + path
            + " error:" + e);
        return new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0, stableHow,
            Nfs3Constant.WRITE_COMMIT_VERF);
      }

      wccData.setPostOpAttr(postOpAttr);
      response = new WRITE3Response(Nfs3Status.NFS3_OK, wccData, count,
          stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
    }
    return response;
  }

  /**
   * return one commit status: COMMIT_FINISHED, COMMIT_WAIT,
   * COMMIT_INACTIVE_CTX, COMMIT_ERROR
   */
  public int checkCommit(long commitOffset) {
    return activeState ? checkCommitInternal(commitOffset)
        : COMMIT_INACTIVE_CTX;
  }
  
  private int checkCommitInternal(long commitOffset) {
    if (commitOffset == 0) {
      // Commit whole file
      commitOffset = nextOffset.get();
    }

    long flushed = 0;
    try {
      flushed = getFlushedOffset();
    } catch (IOException e) {
      LOG.error("Can't get flushed offset, error:" + e);
      return COMMIT_ERROR;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("getFlushedOffset=" + flushed + " commitOffset=" + commitOffset);
    }
    if (flushed < commitOffset) {
      // Keep stream active
      updateLastAccessTime();
      return COMMIT_WAIT;
    }

    int ret = COMMIT_WAIT;
    try {
      // Sync file data and length
      fos.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
      // Nothing to do for metadata since attr related change is pass-through
      ret = COMMIT_FINISHED;
    } catch (ClosedChannelException cce) { 
      ret = COMMIT_INACTIVE_CTX;
      if (pendingWrites.isEmpty()) {
        ret = COMMIT_INACTIVE_CTX;
      } else {
        ret = COMMIT_INACTIVE_WITH_PENDING_WRITE;
      }
    } catch (IOException e) {
      LOG.error("Got stream error during data sync:" + e);
      // Do nothing. Stream will be closed eventually by StreamMonitor.
      ret = COMMIT_ERROR;
    }

    // Keep stream active
    updateLastAccessTime();
    return ret;
  }
  
  private void addWrite(WriteCtx writeCtx) {
    long offset = writeCtx.getOffset();
    int count = writeCtx.getCount();
    // For the offset range (min, max), min is inclusive, and max is exclusive
    pendingWrites.put(new OffsetRange(offset, offset + count), writeCtx);
  }
  
  /**
   * Check stream status to decide if it should be closed
   * @return true, remove stream; false, keep stream
   */
  public synchronized boolean streamCleanup(long fileId, long streamTimeout) {
    if (streamTimeout < WriteManager.MINIMIUM_STREAM_TIMEOUT) {
      throw new InvalidParameterException("StreamTimeout" + streamTimeout
          + "ms is less than MINIMIUM_STREAM_TIMEOUT "
          + WriteManager.MINIMIUM_STREAM_TIMEOUT + "ms");
    }
    
    boolean flag = false;
    // Check the stream timeout
    if (checkStreamTimeout(streamTimeout)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("closing stream for fileId:" + fileId);
      }
      cleanup();
      flag = true;
    }
    return flag;
  }
  
  /**
   * Get (and remove) the next WriteCtx from {@link #pendingWrites} if possible.
   * 
   * @return Null if {@link #pendingWrites} is null, or the next WriteCtx's
   *         offset is larger than nextOffSet.
   */
  private synchronized WriteCtx offerNextToWrite() {
    if (pendingWrites.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("The asyn write task has no pending writes, fileId: "
            + latestAttr.getFileId());
      }
      this.asyncStatus = false;
    } else {
      Entry<OffsetRange, WriteCtx> lastEntry = pendingWrites.lastEntry();
      OffsetRange range = lastEntry.getKey();
      WriteCtx toWrite = lastEntry.getValue();
      
      if (LOG.isTraceEnabled()) {
        LOG.trace("range.getMin()=" + range.getMin() + " nextOffset="
            + nextOffset);
      }
      
      long offset = nextOffset.get();
      if (range.getMin() > offset) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The next sequencial write has not arrived yet");
        }
        this.asyncStatus = false;
      } else if (range.getMin() < offset && range.getMax() > offset) {
        // shouldn't happen since we do sync for overlapped concurrent writers
        LOG.warn("Got a overlapping write (" + range.getMin() + ","
            + range.getMax() + "), nextOffset=" + offset
            + ". Silently drop it now");
        pendingWrites.remove(range);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove write(" + range.getMin() + "-" + range.getMax()
              + ") from the list");
        }
        // after writing, remove the WriteCtx from cache 
        pendingWrites.remove(range);
        // update nextOffset
        nextOffset.addAndGet(toWrite.getCount());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Change nextOffset to " + nextOffset.get());
        }
        return toWrite;
      }
    }
    return null;
  }
  
  /** Invoked by AsynDataService to write back to HDFS */
  void executeWriteBack() {
    Preconditions.checkState(asyncStatus,
        "The openFileCtx has false async status");
    try {
      while (activeState) {
        WriteCtx toWrite = offerNextToWrite();
        if (toWrite != null) {
          // Do the write
          doSingleWrite(toWrite);
          updateLastAccessTime();
        } else {
          break;
        }
      }
      
      if (!activeState && LOG.isDebugEnabled()) {
        LOG.debug("The openFileCtx is not active anymore, fileId: "
            + +latestAttr.getFileId());
      }
    } finally {
      // make sure we reset asyncStatus to false
      asyncStatus = false;
    }
  }

  private void doSingleWrite(final WriteCtx writeCtx) {
    Channel channel = writeCtx.getChannel();
    int xid = writeCtx.getXid();

    long offset = writeCtx.getOffset();
    int count = writeCtx.getCount();
    WriteStableHow stableHow = writeCtx.getStableHow();
    
    FileHandle handle = writeCtx.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("do write, fileId: " + handle.getFileId() + " offset: "
          + offset + " length:" + count + " stableHow:" + stableHow.getValue());
    }

    try {
      // The write is not protected by lock. asyncState is used to make sure
      // there is one thread doing write back at any time    
      writeCtx.writeData(fos);
      
      long flushedOffset = getFlushedOffset();
      if (flushedOffset != (offset + count)) {
        throw new IOException("output stream is out of sync, pos="
            + flushedOffset + " and nextOffset should be"
            + (offset + count));
      }
      

      // Reduce memory occupation size if request was allowed dumped
      if (writeCtx.getDataState() == WriteCtx.DataState.ALLOW_DUMP) {
        synchronized (writeCtx) {
          if (writeCtx.getDataState() == WriteCtx.DataState.ALLOW_DUMP) {
            writeCtx.setDataState(WriteCtx.DataState.NO_DUMP);
            updateNonSequentialWriteInMemory(-count);
            if (LOG.isDebugEnabled()) {
              LOG.debug("After writing " + handle.getFileId() + " at offset "
                  + offset + ", updated the memory count, new value:"
                  + nonSequentialWriteInMemory.get());
            }
          }
        }
      }
      
      if (!writeCtx.getReplied()) {
        WccAttr preOpAttr = latestAttr.getWccAttr();
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        if (writeCtx.getOriginalCount() != WriteCtx.INVALID_ORIGINAL_COUNT) {
          LOG.warn("Return original count:" + writeCtx.getOriginalCount()
              + " instead of real data count:" + count);
          count = writeCtx.getOriginalCount();
        }
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
            new XDR(), xid, new VerifierNone()), xid);
      }
    } catch (IOException e) {
      LOG.error("Error writing to fileId " + handle.getFileId() + " at offset "
          + offset + " and length " + count, e);
      if (!writeCtx.getReplied()) {
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO);
        Nfs3Utils.writeChannel(channel, response.writeHeaderAndResponse(
            new XDR(), xid, new VerifierNone()), xid);
        // Keep stream open. Either client retries or SteamMonitor closes it.
      }

      LOG.info("Clean up open file context for fileId: "
          + latestAttr.getFileid());
      cleanup();
    }
  }

  private synchronized void cleanup() {
    if (!activeState) {
      LOG.info("Current OpenFileCtx is already inactive, no need to cleanup.");
      return;
    }
    activeState = false;

    // stop the dump thread
    if (dumpThread != null) {
      dumpThread.interrupt();
      try {
        dumpThread.join(3000);
      } catch (InterruptedException e) {
      }
    }
    
    // Close stream
    try {
      if (fos != null) {
        fos.close();
      }
    } catch (IOException e) {
      LOG.info("Can't close stream for fileId:" + latestAttr.getFileid()
          + ", error:" + e);
    }
    
    // Reply error for pending writes
    LOG.info("There are " + pendingWrites.size() + " pending writes.");
    WccAttr preOpAttr = latestAttr.getWccAttr();
    while (!pendingWrites.isEmpty()) {
      OffsetRange key = pendingWrites.firstKey();
      LOG.info("Fail pending write: (" + key.getMin() + "," + key.getMax()
          + "), nextOffset=" + nextOffset.get());
      
      WriteCtx writeCtx = pendingWrites.remove(key);
      if (!writeCtx.getReplied()) {
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
            fileWcc, 0, writeCtx.getStableHow(), Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(writeCtx.getChannel(), response
            .writeHeaderAndResponse(new XDR(), writeCtx.getXid(),
                new VerifierNone()), writeCtx.getXid());
      }
    }
    
    // Cleanup dump file
    if (dumpOut != null) {
      try {
        dumpOut.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      File dumpFile = new File(dumpFilePath);
      if (dumpFile.exists() && !dumpFile.delete()) {
        LOG.error("Failed to delete dumpfile: " + dumpFile);
      }
    }
    if (raf != null) {
      try {
        raf.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}

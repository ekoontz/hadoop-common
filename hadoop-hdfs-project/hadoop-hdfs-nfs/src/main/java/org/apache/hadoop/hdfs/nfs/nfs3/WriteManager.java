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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.nfs.NfsFileType;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.util.Daemon;
import org.jboss.netty.channel.Channel;

import com.google.common.collect.Maps;

/**
 * Manage the writes and responds asynchronously.
 */
public class WriteManager {
  public static final Log LOG = LogFactory.getLog(WriteManager.class);

  private final IdUserGroup iug;
  private final ConcurrentMap<FileHandle, OpenFileCtx> openFileMap = Maps
      .newConcurrentMap();

  private AsyncDataService asyncDataService;
  private boolean asyncDataServiceStarted = false;

  private final StreamMonitor streamMonitor;
  
  /**
   * The time limit to wait for accumulate reordered sequential writes to the
   * same file before the write is considered done.
   */
  private long streamTimeout;
  
  public static final long DEFAULT_STREAM_TIMEOUT = 10 * 1000; // 10 second
  public static final long MINIMIUM_STREAM_TIMEOUT = 1 * 1000; // 1 second
  
  void addOpenFileStream(FileHandle h, OpenFileCtx ctx) {
    openFileMap.put(h, ctx);
    if (LOG.isDebugEnabled()) {
      LOG.debug("After add the new stream " + h.getFileId()
          + ", the stream number:" + openFileMap.size());
    }
  }

  WriteManager(IdUserGroup iug, final Configuration config) {
    this.iug = iug;
    
    streamTimeout = config.getLong("dfs.nfs3.stream.timeout",
        DEFAULT_STREAM_TIMEOUT);
    LOG.info("Stream timeout is " + streamTimeout + "ms.");
    if (streamTimeout < MINIMIUM_STREAM_TIMEOUT) {
      LOG.info("Reset stream timeout to minimum value "
          + MINIMIUM_STREAM_TIMEOUT + "ms.");
      streamTimeout = MINIMIUM_STREAM_TIMEOUT;
    }
    
    this.streamMonitor = new StreamMonitor();
  }

  private void startAsyncDataSerivce() {
    streamMonitor.start();
    this.asyncDataService = new AsyncDataService();
    asyncDataServiceStarted = true;
  }

  private void shutdownAsyncDataService() {
    asyncDataService.shutdown();
    asyncDataServiceStarted = false;
    streamMonitor.interrupt();
  }

  void handleWrite(DFSClient dfsClient, WRITE3Request request, Channel channel,
      int xid, Nfs3FileAttributes preOpAttr) throws IOException {
    // First write request starts the async data service
    if (!asyncDataServiceStarted) {
      startAsyncDataSerivce();
    }

    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();
    byte[] data = request.getData().array();
    if (data.length < count) {
      WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL);
      Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      return;
    }

    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("handleWrite fileId: " + handle.getFileId() + " offset: "
          + offset + " length:" + count + " stableHow:" + stableHow.getValue());
    }

    // Check if there is a stream to write
    FileHandle fileHandle = request.getHandle();
    OpenFileCtx openFileCtx = openFileMap.get(fileHandle);
    if (openFileCtx == null) {
      LOG.info("No opened stream for fileId:" + fileHandle.getFileId());
      WccData fileWcc = new WccData(Nfs3Utils.getWccAttr(preOpAttr), preOpAttr);
      WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
          fileWcc, count, request.getStableHow(),
          Nfs3Constant.WRITE_COMMIT_VERF);
      Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      return;
    }

    // Add write into the async job queue
    openFileCtx.receivedNewWrite(dfsClient, request, channel, xid,
        asyncDataService, iug);
    // Block stable write
    if (request.getStableHow() != WriteStableHow.UNSTABLE) {
      if (handleCommit(fileHandle, offset + count)) {
        Nfs3FileAttributes postOpAttr = getFileAttr(dfsClient, handle, iug);
        WccData fileWcc = new WccData(Nfs3Utils.getWccAttr(preOpAttr),
            postOpAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, request.getStableHow(),
            Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      } else {
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO);
        Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      }
    }

    return;
  }

  boolean handleCommit(FileHandle fileHandle, long commitOffset) {
    OpenFileCtx openFileCtx = openFileMap.get(fileHandle);
    if (openFileCtx == null) {
      LOG.info("No opened stream for fileId:" + fileHandle.getFileId()
          + " commitOffset=" + commitOffset);
      return true;
    }
    long timeout = 30 * 1000; // 30 seconds
    long startCommit = System.currentTimeMillis();
    while (true) {
      int ret = openFileCtx.checkCommit(commitOffset);
      if (ret == OpenFileCtx.COMMIT_FINISHED) {
        // Committed
        return true;
      } else if (ret == OpenFileCtx.COMMIT_INACTIVE_CTX) {
        LOG.info("Inactive stream, fileId=" + fileHandle.getFileId()
            + " commitOffset=" + commitOffset);
        return true;
      }
      assert (ret == OpenFileCtx.COMMIT_WAIT || ret == OpenFileCtx.COMMIT_ERROR);
      if (ret == OpenFileCtx.COMMIT_ERROR) {
        return false;
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not committed yet, wait., fileId=" + fileHandle.getFileId()
            + " commitOffset=" + commitOffset);
      }
      if (System.currentTimeMillis() - startCommit > timeout) {
        // Commit took too long, return error
        return false;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.info("Commit is interrupted, fileId=" + fileHandle.getFileId()
            + " commitOffset=" + commitOffset);
        return false;
      }
    }// while
  }

  /**
   * If the file is in cache, update the size based on the cached data size
   */
  Nfs3FileAttributes getFileAttr(DFSClient client, FileHandle fileHandle,
      IdUserGroup iug) throws IOException {
    String fileIdPath = Nfs3Utils.getFileIdPath(fileHandle);
    Nfs3FileAttributes attr = Nfs3Utils.getFileAttr(client, fileIdPath, iug);
    if (attr != null) {
      OpenFileCtx openFileCtx = openFileMap.get(fileHandle);
      if (openFileCtx != null) {
        attr.setSize(openFileCtx.getNextOffset());
        attr.setUsed(openFileCtx.getNextOffset());
      }
    }
    return attr;
  }

  Nfs3FileAttributes getFileAttr(DFSClient client, FileHandle dirHandle,
      String fileName) throws IOException {
    String fileIdPath = Nfs3Utils.getFileIdPath(dirHandle) + "/" + fileName;
    Nfs3FileAttributes attr = Nfs3Utils.getFileAttr(client, fileIdPath, iug);

    if ((attr != null) && (attr.getType() == NfsFileType.NFSREG.toValue())) {
      OpenFileCtx openFileCtx = openFileMap
          .get(new FileHandle(attr.getFileId()));

      if (openFileCtx != null) {
        attr.setSize(openFileCtx.getNextOffset());
        attr.setUsed(openFileCtx.getNextOffset());
      }
    }
    return attr;
  }
  
  /**
   * StreamMonitor wakes up periodically to find and closes idle streams.
   */
  class StreamMonitor extends Daemon {
    private int rotation = 5 * 1000; // 5 seconds
    private long lastWakeupTime = 0;

    @Override
    public void run() {
      while (true) {
        Iterator<Entry<FileHandle, OpenFileCtx>> it = openFileMap.entrySet()
            .iterator();
        if (LOG.isTraceEnabled()) {
          LOG.trace("openFileMap size:" + openFileMap.size());
        }
        while (it.hasNext()) {
          Entry<FileHandle, OpenFileCtx> pairs = it.next();
          OpenFileCtx ctx = pairs.getValue();
          if (ctx.streamCleanup((pairs.getKey()).getFileId(), streamTimeout)) {
            it.remove();
            if (LOG.isDebugEnabled()) {
              LOG.debug("After remove stream " + pairs.getKey().getFileId()
                  + ", the stream number:" + openFileMap.size());
            }
          }
        }

        // Check if it can sleep
        try {
          long workedTime = System.currentTimeMillis() - lastWakeupTime;
          if (workedTime < rotation) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("StreamMonitor can still have a sleep:"
                  + ((rotation - workedTime) / 1000));
            }
            Thread.sleep(rotation - workedTime);
          }
          lastWakeupTime = System.currentTimeMillis();

        } catch (InterruptedException e) {
          LOG.info("StreamMonitor got interrupted");
          return;
        }
      }
    }
  }
}

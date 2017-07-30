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
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.NFSPROC3;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Interface;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.ACCESS3Request;
import org.apache.hadoop.nfs.nfs3.request.COMMIT3Request;
import org.apache.hadoop.nfs.nfs3.request.CREATE3Request;
import org.apache.hadoop.nfs.nfs3.request.FSINFO3Request;
import org.apache.hadoop.nfs.nfs3.request.FSSTAT3Request;
import org.apache.hadoop.nfs.nfs3.request.GETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.LOOKUP3Request;
import org.apache.hadoop.nfs.nfs3.request.MKDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.PATHCONF3Request;
import org.apache.hadoop.nfs.nfs3.request.READ3Request;
import org.apache.hadoop.nfs.nfs3.request.READDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.READDIRPLUS3Request;
import org.apache.hadoop.nfs.nfs3.request.REMOVE3Request;
import org.apache.hadoop.nfs.nfs3.request.RENAME3Request;
import org.apache.hadoop.nfs.nfs3.request.RMDIR3Request;
import org.apache.hadoop.nfs.nfs3.request.SETATTR3Request;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3.SetAttrField;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.ACCESS3Response;
import org.apache.hadoop.nfs.nfs3.response.COMMIT3Response;
import org.apache.hadoop.nfs.nfs3.response.CREATE3Response;
import org.apache.hadoop.nfs.nfs3.response.FSINFO3Response;
import org.apache.hadoop.nfs.nfs3.response.FSSTAT3Response;
import org.apache.hadoop.nfs.nfs3.response.GETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.LOOKUP3Response;
import org.apache.hadoop.nfs.nfs3.response.MKDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.NFS3Response;
import org.apache.hadoop.nfs.nfs3.response.PATHCONF3Response;
import org.apache.hadoop.nfs.nfs3.response.READ3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response.DirList3;
import org.apache.hadoop.nfs.nfs3.response.READDIR3Response.Entry3;
import org.apache.hadoop.nfs.nfs3.response.READDIRPLUS3Response;
import org.apache.hadoop.nfs.nfs3.response.READDIRPLUS3Response.DirListPlus3;
import org.apache.hadoop.nfs.nfs3.response.READLINK3Response;
import org.apache.hadoop.nfs.nfs3.response.REMOVE3Response;
import org.apache.hadoop.nfs.nfs3.response.RENAME3Response;
import org.apache.hadoop.nfs.nfs3.response.RMDIR3Response;
import org.apache.hadoop.nfs.nfs3.response.SETATTR3Response;
import org.apache.hadoop.nfs.nfs3.response.SYMLINK3Response;
import org.apache.hadoop.nfs.nfs3.response.VoidResponse;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcAuthInfo.AuthFlavor;
import org.apache.hadoop.oncrpc.RpcAuthSys;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcDeniedReply;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.RpcReply;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.security.AccessControlException;
import org.jboss.netty.channel.Channel;

/**
 * RPC program corresponding to nfs daemon. See {@link Nfs3}.
 */
public class RpcProgramNfs3 extends RpcProgram implements Nfs3Interface {
  public static final int DEFAULT_UMASK = 0022;
  public static final FsPermission umask = new FsPermission(
      (short) DEFAULT_UMASK);
  
  private static final Log LOG = LogFactory.getLog(RpcProgramNfs3.class);
  private static final int MAX_READ_TRANSFER_SIZE = 64 * 1024;
  private static final int MAX_WRITE_TRANSFER_SIZE = 64 * 1024;
  private static final int MAX_READDIR_TRANSFER_SIZE = 64 * 1024;

  private final Configuration config = new Configuration();
  private final WriteManager writeManager;
  private final IdUserGroup iug;// = new IdUserGroup();
  private final DFSClientCache clientCache;

  /**
   * superUserClient should always impersonate HDFS file system owner to send
   * requests which requires supergroup privilege. This requires the same user
   * to start HDFS and NFS.
   */
  private final DFSClient superUserClient;
  
  private final short replication;
  private final long blockSize;
  private final int bufferSize;
  private Statistics statistics;
  private String writeDumpDir; // The dir save dump files
  
  public RpcProgramNfs3(List<String> exports) throws IOException {
    this(exports, new Configuration());
  }

  public RpcProgramNfs3(List<String> exports, Configuration config)
      throws IOException {
    super("NFS3", "localhost", Nfs3Constant.PORT, Nfs3Constant.PROGRAM,
        Nfs3Constant.VERSION, Nfs3Constant.VERSION, 100);
   
    config.set(FsPermission.UMASK_LABEL, "000");
    iug = new IdUserGroup();
    writeManager = new WriteManager(iug, config);
    clientCache = new DFSClientCache(config);
    superUserClient = new DFSClient(NameNode.getAddress(config), config);
    replication = (short) config.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    blockSize = config.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    bufferSize = config.getInt("io.file.buffer.size", 4096);
    
    writeDumpDir = config.get("dfs.nfs3.dump.dir", "/tmp/.hdfs-nfs");    
    boolean enableDump = config.getBoolean("dfs.nfs3.enableDump", true);
    if (!enableDump) {
      writeDumpDir = null;
    } else {
      clearDirectory(writeDumpDir);
    }
  }

  private void clearDirectory(String writeDumpDir) throws IOException {
    File dumpDir = new File(writeDumpDir);
    if (dumpDir.exists()) {
      LOG.info("Delete current dump directory " + writeDumpDir);
      if (!(FileUtil.fullyDelete(dumpDir))) {
        throw new IOException("Cannot remove current dump directory: "
            + dumpDir);
      }
    }
    LOG.info("Create new dump directory " + writeDumpDir);
    if (!dumpDir.mkdirs()) {
      throw new IOException("Cannot create dump directory " + dumpDir);
    }
  }
  
  /******************************************************
   * RPC call handlers
   ******************************************************/
  
  public NFS3Response nullProcedure() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS NULL");
    }
    return new VoidResponse(Nfs3Status.NFS3_OK);
  }

  public GETATTR3Response getattr(XDR xdr, RpcAuthSys authSys) {
    GETATTR3Response response = new GETATTR3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    GETATTR3Request request = null;
    try {
      request = new GETATTR3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid GETATTR request");
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    FileHandle handle = request.getHandle();
    if (LOG.isTraceEnabled()) {
      LOG.trace("GETATTR for fileId: " + handle.getFileId());
    }

    Nfs3FileAttributes attrs = null;
    try {
      attrs = writeManager.getFileAttr(dfsClient, handle, iug);
    } catch (IOException e) {
      LOG.info("Can't get file attribute, fileId=" + handle.getFileId());
      response.setStatus(Nfs3Status.NFS3ERR_IO);
      return response;
    }
    if (attrs == null) {
      LOG.error("Can't get path for fileId:" + handle.getFileId());
      response.setStatus(Nfs3Status.NFS3ERR_STALE);
      return response;
    }
    response.setPostOpAttr(attrs);
    return response;
  }

  // Set attribute, don't support setting "size". For file/dir creation, mode is
  // set during creation and setMode should be false here.
  private void setattrInternal(DFSClient dfsClient, String fileIdPath,
      SetAttr3 newAttr, boolean setMode) throws IOException {
    EnumSet<SetAttrField> updateFields = newAttr.getUpdateFields();
    
    if (setMode && updateFields.contains(SetAttrField.MODE)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("set new mode:" + newAttr.getMode());
      }
      dfsClient.setPermission(fileIdPath,
          new FsPermission((short) (newAttr.getMode())));
    }
    if (updateFields.contains(SetAttrField.UID)
        || updateFields.contains(SetAttrField.GID)) {
      String uname = updateFields.contains(SetAttrField.UID) ? iug.getUserName(
          newAttr.getUid(), UNKNOWN_USER) : null;
      String gname = updateFields.contains(SetAttrField.GID) ? iug
          .getGroupName(newAttr.getGid(), UNKNOWN_GROUP) : null;
      dfsClient.setOwner(fileIdPath, uname, gname);
    }

    long atime = updateFields.contains(SetAttrField.ATIME) ? newAttr.getAtime()
        .getMilliSeconds() : -1;
    long mtime = updateFields.contains(SetAttrField.MTIME) ? newAttr.getMtime()
        .getMilliSeconds() : -1;
    if (atime != -1 || mtime != -1) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("set atime:" + +atime + " mtime:" + mtime);
      }
      dfsClient.setTimes(fileIdPath, mtime, atime);
    }
  }

  public SETATTR3Response setattr(XDR xdr, RpcAuthSys authSys) {
    SETATTR3Response response = new SETATTR3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    SETATTR3Request request = null;
    try {
      request = new SETATTR3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid SETATTR request");
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS SETATTR fileId: " + handle.getFileId());
    }

    if (request.getAttr().getUpdateFields().contains(SetAttrField.SIZE)) {
      LOG.error("Setting file size is not supported when setattr, fileId: "
          + handle.getFileId());
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    String fileIdPath = Nfs3Utils.getFileIdPath(handle);
    WccAttr preOpAttr = null;
    try {
      preOpAttr = Nfs3Utils.getWccAttr(dfsClient, fileIdPath);
      if (preOpAttr == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        response.setStatus(Nfs3Status.NFS3ERR_STALE);
        return response;
      }
      if (request.isCheck()) {
        if (!preOpAttr.getCtime().equals(request.getCtime())) {
          WccData wccData = Nfs3Utils.createWccData(preOpAttr, dfsClient,
              fileIdPath, iug);
          return new SETATTR3Response(Nfs3Status.NFS3ERR_NOT_SYNC, wccData);
        }
      }

      setattrInternal(dfsClient, fileIdPath, request.getAttr(), true);
      Nfs3FileAttributes postOpAttr = Nfs3Utils.getFileAttr(dfsClient,
          fileIdPath, iug);
      WccData wccData = new WccData(preOpAttr, postOpAttr);
      return new SETATTR3Response(Nfs3Status.NFS3_OK, wccData);

    } catch (IOException e) {
      LOG.warn("Exception ", e);
      WccData wccData = null;
      try {
        wccData = Nfs3Utils
            .createWccData(preOpAttr, dfsClient, fileIdPath, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpAttr for fileIdPath: " + fileIdPath);
      }
      if (e instanceof AccessControlException) {
        return new SETATTR3Response(Nfs3Status.NFS3ERR_ACCES, wccData);
      } else {
        return new SETATTR3Response(Nfs3Status.NFS3ERR_IO, wccData);
      }
    }
  }

  public LOOKUP3Response lookup(XDR xdr, RpcAuthSys authSys) {
    LOOKUP3Response response = new LOOKUP3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    LOOKUP3Request request = null;
    try {
      request = new LOOKUP3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid LOOKUP request");
      return new LOOKUP3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS LOOKUP dir fileId: " + dirHandle.getFileId() + " name: "
          + fileName);
    }

    try {
      String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
      Nfs3FileAttributes postOpObjAttr = writeManager.getFileAttr(dfsClient,
          dirHandle, fileName);
      if (postOpObjAttr == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("NFS LOOKUP fileId: " + dirHandle.getFileId() + " name:"
              + fileName + " does not exist");
        }
        Nfs3FileAttributes postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient,
            dirFileIdPath, iug);
        return new LOOKUP3Response(Nfs3Status.NFS3ERR_NOENT, null, null,
            postOpDirAttr);
      }

      Nfs3FileAttributes postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient,
          dirFileIdPath, iug);
      if (postOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId:" + dirHandle.getFileId());
        return new LOOKUP3Response(Nfs3Status.NFS3ERR_STALE);
      }
      FileHandle fileHandle = new FileHandle(postOpObjAttr.getFileId());
      return new LOOKUP3Response(Nfs3Status.NFS3_OK, fileHandle, postOpObjAttr,
          postOpDirAttr);

    } catch (IOException e) {
      LOG.warn("Exception ", e);
      return new LOOKUP3Response(Nfs3Status.NFS3ERR_IO);
    }
  }
  
  public ACCESS3Response access(XDR xdr, RpcAuthSys authSys) {
    ACCESS3Response response = new ACCESS3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    ACCESS3Request request = null;
    try {
      request = new ACCESS3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid ACCESS request");
      return new ACCESS3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    Nfs3FileAttributes attrs;

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS ACCESS fileId: " + handle.getFileId());
    } 

    try {
      // Use superUserClient to get file attr since we don't know whether the
      // NFS client user has access permission to the file
      attrs = writeManager.getFileAttr(superUserClient, handle, iug);
      if (attrs == null) {
        LOG.error("Can't get path for fileId:" + handle.getFileId());
        return new ACCESS3Response(Nfs3Status.NFS3ERR_STALE);
      }
      int access = Nfs3Utils.getAccessRightsForUserGroup(authSys.getUid(),
          authSys.getGid(), attrs);
      
      return new ACCESS3Response(Nfs3Status.NFS3_OK, attrs, access);
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      return new ACCESS3Response(Nfs3Status.NFS3ERR_IO);
    }
  }

  public READLINK3Response readlink(XDR xdr, RpcAuthSys authSys) {
    return new READLINK3Response(Nfs3Status.NFS3ERR_NOTSUPP);
  }

  public READ3Response read(XDR xdr, RpcAuthSys authSys) {
    READ3Response response = new READ3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    READ3Request request = null;

    try {
      request = new READ3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid READ request");
      return new READ3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    long offset = request.getOffset();
    int count = request.getCount();

    
    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS READ fileId: " + handle.getFileId() + " offset: " + offset
          + " count: " + count);
    }

    Nfs3FileAttributes attrs;
    boolean eof;
    if (count == 0) {
      // Only do access check.
      try {
        // Don't read from cache. Client may not have read permission.
        attrs = Nfs3Utils.getFileAttr(superUserClient,
            Nfs3Utils.getFileIdPath(handle), iug);
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Get error accessing file, fileId:" + handle.getFileId());
        }
        return new READ3Response(Nfs3Status.NFS3ERR_IO);
      }
      if (attrs == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Can't get path for fileId:" + handle.getFileId());
        }
        return new READ3Response(Nfs3Status.NFS3ERR_NOENT);
      }
      int access = Nfs3Utils.getAccessRightsForUserGroup(authSys.getUid(),
          authSys.getGid(), attrs);
      if ((access & Nfs3Constant.ACCESS3_READ) != 0) {
        eof = offset < attrs.getSize() ? false : true;
        return new READ3Response(Nfs3Status.NFS3_OK, attrs, 0, eof,
            ByteBuffer.wrap(new byte[0]));
      } else {
        return new READ3Response(Nfs3Status.NFS3ERR_ACCES);
      }
    }
    
    try {
      int buffSize = Math.min(MAX_READ_TRANSFER_SIZE, count);
      byte[] readbuffer = new byte[buffSize];

      DFSInputStream is = dfsClient.open(Nfs3Utils.getFileIdPath(handle));
      FSDataInputStream fis = new FSDataInputStream(is);
      
      int readCount = fis.read(offset, readbuffer, 0, count);
      fis.close();

      attrs = Nfs3Utils.getFileAttr(dfsClient, Nfs3Utils.getFileIdPath(handle),
          iug);
      if (readCount < count) {
        LOG.info("Partical read. Asked offset:" + offset + " count:" + count
            + " and read back:" + readCount + "file size:" + attrs.getSize());
      }
      // HDFS returns -1 for read beyond file size.
      if (readCount < 0) {
        readCount = 0;
      }
      eof = (offset + readCount) < attrs.getSize() ? false : true;
      return new READ3Response(Nfs3Status.NFS3_OK, attrs, readCount, eof,
          ByteBuffer.wrap(readbuffer));

    } catch (IOException e) {
      LOG.warn("Read error: " + e.getClass() + " offset: " + offset
          + " count: " + count, e);
      return new READ3Response(Nfs3Status.NFS3ERR_IO);
    }
  }

  public WRITE3Response write(XDR xdr, Channel channel, int xid,
      RpcAuthSys authSys) {
    WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    WRITE3Request request = null;

    try {
      request = new WRITE3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid WRITE request");
      return new WRITE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();
    byte[] data = request.getData().array();
    if (data.length < count) {
      LOG.error("Invalid argument, data size is less than count in request");
      return new WRITE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS WRITE fileId: " + handle.getFileId() + " offset: "
          + offset + " length:" + count + " stableHow:" + stableHow.getValue()
          + " xid:" + xid);
    }

    Nfs3FileAttributes preOpAttr = null;
    try {
      preOpAttr = writeManager.getFileAttr(dfsClient, handle, iug);
      if (preOpAttr == null) {
        LOG.error("Can't get path for fileId:" + handle.getFileId());
        return new WRITE3Response(Nfs3Status.NFS3ERR_STALE);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("requesed offset=" + offset + " and current filesize="
            + preOpAttr.getSize());
      }

      writeManager.handleWrite(dfsClient, request, channel, xid, preOpAttr);

    } catch (IOException e) {
      LOG.info("Error writing to fileId " + handle.getFileId() + " at offset "
          + offset + " and length " + data.length, e);
      // Try to return WccData
      Nfs3FileAttributes postOpAttr = null;
      try {
        postOpAttr = writeManager.getFileAttr(dfsClient, handle, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpAttr for fileId: " + handle.getFileId());
      }
      WccAttr attr = preOpAttr == null ? null : Nfs3Utils.getWccAttr(preOpAttr);
      WccData fileWcc = new WccData(attr, postOpAttr);
      return new WRITE3Response(Nfs3Status.NFS3ERR_IO, fileWcc, 0,
          request.getStableHow(), Nfs3Constant.WRITE_COMMIT_VERF);
    }

    return null;
  }

  public CREATE3Response create(XDR xdr, RpcAuthSys authSys) {
    CREATE3Response response = new CREATE3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    CREATE3Request request = null;

    try {
      request = new CREATE3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid CREATE request");
      return new CREATE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS CREATE dir fileId: " + dirHandle.getFileId()
          + " filename: " + fileName);
    }

    int createMode = request.getMode();
    if ((createMode != Nfs3Constant.CREATE_EXCLUSIVE)
        && request.getObjAttr().getUpdateFields().contains(SetAttrField.SIZE)) {
      LOG.error("Setting file size is not supported when creating file: "
          + fileName + " dir fileId:" + dirHandle.getFileId());
      return new CREATE3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FSDataOutputStream fos = null;
    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    WccAttr preOpDirAttr = null;
    Nfs3FileAttributes postOpObjAttr = null;
    FileHandle fileHandle = null;
    WccData dirWcc = null;
    try {
      preOpDirAttr = Nfs3Utils.getWccAttr(dfsClient, dirFileIdPath);
      if (preOpDirAttr == null) {
        LOG.error("Can't get path for dirHandle:" + dirHandle);
        return new CREATE3Response(Nfs3Status.NFS3ERR_STALE);
      }

      String fileIdPath = Nfs3Utils.getFileIdPath(dirHandle) + "/" + fileName;
      SetAttr3 setAttr3 = request.getObjAttr();
      assert (setAttr3 != null);
      FsPermission permission = setAttr3.getUpdateFields().contains(
          SetAttrField.MODE) ? new FsPermission((short) setAttr3.getMode())
          : FsPermission.getDefault().applyUMask(umask);
          
      EnumSet<CreateFlag> flag = (createMode != Nfs3Constant.CREATE_EXCLUSIVE) ? EnumSet
          .of(CreateFlag.CREATE, CreateFlag.OVERWRITE) : EnumSet
          .of(CreateFlag.CREATE);
      fos = new FSDataOutputStream(dfsClient.create(fileIdPath, permission,
          flag, false, replication, blockSize, null, bufferSize, null),
          statistics);
      
      if ((createMode == Nfs3Constant.CREATE_UNCHECKED)
          || (createMode == Nfs3Constant.CREATE_GUARDED)) {
        // Set group if it's not specified in the request.
        if (!setAttr3.getUpdateFields().contains(SetAttrField.GID)) {
          setAttr3.getUpdateFields().add(SetAttrField.GID);
          setAttr3.setGid(authSys.getGid());
        }
        setattrInternal(dfsClient, fileIdPath, setAttr3, false);
      }

      postOpObjAttr = Nfs3Utils.getFileAttr(dfsClient, fileIdPath, iug);
      dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient, dirFileIdPath,
          iug);
    } catch (IOException e) {
      LOG.error("Exception", e);
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e1) {
          LOG.error("Can't close stream for dirFileId:" + dirHandle.getFileId()
              + " filename: " + fileName);
        }
      }
      if (dirWcc == null) {
        try {
          dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
              dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.error("Can't get postOpDirAttr for dirFileId:"
              + dirHandle.getFileId());
        }
      }
      if (e instanceof AccessControlException) {
        return new CREATE3Response(Nfs3Status.NFS3ERR_ACCES, fileHandle,
            postOpObjAttr, dirWcc);
      } else {
        return new CREATE3Response(Nfs3Status.NFS3ERR_IO, fileHandle,
            postOpObjAttr, dirWcc);
      }
    }
    
    // Add open stream
    OpenFileCtx openFileCtx = new OpenFileCtx(fos, postOpObjAttr, writeDumpDir
        + "/" + postOpObjAttr.getFileId());
    fileHandle = new FileHandle(postOpObjAttr.getFileId());
    writeManager.addOpenFileStream(fileHandle, openFileCtx);
    if (LOG.isDebugEnabled()) {
      LOG.debug("open stream for file:" + fileName + ", fileId:"
          + fileHandle.getFileId());
    }
    
    return new CREATE3Response(Nfs3Status.NFS3_OK, fileHandle, postOpObjAttr,
        dirWcc);
  }

  public MKDIR3Response mkdir(XDR xdr, RpcAuthSys authSys) {
    MKDIR3Response response = new MKDIR3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    MKDIR3Request request = null;

    try {
      request = new MKDIR3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid MKDIR request");
      return new MKDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();

    if (request.getObjAttr().getUpdateFields().contains(SetAttrField.SIZE)) {
      LOG.error("Setting file size is not supported when mkdir: " + fileName
          + " in dirHandle" + dirHandle);
      return new MKDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    WccAttr preOpDirAttr = null;
    Nfs3FileAttributes postOpDirAttr = null;
    Nfs3FileAttributes postOpObjAttr = null;
    FileHandle objFileHandle = null;
    try {
      preOpDirAttr = Nfs3Utils.getWccAttr(dfsClient, dirFileIdPath);
      if (preOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId:" + dirHandle.getFileId());
        return new MKDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }

      final String fileIdPath = dirFileIdPath + "/" + fileName;
      SetAttr3 setAttr3 = request.getObjAttr();
      FsPermission permission = setAttr3.getUpdateFields().contains(
          SetAttrField.MODE) ? new FsPermission((short) setAttr3.getMode())
          : FsPermission.getDefault().applyUMask(umask);

      if (!dfsClient.mkdirs(fileIdPath, permission, false)) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new MKDIR3Response(Nfs3Status.NFS3ERR_IO, null, null, dirWcc);
      }

      // Set group if it's not specified in the request.
      if (!setAttr3.getUpdateFields().contains(SetAttrField.GID)) {
        setAttr3.getUpdateFields().add(SetAttrField.GID);
        setAttr3.setGid(authSys.getGid());
      }
      setattrInternal(dfsClient, fileIdPath, setAttr3, false);
      
      postOpObjAttr = Nfs3Utils.getFileAttr(dfsClient, fileIdPath, iug);
      objFileHandle = new FileHandle(postOpObjAttr.getFileId());
      WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
          dirFileIdPath, iug);
      return new MKDIR3Response(Nfs3Status.NFS3_OK, new FileHandle(
          postOpObjAttr.getFileId()), postOpObjAttr, dirWcc);
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      // Try to return correct WccData
      if (postOpDirAttr == null) {
        try {
          postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.info("Can't get postOpDirAttr for " + dirFileIdPath);
        }
      }
      WccData dirWcc = new WccData(preOpDirAttr, postOpDirAttr);
      if (e instanceof AccessControlException) {
        return new MKDIR3Response(Nfs3Status.NFS3ERR_PERM, objFileHandle,
            postOpObjAttr, dirWcc);
      } else {
        return new MKDIR3Response(Nfs3Status.NFS3ERR_IO, objFileHandle,
            postOpObjAttr, dirWcc);
      }
    }
  }


  public READDIR3Response mknod(XDR xdr, RpcAuthSys authSys) {
    return new READDIR3Response(Nfs3Status.NFS3ERR_NOTSUPP);
  }
  
  public REMOVE3Response remove(XDR xdr, RpcAuthSys authSys) {
    REMOVE3Response response = new REMOVE3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    REMOVE3Request request = null;
    try {
      request = new REMOVE3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid REMOVE request");
      return new REMOVE3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS REMOVE dir fileId: " + dirHandle.getFileId()
          + " fileName: " + fileName);
    }

    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    WccAttr preOpDirAttr = null;
    Nfs3FileAttributes postOpDirAttr = null;
    try {
      preOpDirAttr = Nfs3Utils.getWccAttr(dfsClient, dirFileIdPath);
      if (preOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId:" + dirHandle.getFileId());
        return new REMOVE3Response(Nfs3Status.NFS3ERR_STALE);
      }

      String fileIdPath = dirFileIdPath + "/" + fileName;
      HdfsFileStatus fstat = Nfs3Utils.getFileStatus(dfsClient,
          fileIdPath);
      if (fstat == null) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new REMOVE3Response(Nfs3Status.NFS3ERR_NOENT, dirWcc);
      }
      if (fstat.isDir()) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new REMOVE3Response(Nfs3Status.NFS3ERR_ISDIR, dirWcc);
      }

      if (dfsClient.delete(fileIdPath, false) == false) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new REMOVE3Response(Nfs3Status.NFS3ERR_ACCES, dirWcc);
      }

      WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
          dirFileIdPath, iug);
      return new REMOVE3Response(Nfs3Status.NFS3_OK, dirWcc);
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      // Try to return correct WccData
      if (postOpDirAttr == null) {
        try {
          postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.info("Can't get postOpDirAttr for " + dirFileIdPath);
        }
      }
      WccData dirWcc = new WccData(preOpDirAttr, postOpDirAttr);
      if (e instanceof AccessControlException) {
        return new REMOVE3Response(Nfs3Status.NFS3ERR_PERM, dirWcc);
      } else {
        return new REMOVE3Response(Nfs3Status.NFS3ERR_IO, dirWcc);
      }
    }
  }

  public RMDIR3Response rmdir(XDR xdr, RpcAuthSys authSys) {
    RMDIR3Response response = new RMDIR3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    RMDIR3Request request = null;
    try {
      request = new RMDIR3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid RMDIR request");
      return new RMDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle dirHandle = request.getHandle();
    String fileName = request.getName();

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS RMDIR dir fileId: " + dirHandle.getFileId()
          + " fileName: " + fileName);
    }

    String dirFileIdPath = Nfs3Utils.getFileIdPath(dirHandle);
    WccAttr preOpDirAttr = null;
    Nfs3FileAttributes postOpDirAttr = null;
    try {
      preOpDirAttr = Nfs3Utils.getWccAttr(dfsClient, dirFileIdPath);
      if (preOpDirAttr == null) {
        LOG.info("Can't get path for dir fileId:" + dirHandle.getFileId());
        return new RMDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }

      String fileIdPath = dirFileIdPath + "/" + fileName;
      HdfsFileStatus fstat = Nfs3Utils.getFileStatus(dfsClient,
          fileIdPath);
      if (fstat == null) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOENT, dirWcc);
      }
      if (!fstat.isDir()) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOTDIR, dirWcc);
      }
      
      if (fstat.getChildrenNum() > 0) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new RMDIR3Response(Nfs3Status.NFS3ERR_NOTEMPTY, dirWcc);
      }

      if (dfsClient.delete(fileIdPath, false) == false) {
        WccData dirWcc = Nfs3Utils.createWccData(preOpDirAttr, dfsClient,
            dirFileIdPath, iug);
        return new RMDIR3Response(Nfs3Status.NFS3ERR_ACCES, dirWcc);
      }

      postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      WccData wccData = new WccData(preOpDirAttr, postOpDirAttr);
      return new RMDIR3Response(Nfs3Status.NFS3_OK, wccData);

    } catch (IOException e) {
      LOG.warn("Exception ", e);
      // Try to return correct WccData
      if (postOpDirAttr == null) {
        try {
          postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
        } catch (IOException e1) {
          LOG.info("Can't get postOpDirAttr for " + dirFileIdPath);
        }
      }
      WccData dirWcc = new WccData(preOpDirAttr, postOpDirAttr);
      if (e instanceof AccessControlException) {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_PERM, dirWcc);
      } else {
        return new RMDIR3Response(Nfs3Status.NFS3ERR_IO, dirWcc);
      }
    }
  }

  public RENAME3Response rename(XDR xdr, RpcAuthSys authSys) {
    RENAME3Response response = new RENAME3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    RENAME3Request request = null;
    try {
      request = new RENAME3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid RENAME request");
      return new RENAME3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle fromHandle = request.getFromDirHandle();
    String fromName = request.getFromName();
    FileHandle toHandle = request.getToDirHandle();
    String toName = request.getToName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS RENAME from: " + fromHandle.getFileId() + "/" + fromName
          + " to: " + toHandle.getFileId() + "/" + toName);
    }

    String fromDirFileIdPath = Nfs3Utils.getFileIdPath(fromHandle);
    String toDirFileIdPath = Nfs3Utils.getFileIdPath(toHandle);
    WccAttr fromPreOpAttr = null;
    WccAttr toPreOpAttr = null;
    WccData fromDirWcc = null;
    WccData toDirWcc = null;
    try {
      fromPreOpAttr = Nfs3Utils.getWccAttr(dfsClient, fromDirFileIdPath);
      if (fromPreOpAttr == null) {
        LOG.info("Can't get path for fromHandle fileId:"
            + fromHandle.getFileId());
        return new RENAME3Response(Nfs3Status.NFS3ERR_STALE);
      }

      toPreOpAttr = Nfs3Utils.getWccAttr(dfsClient, toDirFileIdPath);
      if (toPreOpAttr == null) {
        LOG.info("Can't get path for toHandle fileId:" + toHandle.getFileId());
        return new RENAME3Response(Nfs3Status.NFS3ERR_STALE);
      }

      String src = fromDirFileIdPath + "/" + fromName;
      String dst = toDirFileIdPath + "/" + toName;

      dfsClient.rename(src, dst, Options.Rename.NONE);

      // Assemble the reply
      fromDirWcc = Nfs3Utils.createWccData(fromPreOpAttr, dfsClient,
          fromDirFileIdPath, iug);
      toDirWcc = Nfs3Utils.createWccData(toPreOpAttr, dfsClient,
          toDirFileIdPath, iug);
      return new RENAME3Response(Nfs3Status.NFS3_OK, fromDirWcc, toDirWcc);

    } catch (IOException e) {
      LOG.warn("Exception ", e);
      // Try to return correct WccData      
      try {
        fromDirWcc = Nfs3Utils.createWccData(fromPreOpAttr, dfsClient,
            fromDirFileIdPath, iug);
        toDirWcc = Nfs3Utils.createWccData(toPreOpAttr, dfsClient,
            toDirFileIdPath, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpDirAttr for " + fromDirFileIdPath + " or"
            + toDirFileIdPath);
      }
      if (e instanceof AccessControlException) {
        return new RENAME3Response(Nfs3Status.NFS3ERR_PERM, fromDirWcc,
            toDirWcc);
      } else {
        return new RENAME3Response(Nfs3Status.NFS3ERR_IO, fromDirWcc, toDirWcc);
      }
    }
  }

  public SYMLINK3Response symlink(XDR xdr, RpcAuthSys authSys) {
    return new SYMLINK3Response(Nfs3Status.NFS3ERR_NOTSUPP);
  }

  public READDIR3Response link(XDR xdr, RpcAuthSys authSys) {
    return new READDIR3Response(Nfs3Status.NFS3ERR_NOTSUPP);
  }

  public READDIR3Response readdir(XDR xdr, RpcAuthSys authSys) {
    READDIR3Response response = new READDIR3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    READDIR3Request request = null;
    try {
      request = new READDIR3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid READDIR request");
      return new READDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    FileHandle handle = request.getHandle();
    long cookie = request.getCookie();
    if (cookie < 0) {
      LOG.error("Invalid READDIR request, with negitve cookie:" + cookie);
      return new READDIR3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    long count = request.getCount();
    if (count <= 0) {
      LOG.info("Nonpositive count in invalid READDIR request:" + count);
      return new READDIR3Response(Nfs3Status.NFS3_OK);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS READDIR fileId: " + handle.getFileId() + " cookie: "
          + cookie + " count: " + count);
    }

    HdfsFileStatus dirStatus;
    DirectoryListing dlisting = null;
    Nfs3FileAttributes postOpAttr = null;
    long dotdotFileId = 0;
    try {
      String dirFileIdPath = Nfs3Utils.getFileIdPath(handle);
      dirStatus = dfsClient.getFileInfo(dirFileIdPath);
      if (dirStatus == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        return new READDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }
      if (!dirStatus.isDir()) {
        LOG.error("Can't readdir for regular file, fileId:"
            + handle.getFileId());
        return new READDIR3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
      long cookieVerf = request.getCookieVerf();
      if ((cookieVerf != 0) && (cookieVerf != dirStatus.getModificationTime())) {
        LOG.error("CookierVerf mismatch. request cookierVerf:" + cookieVerf
            + " dir cookieVerf:" + dirStatus.getModificationTime());
        return new READDIR3Response(Nfs3Status.NFS3ERR_BAD_COOKIE);
      }

      if (cookie == 0) {
        // Get dotdot fileId
        String dotdotFileIdPath = dirFileIdPath + "/..";
        HdfsFileStatus dotdotStatus = dfsClient.getFileInfo(dotdotFileIdPath);

        if (dotdotStatus == null) {
          // This should not happen
          throw new IOException("Can't get path for handle path:"
              + dotdotFileIdPath);
        }
        dotdotFileId = dotdotStatus.getFileId();
      }

      // Get the list from the resume point
      byte[] startAfter;
      if(cookie == 0 ) {
        startAfter = HdfsFileStatus.EMPTY_NAME;
      } else {
        String inodeIdPath = Nfs3Utils.getFileIdPath(cookie);
        startAfter = inodeIdPath.getBytes();
      }
      dlisting = dfsClient.listPaths(dirFileIdPath, startAfter);

      postOpAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (postOpAttr == null) {
        LOG.error("Can't get path for fileId:" + handle.getFileId());
        return new READDIR3Response(Nfs3Status.NFS3ERR_STALE);
      }
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      return new READDIR3Response(Nfs3Status.NFS3ERR_IO);
    }

    /**
     * Set up the dirents in the response. fileId is used as the cookie with one
     * exception. Linux client can either be stuck with "ls" command (on REHL)
     * or report "Too many levels of symbolic links" (Ubuntu).
     * 
     * The problem is that, only two items returned, "." and ".." when the
     * namespace is empty. Both of them are "/" with the same cookie(root
     * fileId). Linux client doesn't think such a directory is a real directory.
     * Even though NFS protocol specifies cookie is an opaque data, Linux client
     * somehow doesn't like an empty dir returns same cookie for both "." and
     * "..".
     * 
     * The workaround is to use 0 as the cookie for "." and always return "." as
     * the first entry in readdir/readdirplus response.
     */
    HdfsFileStatus[] fstatus = dlisting.getPartialListing();    
    int n = (int) Math.min(fstatus.length, count-2);
    boolean eof = (n < fstatus.length) ? false : (dlisting
        .getRemainingEntries() == 0);
    
    Entry3[] entries;
    if (cookie == 0) {
      entries = new Entry3[n + 2];
      entries[0] = new READDIR3Response.Entry3(postOpAttr.getFileId(), ".", 0);
      entries[1] = new READDIR3Response.Entry3(dotdotFileId, "..", dotdotFileId);

      for (int i = 2; i < n + 2; i++) {
        entries[i] = new READDIR3Response.Entry3(fstatus[i - 2].getFileId(),
            fstatus[i - 2].getLocalName(), fstatus[i - 2].getFileId());
      }
    } else {
      // Resume from last readdirplus. If the cookie is "..", the result
      // list is up the directory content since HDFS uses name as resume point.    
      entries = new Entry3[n];    
      for (int i = 0; i < n; i++) {
        entries[i] = new READDIR3Response.Entry3(fstatus[i].getFileId(),
            fstatus[i].getLocalName(), fstatus[i].getFileId());
      }
    }
    
    DirList3 dirList = new READDIR3Response.DirList3(entries, eof);
    return new READDIR3Response(Nfs3Status.NFS3_OK, postOpAttr,
        dirStatus.getModificationTime(), dirList);
  }

  public READDIRPLUS3Response readdirplus(XDR xdr, RpcAuthSys authSys) {   
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_SERVERFAULT);
    }
    
    READDIRPLUS3Request request = null;
    try {
      request = new READDIRPLUS3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid READDIRPLUS request");
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    long cookie = request.getCookie();
    if (cookie < 0) {
      LOG.error("Invalid READDIRPLUS request, with negitve cookie:" + cookie);
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_INVAL);
    }
    long dirCount = request.getDirCount();
    if (dirCount <= 0) {
      LOG.info("Nonpositive count in invalid READDIRPLUS request:" + dirCount);
      return new READDIRPLUS3Response(Nfs3Status.NFS3_OK);
    }
    int maxCount = request.getMaxCount();

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS READDIRPLUS fileId: " + handle.getFileId() + " cookie: "
          + cookie + " dirCount: " + dirCount + " maxCount: " + maxCount);
    }

    HdfsFileStatus dirStatus;
    DirectoryListing dlisting = null;
    Nfs3FileAttributes postOpDirAttr = null;
    long dotdotFileId = 0;
    try {
      String dirFileIdPath = Nfs3Utils.getFileIdPath(handle);
      dirStatus = dfsClient.getFileInfo(dirFileIdPath);
      if (dirStatus == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_STALE);
      }
      if (!dirStatus.isDir()) {
        LOG.error("Can't readdirplus for regular file, fileId:"
            + handle.getFileId());
        return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_NOTDIR);
      }
      long cookieVerf = request.getCookieVerf();
      if ((cookieVerf != 0) && (cookieVerf != dirStatus.getModificationTime())) {
        LOG.error("CookierVerf mismatch. request cookierVerf:" + cookieVerf
            + " dir cookieVerf:" + dirStatus.getModificationTime());
        return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_BAD_COOKIE);
      }

      if (cookie == 0) {
        // Get dotdot fileId
        String dotdotFileIdPath = dirFileIdPath + "/..";
        HdfsFileStatus dotdotStatus = dfsClient.getFileInfo(dotdotFileIdPath);

        if (dotdotStatus == null) {
          // This should not happen
          throw new IOException("Can't get path for handle path:"
              + dotdotFileIdPath);
        }
        dotdotFileId = dotdotStatus.getFileId();
      }

      // Get the list from the resume point
      byte[] startAfter;
      if (cookie == 0) {
        startAfter = HdfsFileStatus.EMPTY_NAME;
      } else {
        String inodeIdPath = Nfs3Utils.getFileIdPath(cookie);
        startAfter = inodeIdPath.getBytes();
      }
      dlisting = dfsClient.listPaths(dirFileIdPath, startAfter);

      postOpDirAttr = Nfs3Utils.getFileAttr(dfsClient, dirFileIdPath, iug);
      if (postOpDirAttr == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_STALE);
      }
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      return new READDIRPLUS3Response(Nfs3Status.NFS3ERR_IO);
    }
    
    // Set up the dirents in the response
    HdfsFileStatus[] fstatus = dlisting.getPartialListing();
    int n = (int) Math.min(fstatus.length, dirCount-2);
    boolean eof = (n < fstatus.length) ? false : (dlisting
        .getRemainingEntries() == 0);
    
    READDIRPLUS3Response.EntryPlus3[] entries;
    if (cookie == 0) {
      entries = new READDIRPLUS3Response.EntryPlus3[n+2];
      
      entries[0] = new READDIRPLUS3Response.EntryPlus3(
          postOpDirAttr.getFileId(), ".", 0, postOpDirAttr, new FileHandle(
              postOpDirAttr.getFileid()));
      entries[1] = new READDIRPLUS3Response.EntryPlus3(dotdotFileId, "..",
          dotdotFileId, postOpDirAttr, new FileHandle(dotdotFileId));

      for (int i = 2; i < n + 2; i++) {
        long fileId = fstatus[i - 2].getFileId();
        FileHandle childHandle = new FileHandle(fileId);
        Nfs3FileAttributes attr;
        try {
          attr = writeManager.getFileAttr(dfsClient, childHandle, iug);
        } catch (IOException e) {
          LOG.error("Can't get file attributes for fileId:" + fileId
              + " error:" + e);
          continue;
        }
        entries[i] = new READDIRPLUS3Response.EntryPlus3(fileId,
            fstatus[i - 2].getLocalName(), fileId, attr, childHandle);
      }
    } else {
      // Resume from last readdirplus. If the cookie is "..", the result
      // list is up the directory content since HDFS uses name as resume point.
      entries = new READDIRPLUS3Response.EntryPlus3[n]; 
      for (int i = 0; i < n; i++) {
        long fileId = fstatus[i].getFileId();
        FileHandle childHandle = new FileHandle(fileId);
        Nfs3FileAttributes attr;
        try {
          attr = writeManager.getFileAttr(dfsClient, childHandle, iug);
        } catch (IOException e) {
          LOG.error("Can't get file attributes for fileId:" + fileId
              + " error:" + e);
          continue;
        }
        entries[i] = new READDIRPLUS3Response.EntryPlus3(fileId,
            fstatus[i].getLocalName(), fileId, attr, childHandle);
      }
    }

    DirListPlus3 dirListPlus = new READDIRPLUS3Response.DirListPlus3(entries,
        eof);
    return new READDIRPLUS3Response(Nfs3Status.NFS3_OK, postOpDirAttr,
        dirStatus.getModificationTime(), dirListPlus);
  }
  
  public FSSTAT3Response fsstat(XDR xdr, RpcAuthSys authSys) {
    FSSTAT3Response response = new FSSTAT3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    FSSTAT3Request request = null;
    try {
      request = new FSSTAT3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid FSSTAT request");
      return new FSSTAT3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS FSSTAT fileId: " + handle.getFileId());
    }

    try {
      // Use superUserClient to get file system status
      FsStatus fsStatus = superUserClient.getDiskStatus();
      long totalBytes = fsStatus.getCapacity();
      long freeBytes = fsStatus.getRemaining();
      
      Nfs3FileAttributes attrs = writeManager.getFileAttr(dfsClient, handle,
          iug);
      if (attrs == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        return new FSSTAT3Response(Nfs3Status.NFS3ERR_STALE);
      }
      
      long maxFsObjects = config.getLong("dfs.max.objects", 0);
      if (maxFsObjects == 0) {
        // A value of zero in HDFS indicates no limit to the number
        // of objects that dfs supports. Using Integer.MAX_VALUE instead of
        // Long.MAX_VALUE so 32bit client won't complain.
        maxFsObjects = Integer.MAX_VALUE;
      }
      
      return new FSSTAT3Response(Nfs3Status.NFS3_OK, attrs, totalBytes,
          freeBytes, freeBytes, maxFsObjects, maxFsObjects, maxFsObjects, 0);
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      return new FSSTAT3Response(Nfs3Status.NFS3ERR_IO);
    }
  }

  public FSINFO3Response fsinfo(XDR xdr, RpcAuthSys authSys) {
    FSINFO3Response response = new FSINFO3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    FSINFO3Request request = null;
    try {
      request = new FSINFO3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid FSINFO request");
      return new FSINFO3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS FSINFO fileId: " + handle.getFileId());
    }

    try {
      int rtmax = MAX_READ_TRANSFER_SIZE;
      int wtmax = MAX_WRITE_TRANSFER_SIZE;
      int dtperf = MAX_READDIR_TRANSFER_SIZE;

      Nfs3FileAttributes attrs = Nfs3Utils.getFileAttr(dfsClient,
          Nfs3Utils.getFileIdPath(handle), iug);
      if (attrs == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        return new FSINFO3Response(Nfs3Status.NFS3ERR_STALE);
      }
      
      int fsProperty = Nfs3Constant.FSF3_CANSETTIME
          | Nfs3Constant.FSF3_HOMOGENEOUS;

      return new FSINFO3Response(Nfs3Status.NFS3_OK, attrs, rtmax, rtmax, 1,
          wtmax, wtmax, 1, dtperf, Long.MAX_VALUE, new NfsTime(1), fsProperty);
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      return new FSINFO3Response(Nfs3Status.NFS3ERR_IO);
    }
  }

  public PATHCONF3Response pathconf(XDR xdr, RpcAuthSys authSys) {
    PATHCONF3Response response = new PATHCONF3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    PATHCONF3Request request = null;
    try {
      request = new PATHCONF3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid PATHCONF request");
      return new PATHCONF3Response(Nfs3Status.NFS3ERR_INVAL);
    }

    FileHandle handle = request.getHandle();
    Nfs3FileAttributes attrs;

    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS PATHCONF fileId: " + handle.getFileId());
    }

    try {
      attrs = Nfs3Utils.getFileAttr(dfsClient, Nfs3Utils.getFileIdPath(handle),
          iug);
      if (attrs == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        return new PATHCONF3Response(Nfs3Status.NFS3ERR_STALE);
      }

      return new PATHCONF3Response(Nfs3Status.NFS3_OK, attrs, 0,
          HdfsConstants.MAX_PATH_LENGTH, true, false, false, true);
    } catch (IOException e) {
      LOG.warn("Exception ", e);
      return new PATHCONF3Response(Nfs3Status.NFS3ERR_IO);
    }
  }

  public COMMIT3Response commit(XDR xdr, RpcAuthSys authSys) {
    COMMIT3Response response = new COMMIT3Response(Nfs3Status.NFS3_OK);
    String uname = authSysCheck(authSys);
    DFSClient dfsClient = clientCache.get(uname);
    if (dfsClient == null) {
      response.setStatus(Nfs3Status.NFS3ERR_SERVERFAULT);
      return response;
    }
    
    COMMIT3Request request = null;
    try {
      request = new COMMIT3Request(xdr);
    } catch (IOException e) {
      LOG.error("Invalid COMMIT request");
      response.setStatus(Nfs3Status.NFS3ERR_INVAL);
      return response;
    }

    FileHandle handle = request.getHandle();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NFS COMMIT fileId: " + handle.getFileId() + " offset="
          + request.getOffset() + " count=" + request.getCount());
    }

    String fileIdPath = Nfs3Utils.getFileIdPath(handle);
    WccAttr preOpAttr = null;
    try {
      preOpAttr = Nfs3Utils.getWccAttr(dfsClient, fileIdPath);
      if (preOpAttr == null) {
        LOG.info("Can't get path for fileId:" + handle.getFileId());
        return new COMMIT3Response(Nfs3Status.NFS3ERR_STALE);
      }
      long commitOffset = (request.getCount() == 0) ? 0
          : (request.getOffset() + request.getCount());
      
      int status;
      if (writeManager.handleCommit(handle, commitOffset)) {
        status = Nfs3Status.NFS3_OK;
      } else {
        status = Nfs3Status.NFS3ERR_IO;
      }
      Nfs3FileAttributes postOpAttr = writeManager.getFileAttr(dfsClient,
          handle, iug);
      WccData fileWcc = new WccData(preOpAttr, postOpAttr);
      return new COMMIT3Response(status, fileWcc,
          Nfs3Constant.WRITE_COMMIT_VERF);

    } catch (IOException e) {
      LOG.warn("Exception ", e);
      Nfs3FileAttributes postOpAttr = null;
      try {
        postOpAttr = writeManager.getFileAttr(dfsClient, handle, iug);
      } catch (IOException e1) {
        LOG.info("Can't get postOpAttr for fileId: " + handle.getFileId());
      }
      WccData fileWcc = new WccData(preOpAttr, postOpAttr);
      return new COMMIT3Response(Nfs3Status.NFS3ERR_IO, fileWcc,
          Nfs3Constant.WRITE_COMMIT_VERF);
    }
  }
  
  private final static String UNKNOWN_USER = "nobody";
  private final static String UNKNOWN_GROUP = "nobody";

  private String authSysCheck(RpcAuthSys authSys) {
    return iug.getUserName(authSys.getUid(), UNKNOWN_USER);
  }
  
  @Override
  public XDR handleInternal(RpcCall rpcCall, final XDR xdr, XDR out,
      InetAddress client, Channel channel) {
    final NFSPROC3 nfsproc3 = NFSPROC3.fromValue(rpcCall.getProcedure());
    int xid = rpcCall.getXid();
    RpcAuthSys authSys = null;
    
    // Ignore auth only for NFSPROC3_NULL, especially for Linux clients.
    if (nfsproc3 != NFSPROC3.NULL) {
      if (rpcCall.getCredential().getFlavor() != AuthFlavor.AUTH_SYS) {
        LOG.info("Wrong RPC AUTH flavor, "
            + rpcCall.getCredential().getFlavor() + " is not AUTH_SYS.");
        XDR reply = new XDR();
        reply = RpcDeniedReply.voidReply(reply, xid,
            RpcReply.ReplyState.MSG_ACCEPTED,
            RpcDeniedReply.RejectState.AUTH_ERROR);
        return reply;
      }
      authSys = RpcAuthSys.from(rpcCall.getCredential().getBody());
    }
    
    NFS3Response response = null;
    if (nfsproc3 == NFSPROC3.NULL) {
      response = nullProcedure();
    } else if (nfsproc3 == NFSPROC3.GETATTR) {
      response = getattr(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.SETATTR) {
      response = setattr(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.LOOKUP) {
      response = lookup(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.ACCESS) {
      response = access(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.READLINK) {
      response = readlink(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.READ) {
      response = read(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.WRITE) {
      response = write(xdr, channel, xid, authSys);
    } else if (nfsproc3 == NFSPROC3.CREATE) {
      response = create(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.MKDIR) {      
      response = mkdir(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.SYMLINK) {
      response = symlink(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.MKNOD) {
      response = mknod(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.REMOVE) {
      response = remove(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.RMDIR) {
      response = rmdir(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.RENAME) {
      response = rename(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.LINK) {
      response = link(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.READDIR) {
      response = readdir(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.READDIRPLUS) {
      response = readdirplus(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.FSSTAT) {
      response = fsstat(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.FSINFO) {
      response = fsinfo(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.PATHCONF) {
      response = pathconf(xdr, authSys);
    } else if (nfsproc3 == NFSPROC3.COMMIT) {
      response = commit(xdr, authSys);
    } else {
      // Invalid procedure
      RpcAcceptedReply.voidReply(out, xid,
          RpcAcceptedReply.AcceptState.PROC_UNAVAIL);
    }
    if (response != null) {
      out = response.send(out, xid);
    }
    return out;
  }
  
  @Override
  protected boolean isIdempotent(RpcCall call) {
    final NFSPROC3 nfsproc3 = NFSPROC3.fromValue(call.getProcedure()); 
    return nfsproc3 == null || nfsproc3.isIdempotent();
  }
}

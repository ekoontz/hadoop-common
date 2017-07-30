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
package org.apache.hadoop.hdfs.nfs.mount;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.mount.MountEntry;
import org.apache.hadoop.mount.MountInterface;
import org.apache.hadoop.mount.MountResponse;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.XDR;
import org.jboss.netty.channel.Channel;

/**
 * RPC program corresponding to mountd daemon. See {@link Mountd}.
 */
public class RpcProgramMountd extends RpcProgram implements MountInterface {
  private static final Log LOG = LogFactory.getLog(RpcProgramMountd.class);
  public static final int PROGRAM = 100005;
  public static final int VERSION_1 = 1;
  public static final int VERSION_2 = 2;
  public static final int VERSION_3 = 3;
  public static final int PORT = 4242;

  // Need DFSClient for branch-1 to get ExtendedHdfsFileStatus
  private final DFSClient dfsClient;
  
  /** Synchronized list */
  private final List<MountEntry> mounts;
  
  /** List that is unmodifiable */
  private final List<String> exports;

  public RpcProgramMountd() throws IOException {
    this(new ArrayList<String>(0));
  }

  public RpcProgramMountd(List<String> exports) throws IOException {
    this(exports, new Configuration());
  }

  public RpcProgramMountd(List<String> exports, Configuration config)
      throws IOException {
    // Note that RPC cache is not enabled
    super("mountd", "localhost", PORT, PROGRAM, VERSION_1, VERSION_3, 0);
    this.mounts = Collections.synchronizedList(new ArrayList<MountEntry>());
    this.exports = Collections.unmodifiableList(exports);
    this.dfsClient = new DFSClient(NameNode.getAddress(config), config);
  }
  
  public XDR nullOp(XDR out, int xid, InetAddress client) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT NULLOP : " + " client: " + client);
    }
    return  RpcAcceptedReply.voidReply(out, xid);
  }

  public XDR mnt(XDR xdr, XDR out, int xid, InetAddress client) {
    String path = xdr.readString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT MNT path: " + path + " client: " + client);
    }

    String host = client.getHostName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got host: " + host + " path: " + path);
    }
    if (!exports.contains(path)) {
      LOG.info("Path " + path + " is not shared.");
      MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_NOENT, out, xid, null);
      return out;
    }

    FileHandle handle = null;
    try {
      HdfsFileStatus exFileStatus = dfsClient.getFileInfo(path);
      
      handle = new FileHandle(exFileStatus.getFileId());
    } catch (IOException e) {
      LOG.error("Can't get handle for export:" + path + ", exception:" + e);
      MountResponse.writeMNTResponse(Nfs3Status.NFS3ERR_NOENT, out, xid, null);
      return out;
    }

    assert (handle != null);
    LOG.info("Giving handle (fileId:" + handle.getFileId()
        + ") to client for export " + path);
    mounts.add(new MountEntry(host, path));

    MountResponse.writeMNTResponse(Nfs3Status.NFS3_OK, out, xid,
        handle.getContent());
    return out;
  }

  public XDR dump(XDR out, int xid, InetAddress client) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT NULLOP : " + " client: " + client);
    }

    List<MountEntry> copy = new ArrayList<MountEntry>(mounts);
    MountResponse.writeMountList(out, xid, copy);
    return out;
  }

  public XDR umnt(XDR xdr, XDR out, int xid, InetAddress client) {
    String path = xdr.readString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT UMNT path: " + path + " client: " + client);
    }
    
    String host = client.getHostName();
    mounts.remove(new MountEntry(host, path));
    RpcAcceptedReply.voidReply(out, xid);
    return out;
  }

  public XDR umntall(XDR out, int xid, InetAddress client) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MOUNT UMNTALL : " + " client: " + client);
    }
    mounts.clear();
    return RpcAcceptedReply.voidReply(out, xid);
  }

  @Override
  public XDR handleInternal(RpcCall rpcCall, XDR xdr, XDR out,
      InetAddress client, Channel channel) {
    final MNTPROC mntproc = MNTPROC.fromValue(rpcCall.getProcedure());
    int xid = rpcCall.getXid();
    if (mntproc == MNTPROC.NULL) {
      out = nullOp(out, xid, client);
    } else if (mntproc == MNTPROC.MNT) {
      out = mnt(xdr, out, xid, client);
    } else if (mntproc == MNTPROC.DUMP) {
      out = dump(out, xid, client);
    } else if (mntproc == MNTPROC.UMNT) {      
      out = umnt(xdr, out, xid, client);
    } else if (mntproc == MNTPROC.UMNTALL) {
      umntall(out, xid, client);
    } else if (mntproc == MNTPROC.EXPORT) {
      out = MountResponse.writeExportList(out, xid, exports);
    } else {
      // Invalid procedure
      RpcAcceptedReply.voidReply(out, xid,
          RpcAcceptedReply.AcceptState.PROC_UNAVAIL);
    }  
    return out;
  }
  
  @Override
  protected boolean isIdempotent(RpcCall call) {
    // Not required, because cache is turned off
    return false;
  }
}

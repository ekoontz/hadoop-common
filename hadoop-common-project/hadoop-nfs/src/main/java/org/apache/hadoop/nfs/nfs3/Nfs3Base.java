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
package org.apache.hadoop.nfs.nfs3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mount.MountdBase;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.SimpleTcpServer;
import org.apache.hadoop.portmap.PortmapMapping;

/**
 * Nfs server. Supports NFS v3 using {@link RpcProgram}.
 * Currently Mountd program is also started inside this class.
 * Only TCP server is supported and UDP is not supported.
 */
public abstract class Nfs3Base {
  public static final Log LOG = LogFactory.getLog(Nfs3Base.class);
  private final MountdBase mountd;
  private final RpcProgram rpcProgram;
  private final int nfsPort;
  
  public MountdBase getMountBase() {
    return mountd;
  }
  
  public RpcProgram getRpcProgram() {
    return rpcProgram;
  }

  protected Nfs3Base(MountdBase mountd, RpcProgram program, Configuration conf) {
    this.mountd = mountd;
    this.rpcProgram = program;
    this.nfsPort = conf.getInt("nfs3.server.port", Nfs3Constant.PORT);
    LOG.info("NFS server port set to: "+nfsPort);
  }

  protected Nfs3Base(MountdBase mountd, RpcProgram program) {
    this.mountd = mountd;
    this.rpcProgram = program;
    this.nfsPort = Nfs3Constant.PORT;
  }

  public void start(boolean register) {
    mountd.start(register); // Start mountd
    startTCPServer(); // Start TCP server
    if (register) {
      rpcProgram.register(PortmapMapping.TRANSPORT_TCP);
    }
  }

  private void startTCPServer() {
    SimpleTcpServer tcpServer = new SimpleTcpServer(nfsPort,
        rpcProgram, 0);
    tcpServer.run();
  }
}

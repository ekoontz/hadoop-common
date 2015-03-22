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
package org.apache.hadoop.mount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.SimpleTcpServer;
import org.apache.hadoop.oncrpc.SimpleUdpServer;
import org.apache.hadoop.portmap.PortmapMapping;

/**
 * Main class for starting mountd daemon. This daemon implements the NFS
 * mount protocol. When receiving a MOUNT request from an NFS client, it checks
 * the request against the list of currently exported file systems. If the
 * client is permitted to mount the file system, rpc.mountd obtains a file
 * handle for requested directory and returns it to the client.
 */
abstract public class MountdBase {
  private final RpcProgram rpcProgram;

  public RpcProgram getRpcProgram() {
    return rpcProgram;
  }
  
  /**
   * Constructor
   * @param exports
   * @throws IOException 
   */
  public MountdBase(List<String> exports, RpcProgram program) throws IOException {
    rpcProgram = program;
  }

  /* Start UDP server */
  private void startUDPServer() {
    SimpleUdpServer udpServer = new SimpleUdpServer(rpcProgram.getPort(),
        rpcProgram, 1);
    udpServer.run();
  }

  /* Start TCP server */
  private void startTCPServer() {
    SimpleTcpServer tcpServer = new SimpleTcpServer(rpcProgram.getPort(),
        rpcProgram, 1);
    tcpServer.run();
  }

  public void start(boolean register) {
    startUDPServer();
    startTCPServer();
    if (register) {
      rpcProgram.register(PortmapMapping.TRANSPORT_UDP);
      rpcProgram.register(PortmapMapping.TRANSPORT_TCP);
    }
  }
}

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
package org.apache.hadoop.oncrpc;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * Simple UDP server implemented using netty.
 */
public class SimpleTcpServer {
  public static final Log LOG = LogFactory.getLog(SimpleTcpServer.class);
  protected final int port;
  protected final ChannelPipelineFactory pipelineFactory;
  protected final RpcProgram rpcProgram;
  
  /** The maximum number of I/O worker threads */
  protected final int workerCount;

  /**
   * @param port TCP port where to start the server at
   * @param program RPC program corresponding to the server
   * @param workercount Number of worker threads
   */
  public SimpleTcpServer(int port, RpcProgram program, int workercount) {
    this.port = port;
    this.rpcProgram = program;
    this.workerCount = workercount;
    this.pipelineFactory = getPipelineFactory();
  }

  public ChannelPipelineFactory getPipelineFactory() {
    return new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        return Channels.pipeline(new RpcFrameDecoder(),
            new SimpleTcpServerHandler(rpcProgram));
      }
    };
  }
  
  public void run() {
    // Configure the Server.
    ChannelFactory factory;
    if (workerCount == 0) {
      // Use default workers: 2 * the number of available processors
      factory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    } else {
      factory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(), Executors.newCachedThreadPool(),
          workerCount);
    }
    
    ServerBootstrap bootstrap = new ServerBootstrap(factory);
    bootstrap.setPipelineFactory(pipelineFactory);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", true);
    
    // Listen to TCP port
    bootstrap.bind(new InetSocketAddress(port));

    LOG.info("Started listening to TCP requests at port " + port + " for "
        + rpcProgram + " with workerCount " + workerCount);
  }
}

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

package org.apache.hadoop.hdfs.nfs;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.nfs.mount.RpcProgramMountd;
import org.apache.hadoop.hdfs.nfs.nfs3.Nfs3;
import org.apache.hadoop.hdfs.nfs.nfs3.RpcProgramNfs3;
import org.apache.hadoop.oncrpc.XDR;
import org.junit.Test;

public class TestMountd {

  public static final Log LOG = LogFactory.getLog(TestMountd.class);

  @Test
  public void testStart() throws IOException {
    // Start minicluster
    Configuration config = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
        .build();
    cluster.waitActive();
    
    // Use emphral port in case tests are running in parallel
    config.setInt("nfs3.mountd.port", 0);
    config.setInt("nfs3.server.port", 0);
    
    // Start nfs
    Nfs3 nfs3 = new Nfs3(config);
    nfs3.startServiceInternal(false);

    RpcProgramMountd mountd = (RpcProgramMountd) nfs3.getMountd()
        .getRpcProgram();
    mountd.nullOp(new XDR(), 1234, InetAddress.getByName("localhost"));
    
    RpcProgramNfs3 nfsd = (RpcProgramNfs3) nfs3.getRpcProgram();
    nfsd.nullProcedure();
    
    cluster.shutdown();
  }
}

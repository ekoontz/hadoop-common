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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mount.MountdBase;

/**
 * Main class for starting mountd daemon. This daemon implements the NFS
 * mount protocol. When receiving a MOUNT request from an NFS client, it checks
 * the request against the list of currently exported file systems. If the
 * client is permitted to mount the file system, rpc.mountd obtains a file
 * handle for requested directory and returns it to the client.
 */
public class Mountd extends MountdBase {

  public Mountd(Configuration config) throws IOException {
    super(new RpcProgramMountd(config));
  }
  
  public static void main(String[] args) throws IOException {
    Configuration config = new Configuration();
    Mountd mountd = new Mountd(config);
    mountd.start(true);
  }
}

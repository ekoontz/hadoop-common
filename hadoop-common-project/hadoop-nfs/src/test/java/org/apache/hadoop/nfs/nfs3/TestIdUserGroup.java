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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class TestIdUserGroup {

  @Test
  public void testDuplicates() throws IOException {
    String GET_ALL_USERS_CMD = "echo \"root:x:0:0:root:/root:/bin/bash\n"
        + "hdfs:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n"
        + "hdfs:x:11502:10788:Grid Distributed File System:/home/hdfs:/bin/bash\n"
        + "hdfs1:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n"
        + "hdfs2:x:11502:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n"
        + "bin:x:2:2:bin:/bin:/bin/sh\n"
        + "bin:x:1:1:bin:/bin:/sbin/nologin\n"
        + "daemon:x:1:1:daemon:/usr/sbin:/bin/sh\n"
        + "daemon:x:2:2:daemon:/sbin:/sbin/nologin\""
        + " | cut -d: -f1,3";
    String GET_ALL_GROUPS_CMD = "echo \"hdfs:*:11501:hrt_hdfs\n"
        + "mapred:x:497\n"
        + "mapred2:x:497\n"
        + "mapred:x:498\n" 
        + "mapred3:x:498\"" 
        + " | cut -d: -f1,3";
    // Maps for id to name map
    BiMap<Integer, String> uMap = HashBiMap.create();
    BiMap<Integer, String> gMap = HashBiMap.create();

    IdUserGroup.updateMapInternal(uMap, "user", GET_ALL_USERS_CMD, ":");
    assertTrue(uMap.size() == 5);
    assertEquals(uMap.get(0), "root");
    assertEquals(uMap.get(11501), "hdfs");
    assertEquals(uMap.get(11502), "hdfs2");
    assertEquals(uMap.get(2), "bin");
    assertEquals(uMap.get(1), "daemon");
    
    IdUserGroup.updateMapInternal(gMap, "group", GET_ALL_GROUPS_CMD, ":");
    assertTrue(gMap.size() == 3);
    assertEquals(gMap.get(11501), "hdfs");
    assertEquals(gMap.get(497), "mapred");
    assertEquals(gMap.get(498), "mapred3");    
  }
  
  @Test
  public void testIdOutOfIntegerRange() throws IOException {
    String GET_ALL_USERS_CMD = "echo \""
        + "nfsnobody:x:4294967294:4294967294:Anonymous NFS User:/var/lib/nfs:/sbin/nologin\n"
        + "nfsnobody1:x:4294967295:4294967295:Anonymous NFS User:/var/lib/nfs1:/sbin/nologin\n"
        + "maxint:x:2147483647:2147483647:Grid Distributed File System:/home/maxint:/bin/bash\n"
        + "minint:x:2147483648:2147483648:Grid Distributed File System:/home/minint:/bin/bash\n"
        + "archivebackup:*:1031:4294967294:Archive Backup:/home/users/archivebackup:/bin/sh\n"
        + "hdfs:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n"
        + "daemon:x:2:2:daemon:/sbin:/sbin/nologin\""
        + " | cut -d: -f1,3";
    String GET_ALL_GROUPS_CMD = "echo \""
        + "hdfs:*:11501:hrt_hdfs\n"
        + "rpcuser:*:29:\n"
        + "nfsnobody:*:4294967294:\n"
        + "nfsnobody1:*:4294967295:\n"
        + "maxint:*:2147483647:\n"
        + "minint:*:2147483648:\n"
        + "mapred3:x:498\"" 
        + " | cut -d: -f1,3";
    // Maps for id to name map
    BiMap<Integer, String> uMap = HashBiMap.create();
    BiMap<Integer, String> gMap = HashBiMap.create();

    IdUserGroup.updateMapInternal(uMap, "user", GET_ALL_USERS_CMD, ":");
    assertTrue(uMap.size() == 7);
    assertEquals("nfsnobody", uMap.get(-2));
    assertEquals("nfsnobody1", uMap.get(-1));
    assertEquals("maxint", uMap.get(2147483647));
    assertEquals("minint", uMap.get(-2147483648));
    assertEquals("archivebackup", uMap.get(1031));
    assertEquals("hdfs",uMap.get(11501));
    assertEquals("daemon", uMap.get(2));

    IdUserGroup.updateMapInternal(gMap, "group", GET_ALL_GROUPS_CMD, ":");
    assertTrue(gMap.size() == 7);
    assertEquals("hdfs",gMap.get(11501));
    assertEquals("rpcuser", gMap.get(29));
    assertEquals("nfsnobody", gMap.get(-2));
    assertEquals("nfsnobody1", gMap.get(-1));
    assertEquals("maxint", gMap.get(2147483647));
    assertEquals("minint", gMap.get(-2147483648));
    assertEquals("mapred3", gMap.get(498));
  }

  @Test
  public void testUserUpdateSetting() throws IOException {
    IdUserGroup iug = new IdUserGroup();
    assertEquals(iug.getTimeout(), IdUserGroup.TIMEOUT_DEFAULT);

    Configuration conf = new Configuration();
    conf.setLong(IdUserGroup.NFS_USERUPDATE_MILLY, 0);
    iug = new IdUserGroup(conf);
    assertEquals(iug.getTimeout(), IdUserGroup.TIMEOUT_MIN);

    conf.setLong(IdUserGroup.NFS_USERUPDATE_MILLY,
        IdUserGroup.TIMEOUT_DEFAULT * 2);
    iug = new IdUserGroup(conf);
    assertEquals(iug.getTimeout(), IdUserGroup.TIMEOUT_DEFAULT * 2);
  }
}

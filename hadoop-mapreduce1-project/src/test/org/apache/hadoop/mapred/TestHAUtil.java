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

package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.HAUtil.MR_HA_JOBTRACKERS_KEY_PREFIX;
import static org.apache.hadoop.mapred.HAUtil.MR_JOBTRACKER_ADDRESS_KEY;
import static org.apache.hadoop.mapred.HAUtil.MR_HA_JOBTRACKER_ID_KEY;
import static org.apache.hadoop.mapred.HAUtil.MR_HA_JOBTRACKER_RPC_ADDRESS_KEY;
import static org.apache.hadoop.mapred.HAUtil.MR_JOBTRACKER_RPC_ADDRESS_KEY;
import static org.apache.hadoop.mapred.HAUtil.addKeySuffixes;
import static org.apache.hadoop.mapred.HAUtil.getHaJtRpcAddresses;
import static org.apache.hadoop.mapred.HAUtil.getJobTrackerId;
import static org.apache.hadoop.mapred.HAUtil.getJobTrackerIdOfOtherNode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import org.junit.Before;
import org.junit.Test;

public class TestHAUtil {
  
  private Configuration conf;
  
  @Before
  public void setUp() {
    conf = new Configuration();
    conf.set(MR_JOBTRACKER_ADDRESS_KEY, "logicalName");
    conf.set(addKeySuffixes(MR_HA_JOBTRACKERS_KEY_PREFIX, "logicalName"), "jt1,jt2");
    final String JT1_ADDRESS = "1.2.3.4:8021";
    final String JT2_ADDRESS = "localhost:8022";
    conf.set(addKeySuffixes(MR_JOBTRACKER_RPC_ADDRESS_KEY, "logicalName", "jt1"), JT1_ADDRESS);
    conf.set(addKeySuffixes(MR_JOBTRACKER_RPC_ADDRESS_KEY, "logicalName", "jt2"), JT2_ADDRESS);
  }

  @Test
  public void testGetLogicalNameIgnoresPort() {
    Configuration c = new Configuration();
    c.set(MR_JOBTRACKER_ADDRESS_KEY, "myjt");
    assertEquals("myjt", HAUtil.getLogicalName(c));
    c.set(MR_JOBTRACKER_ADDRESS_KEY, "myjt:8021");
    assertEquals("myjt", HAUtil.getLogicalName(c));
  }

  @Test
  public void testGetHaJtRpcAddresses() throws Exception {
    Map<String, InetSocketAddress> jtMap = getHaJtRpcAddresses(conf);
    assertEquals(2, jtMap.size());
    InetSocketAddress addr = jtMap.get("jt1");
    assertEquals("1.2.3.4", addr.getHostName());
    assertEquals(8021, addr.getPort());

    addr = jtMap.get("jt2");
    assertEquals("localhost", addr.getHostName());
    assertEquals(8022, addr.getPort());
  }
  
  @Test
  public void testGetJobTrackerId() throws Exception {
    assertEquals("Matches localhost when " + MR_HA_JOBTRACKER_ID_KEY +
        " not set", "jt2", getJobTrackerId(conf));
    conf.set(MR_HA_JOBTRACKER_ID_KEY, "jt1");
    assertEquals("Honors " + MR_HA_JOBTRACKER_ID_KEY + " if set",
        "jt1", getJobTrackerId(conf));
  }

  @Test(expected = HadoopIllegalArgumentException.class)
  public void testGetJobTrackerIdWithNoMatch() throws Exception {
    // change conf so neither jt1 nor jt2 match
    conf.set(addKeySuffixes(MR_JOBTRACKER_RPC_ADDRESS_KEY, "logicalName", "jt2"), "2.3.4.5:8022");
    getJobTrackerId(conf);
  }

  @Test
  public void testGetJobTrackerIdOfOtherNode() throws Exception {
    assertEquals(MR_HA_JOBTRACKER_ID_KEY + " not set", "jt1",
        getJobTrackerIdOfOtherNode(conf));
    conf.set(MR_HA_JOBTRACKER_ID_KEY, "jt1");
    assertEquals(MR_HA_JOBTRACKER_ID_KEY + " set", "jt2",
        getJobTrackerIdOfOtherNode(conf));
  }

  @Test
  public void testGetJtHaRpcAddress() throws Exception {
    final String JT1_HA_ADDRESS = "1.2.3.4:10000";
    final String JT2_HA_ADDRESS = "localhost:10000";
    conf.set(addKeySuffixes(MR_HA_JOBTRACKER_RPC_ADDRESS_KEY, "logicalName", "jt1"), JT1_HA_ADDRESS);
    conf.set(addKeySuffixes(MR_HA_JOBTRACKER_RPC_ADDRESS_KEY, "logicalName", "jt2"), JT2_HA_ADDRESS);
    assertEquals("Matches localhost when " + MR_HA_JOBTRACKER_ID_KEY +
        " not set", "localhost",
        HAUtil.getJtHaRpcAddress(conf).getHostName());
    conf.set(MR_HA_JOBTRACKER_ID_KEY, "jt1");
    assertEquals("Honors " + MR_HA_JOBTRACKER_ID_KEY + " if set",
        "1.2.3.4", HAUtil.getJtHaRpcAddress(conf).getHostName());
  }

}

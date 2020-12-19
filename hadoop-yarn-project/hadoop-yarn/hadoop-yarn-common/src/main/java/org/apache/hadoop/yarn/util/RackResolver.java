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

package org.apache.hadoop.yarn.util;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;

public class RackResolver {
  private static DNSToSwitchMapping dnsToSwitchMapping;
  private static boolean initCalled = false;
  private static final Log LOG = LogFactory.getLog(RackResolver.class);

  public synchronized static void init(Configuration conf) {
    if (initCalled) {
      return;
    } else {
      initCalled = true;
    }
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
      conf.getClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
        ScriptBasedMapping.class,
        DNSToSwitchMapping.class);
    try {
      DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
          dnsToSwitchMappingClass, conf);
      // Wrap around the configured class with the Cached implementation so as
      // to save on repetitive lookups.
      // Check if the impl is already caching, to avoid double caching.
      dnsToSwitchMapping =
          ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
              : new CachedDNSToSwitchMapping(newInstance));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Utility method for getting a hostname resolved to a node in the
   * network topology. This method initializes the class with the 
   * right resolver implementation.
   * @param conf
   * @param hostName
   * @return node {@link Node} after resolving the hostname
   */
  public static Node resolve(Configuration conf, String hostName) {
    init(conf);
    return coreResolve(hostName);
  }

  /**
   * Utility method for getting a hostname resolved to a node in the
   * network topology. This method doesn't initialize the class.
   * Call {@link #init(Configuration)} explicitly.
   * @param hostName
   * @return node {@link Node} after resolving the hostname
   */
  public static Node resolve(String hostName) {
    if (!initCalled) {
      throw new IllegalStateException("RackResolver class not yet initialized");
    }
    return coreResolve(hostName);
  }
  
  private static Node coreResolve(String hostName) {
    List <String> tmpList = new ArrayList<String>(1);
    tmpList.add(hostName);
    List <String> rNameList = dnsToSwitchMapping.resolve(tmpList);
    String rName = rNameList.get(0);
    LOG.info("Resolved " + hostName + " to " + rName);
    return new NodeBase(hostName, rName);
  }

  /**
   * Only used by tests
   */
  @VisibleForTesting
  static DNSToSwitchMapping getDnsToSwitchMapping(){
    return dnsToSwitchMapping;
  }
}

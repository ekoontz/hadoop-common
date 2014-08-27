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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

/**
 * A FailoverProxyProvider implementation which allows one to configure two URIs
 * to connect to during fail-over. The first configured address is tried first,
 * and on a fail-over event the other address is tried.
 */
public class ConfiguredFailoverProxyProvider<T> implements
    FailoverProxyProvider<T> {
  
  private static final Log LOG =
      LogFactory.getLog(ConfiguredFailoverProxyProvider.class);
  
  private final Configuration conf;
  private final List<AddressRpcProxyPair<T>> proxies =
      new ArrayList<AddressRpcProxyPair<T>>();
  private final UserGroupInformation ugi;
  private final Class<T> xface;
  
  private int currentProxyIndex = 0;

  public ConfiguredFailoverProxyProvider(Configuration conf, String jtAddress,
      Class<T> xface) {
    Preconditions.checkArgument(
        xface.isAssignableFrom(JTProtocols.class),
        "Interface class %s is not a valid HAServiceProtocol protocol!", xface);
    this.xface = xface;
    
    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
        HAUtil.MR_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY,
        HAUtil.MR_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);
    
    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        HAUtil.MR_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HAUtil.MR_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);
    
    try {
      ugi = UserGroupInformation.getCurrentUser();
      
      Map<String, InetSocketAddress> addressesInJT = HAUtil.getHaJtRpcAddresses(
          conf);
      
      if (addressesInJT == null || addressesInJT.size() == 0) {
        throw new RuntimeException("Could not find any configured addresses " +
            "for JT " + jtAddress);
      }
      
      Collection<InetSocketAddress> addressesOfJTs = addressesInJT.values();
      for (InetSocketAddress address : addressesOfJTs) {
        proxies.add(new AddressRpcProxyPair<T>(address));
      }

      // The client may have a delegation token set for the logical
      // URI of the cluster. Clone this token to apply to each of the
      // underlying IPC addresses so that the IPC code can find it.
      HAUtil.cloneDelegationTokenForLogicalAddress(ugi, jtAddress, addressesOfJTs);    
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  


  @Override
  public Class<T> getInterface() {
    return xface;
  }

  /**
   * Lazily initialize the RPC proxy object.
   */
  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    AddressRpcProxyPair<T> current = proxies.get(currentProxyIndex);
    if (current.jtHaDaemon == null) {
      try {
        current.jtHaDaemon = JobTrackerProxies.createNonHAProxy(conf,
            current.address, xface, ugi, false).getProxy();
      } catch (IOException e) {
        LOG.error("Failed to create RPC proxy to JobTracker HA Daemon", e);
        throw new RuntimeException(e);
      }
    }
    return new ProxyInfo<T>(current.jtHaDaemon, current.address.toString());
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
  }

  /**
   * A little pair object to store the address and connected RPC proxy object to
   * a JT HA daemon. Note that {@link AddressRpcProxyPair#jtHaDaemon} may be
   * null.
   */
  private static class AddressRpcProxyPair<T> {
    public InetSocketAddress address;
    public T jtHaDaemon;
    
    public AddressRpcProxyPair(InetSocketAddress address) {
      this.address = address;
    }
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (AddressRpcProxyPair<T> proxy : proxies) {
      if (proxy.jtHaDaemon != null) {
        if (proxy.jtHaDaemon instanceof Closeable) {
          ((Closeable)proxy.jtHaDaemon).close();
        } else {
          RPC.stopProxy(proxy.jtHaDaemon);
        }
      }
    }
  }
}

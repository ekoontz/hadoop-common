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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

public class JobTrackerProxies {
  
  private static final Log LOG = LogFactory.getLog(JobTrackerProxies.class);
  
  /**
   * Wrapper for a client proxy as well as its associated service ID.
   * This is simply used as a tuple-like return type for
   * {@link JobTrackerProxies#createProxy} and
   * {@link JobTrackerProxies#createNonHAProxy}.
   */
  public static class ProxyAndInfo<PROXYTYPE> {
    private final PROXYTYPE proxy;
    private final Text dtService;
    
    public ProxyAndInfo(PROXYTYPE proxy, Text dtService) {
      this.proxy = proxy;
      this.dtService = dtService;
    }
    
    public PROXYTYPE getProxy() {
      return proxy;
    }
    
    public Text getDelegationTokenService() {
      return dtService;
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      String jtAddress, Class<T> xface) throws IOException {
    Class<FailoverProxyProvider<T>> failoverProxyProviderClass =
        getFailoverProxyProviderClass(conf, jtAddress, xface);
  
    if (failoverProxyProviderClass == null) {
      // Non-HA case
      return createNonHAProxy(conf, NetUtils.createSocketAddr(jtAddress), xface,
          UserGroupInformation.getCurrentUser(), true);
    } else {
      // HA case
      FailoverProxyProvider<T> failoverProxyProvider = 
          createFailoverProxyProvider(conf, failoverProxyProviderClass, xface,
              jtAddress);
      int maxFailoverAttempts =
        conf.getInt(HAUtil.MR_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
            HAUtil.MR_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      long failoverSleepBaseMillis =
        conf.getInt(HAUtil.MR_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
            HAUtil.MR_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
      long failoverSleepMaxMillis =
        conf.getInt(HAUtil.MR_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
            HAUtil.MR_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);
      T proxy = (T) RetryProxy.create(xface, failoverProxyProvider, RetryPolicies
          .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
              maxFailoverAttempts, failoverSleepBaseMillis,
              failoverSleepMaxMillis));
      
      Text dtService = HAUtil.buildTokenServiceForLogicalAddress(jtAddress);
      return new ProxyAndInfo<T>(proxy, dtService);
    }
  }
  
  private static <T> Class<FailoverProxyProvider<T>> getFailoverProxyProviderClass(
      Configuration conf, String jtAddress, Class<T> xface) throws IOException {
    if (jtAddress == null) {
      return null;
    }
    String configKey = DFSUtil.addKeySuffixes(
        HAUtil.MR_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX,
        HAUtil.getLogicalName(jtAddress));
    return (Class<FailoverProxyProvider<T>>)
      conf.getClass(configKey, null, FailoverProxyProvider.class);
  }
  
  public static <T> ProxyAndInfo<T> createNonHAProxy(
      Configuration conf, InetSocketAddress jtAddr, Class<T> xface,
      UserGroupInformation ugi, boolean withRetries) throws IOException {
    
    Text dtService = SecurityUtil.buildTokenService(jtAddr);
    
    T proxy;
    if (xface == JobSubmissionProtocol.class) {
      proxy = (T) createJTProxyWithJobSubmissionProtocol(jtAddr, conf, ugi,
          withRetries);
    } else if (xface == InterTrackerProtocol.class) {
      proxy = (T) createJTProxyWithInterTrackerProtocol(jtAddr, conf, ugi,
          withRetries);
    } else {
      throw new IOException("wrong protocol");
    }
    return new ProxyAndInfo<T>(proxy, dtService);
  }

  private static int getRpcTimeout(Configuration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
                       CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }
  
  private static JobSubmissionProtocol createJTProxyWithJobSubmissionProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      boolean withRetries) throws IOException {
    RPC.setProtocolEngine(conf, JobSubmissionProtocol.class, WritableRpcEngine.class);

    final long version = RPC.getProtocolVersion(JobSubmissionProtocol.class);
    int rpcTimeout = JobClient.getRpcTimeout(conf);
    RPC.getProxy(JobSubmissionProtocol.class, version, address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf), rpcTimeout);

    JobSubmissionProtocol proxy = RPC.getProtocolProxy(
        JobSubmissionProtocol.class, version, address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf), getRpcTimeout(conf), null).getProxy();
    return proxy;
  }
  
  private static InterTrackerProtocol createJTProxyWithInterTrackerProtocol(
      InetSocketAddress address, Configuration conf, UserGroupInformation ugi,
      boolean withRetries) throws IOException {
    RPC.setProtocolEngine(conf, InterTrackerProtocol.class, WritableRpcEngine.class);

    return RPC.waitForProtocolProxy(
        InterTrackerProtocol.class, InterTrackerProtocol.versionID, address,
        conf, getRpcTimeout(conf), null, Long.MAX_VALUE).getProxy();
  }
  
  @SuppressWarnings("unchecked")
  private static <T> FailoverProxyProvider<T> createFailoverProxyProvider(
      Configuration conf, Class<FailoverProxyProvider<T>> failoverProxyProviderClass,
      Class<T> xface, String jtAddress) throws IOException {
    Preconditions.checkArgument(
        xface.isAssignableFrom(JTProtocols.class),
        "Interface %s is not a JobTracker protocol", xface);
    try {
      Constructor<FailoverProxyProvider<T>> ctor = failoverProxyProviderClass
          .getConstructor(Configuration.class, String.class, Class.class);
      FailoverProxyProvider<?> provider = ctor.newInstance(conf, jtAddress,
          xface);
      return (FailoverProxyProvider<T>) provider;
    } catch (Exception e) {
      String message = "Couldn't create proxy provider " + failoverProxyProviderClass;
      if (LOG.isDebugEnabled()) {
        LOG.debug(message, e);
      }
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(message, e);
      }
    }
  }

}

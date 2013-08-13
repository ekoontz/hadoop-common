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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

public class HAUtil {
  
  private static final Log LOG = 
    LogFactory.getLog(HAUtil.class);

  public static final String  MR_JOBTRACKER_ADDRESS_KEY = "mapred.job.tracker";
  public static final String  MR_JOBTRACKER_RPC_ADDRESS_KEY = "mapred.jobtracker.rpc-address";
  public static final String  MR_JOBTRACKER_HTTP_ADDRESS_KEY = "mapred.job.tracker.http.address";
  public static final String  MR_JOBTRACKER_OLD_HTTP_ADDRESS_KEY = "mapred.job.tracker.info.bindAddress";
  public static final String  MR_JOBTRACKER_OLD_HTTP_PORT_KEY = "mapred.job.tracker.info.port";

  // HA configuration
  public static final String  MR_HA_JOBTRACKERS_KEY_PREFIX = "mapred.jobtrackers";
  public static final String  MR_HA_JOBTRACKER_RPC_ADDRESS_KEY = "mapred.ha.jobtracker.rpc-address";
  public static final String  MR_HA_JOBTRACKER_HTTP_REDIRECT_ADDRESS_KEY = "mapred.ha.jobtracker.http-redirect-address";
  public static final String  MR_HA_JOBTRACKER_ID_KEY = "mapred.ha.jobtracker.id";
  public static final String  MR_HA_FENCING_METHODS_KEY = "mapred.ha.fencing.methods";
  public static final String  MR_HA_AUTO_FAILOVER_ENABLED_KEY = "mapred.ha.automatic-failover.enabled";
  public static final boolean MR_HA_AUTO_FAILOVER_ENABLED_DEFAULT = false;
  public static final String  MR_HA_ZKFC_PORT_KEY = "mapred.ha.zkfc.port";
  public static final int     MR_HA_ZKFC_PORT_DEFAULT = 8018;
  public static final String  MR_HA_ACTIVE_CHECK_MILLIS = "mapred.ha.jobtracker.active-check.millis";
  public static final int     MR_HA_ACTIVE_CHECK_MILLIS_DEFAULT = 1000;

  // Failover configuration
  public static final String  MR_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX = "mapred.client.failover.proxy.provider";
  public static final String  MR_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY = "mapred.client.failover.max.attempts";
  public static final int     MR_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT = 15;
  public static final String  MR_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY = "mapred.client.failover.sleep.base.millis";
  public static final int     MR_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
  public static final String  MR_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY = "mapred.client.failover.sleep.max.millis";
  public static final int     MR_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT = 15000;
  public static final String  MR_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY = "mapred.client.failover.connection.retries";
  public static final int     MR_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT = 0;
  public static final String  MR_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY = "mapred.client.failover.connection.retries.on.timeouts";
  public static final int     MR_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;

  public static final String[] JOB_TRACKER_SPECIFIC_KEYS = {
    MR_JOBTRACKER_RPC_ADDRESS_KEY,
    MR_JOBTRACKER_HTTP_ADDRESS_KEY,
    MR_JOBTRACKER_OLD_HTTP_ADDRESS_KEY,
    MR_JOBTRACKER_OLD_HTTP_PORT_KEY,
    MR_HA_ZKFC_PORT_KEY
  };
  
  private interface AddressMatcher {
    public boolean match(InetSocketAddress s);
  }
  
  /**
   * Address matcher for matching an address to local address
   */
  static final AddressMatcher LOCAL_ADDRESS_MATCHER = new AddressMatcher() {
    @Override
    public boolean match(InetSocketAddress s) {
      return NetUtils.isLocalAddress(s.getAddress());
    };
  };

  private HAUtil() { /* Hidden constructor */ }

  /**
   * Returns true if jobtracker HA is configured.
   * 
   * @return true if HA is configured in the configuration; else false.
   */
  public static boolean isHAEnabled() {
    Configuration conf = new Configuration();
    return isHAEnabled(conf, getLogicalName(conf));
  }
  
  /**
   * Returns true if jobtracker HA is configured.
   * 
   * @param conf Configuration
   * @param jtAddress the (logical) jobtracker address
   * @return true if HA is configured in the configuration; else false.
   */
  public static boolean isHAEnabled(Configuration conf, String jtAddress) {
    Map<String, InetSocketAddress> jtMap = getHaJtRpcAddresses(conf);
    return (jtMap != null && jtMap.size() > 1);
  }

  public static String getLogicalName(String jtAddress) {
    int index = jtAddress.indexOf(":");
    return index == -1 ? jtAddress : jtAddress.substring(0, index);
  }
  
  public static String getLogicalName(Configuration conf) {
    return getLogicalName(conf.get(MR_JOBTRACKER_ADDRESS_KEY));
  }
  
  /**
   * Get the jobtracker Id by matching the {@code addressKey}
   * with the the address of the local node.
   * 
   * If {@link #MR_HA_JOBTRACKER_ID_KEY} is not specifically
   * configured, this method determines the jobtracker Id by matching the local
   * node's address with the configured addresses. When a match is found, it
   * returns the JT Id from the corresponding configuration key.
   * 
   * @param conf Configuration
   * @return jobtracker Id on success, null on failure.
   * @throws HadoopIllegalArgumentException on error
   */
  public static String getJobTrackerId(Configuration conf) {
    String jtId = conf.get(MR_HA_JOBTRACKER_ID_KEY);
    if (jtId != null) {
      return jtId;
    }
    String logicalName = getLogicalName(conf);
    Collection<String> jtServiceIds = getJtServiceIds(conf, logicalName);
    if (jtServiceIds.size() == 1) {
      return jtServiceIds.toArray(new String[1])[0];
    }
    String suffixes[] = getSuffixIDs(conf, MR_JOBTRACKER_RPC_ADDRESS_KEY,
        logicalName, LOCAL_ADDRESS_MATCHER);
    if (suffixes == null) {
      String msg = "Configuration " + MR_JOBTRACKER_RPC_ADDRESS_KEY + 
          " must be suffixed with logicalName and jobtracker ID for HA " +
          "configuration.";
      throw new HadoopIllegalArgumentException(msg);
    }
    
    return suffixes[1];
  }
  
  /**
   * Get the jobtracker Id of the other node in an HA setup.
   * 
   * If {@link #MR_HA_JOBTRACKER_ID_KEY} is not specifically
   * configured, this method determines the jobtracker Id by matching the local
   * node's address with the configured addresses. When a match is found, it
   * returns the JT Id from the corresponding configuration key.
   * 
   * @param conf Configuration
   * @return jobtracker Id on success, null on failure.
   * @throws HadoopIllegalArgumentException on error
   */
  public static String getJobTrackerIdOfOtherNode(Configuration conf) {
    String logicalName = getLogicalName(conf);
    Collection<String> jtServiceIds = getJtServiceIds(conf, logicalName);
    String myJtId = getJobTrackerId(conf);
    ArrayList<String> jtSet = Lists.newArrayList(jtServiceIds);
    jtSet.remove(myJtId);
    assert jtSet.size() == 1;
    return jtSet.get(0);
  }

  /**
   * Returns logicalName and jobtracker Id when the local host matches the
   * configuration parameter {@code addressKey}.<logical>.<jobtracker Id>
   * 
   * @param conf Configuration
   * @param addressKey configuration key corresponding to the address.
   * @param logicalName
   * @param matcher matching criteria for matching the address
   * @return Array with logical name and jobtracker Id on success. First element
   *         in the array is logical name and second element is jobtracker Id.
   *         Null value indicates that the configuration does not have the the
   *         Id.
   * @throws HadoopIllegalArgumentException on error
   */
  static String[] getSuffixIDs(final Configuration conf, final String addressKey,
      String logicalName, final AddressMatcher matcher) {
    String jobTrackerId = null;
    int found = 0;
    

    Collection<String> jtIds = getJtServiceIds(conf, logicalName);
    for (String jtId : emptyAsSingletonNull(jtIds)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("addressKey: %s logicalName: %s jtId: %s",
            addressKey, logicalName, jtId));
      }
      String key = DFSUtil.addKeySuffixes(addressKey, logicalName, jtId);
      String addr = conf.get(key);
      if (addr == null) {
        continue;
      }
      InetSocketAddress s = null;
      try {
        s = NetUtils.createSocketAddr(addr);
      } catch (Exception e) {
        LOG.warn("Exception in creating socket address " + addr, e);
        continue;
      }
      if (!s.isUnresolved() && matcher.match(s)) {
        jobTrackerId = jtId;
        found++;
      }
    }

    // Only one address must match the local address
    if (found == 0) {
      String msg = "Configuration has no addresses that match "
          + "local node's address. Please configure the system with "
          + MR_HA_JOBTRACKER_ID_KEY;
      throw new HadoopIllegalArgumentException(msg);
    } else if (found > 1) {
      String msg = "Configuration has multiple addresses that match "
          + "local node's address. Please configure the system with "
          + MR_HA_JOBTRACKER_ID_KEY;
      throw new HadoopIllegalArgumentException(msg);
    }
    return new String[] { logicalName, jobTrackerId };
  }
  
  /**
   * @return <code>coll</code> if it is non-null and non-empty. Otherwise,
   * returns a list with a single null value.
   */
  private static Collection<String> emptyAsSingletonNull(Collection<String> coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
  }

  public static Map<String, InetSocketAddress> getHaJtRpcAddresses(
      Configuration conf) {
    
    // For JT HA there can only be one logical name (unlike HDFS)
    String logicalName = getLogicalName(conf);
    Map<String, InetSocketAddress> map = Maps.newHashMap();
    for (String jtId : getJtServiceIds(conf, logicalName)) {
      String address = conf.get(
          addKeySuffixes(MR_JOBTRACKER_RPC_ADDRESS_KEY, logicalName, jtId));
      InetSocketAddress isa = NetUtils.createSocketAddr(address);
      map.put(jtId, isa);
    }
    return map;
  }

  private static Collection<String> getJtServiceIds(Configuration conf,
      String logicalName) {
    return conf.getTrimmedStringCollection(MR_HA_JOBTRACKERS_KEY_PREFIX +
        "." + logicalName);
  }

  
  private static String getKey(String prefix, Configuration conf) {
    String logicalName = getLogicalName(conf);
    String jtId = getJobTrackerId(conf);
    return addKeySuffixes(prefix, logicalName, jtId);
  }
  
  public static InetSocketAddress getJtHaRpcAddress(Configuration conf) {
    String address = conf.get(getKey(MR_HA_JOBTRACKER_RPC_ADDRESS_KEY, conf));
    return NetUtils.createSocketAddr(address);
  }
  
  public static void setJtHaRpcAddress(Configuration conf, String address) {
    conf.set(getKey(MR_HA_JOBTRACKER_RPC_ADDRESS_KEY, conf), address);
  }
  
  public static InetSocketAddress getJtHaRpcAddress(Configuration conf,
      String jtId) {
    String logicalName = getLogicalName(conf);
    String address = conf.get(addKeySuffixes(MR_HA_JOBTRACKER_RPC_ADDRESS_KEY,
        logicalName, jtId));
    return NetUtils.createSocketAddr(address);
  }

  /**
   * Set the JT address from the RPC address so that the wrapped JobTracker
   * starts on the correct address.
   */
  public static void setJtRpcAddress(Configuration conf) {
    String rpcAddress = conf.get(getKey(MR_JOBTRACKER_RPC_ADDRESS_KEY, conf));
    conf.set(MR_JOBTRACKER_ADDRESS_KEY, rpcAddress);
  }
  
  public static String getJtHaHttpRedirectAddress(Configuration conf,
      String jtId) {
    String logicalName = getLogicalName(conf);
    return conf.get(addKeySuffixes(MR_HA_JOBTRACKER_HTTP_REDIRECT_ADDRESS_KEY,
        logicalName, jtId));
  }
  
  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    return DFSUtil.addKeySuffixes(key, suffixes);
  }
  
  public static void setGenericConf(Configuration conf,
      String logicalName, String jtId, String... keys) {
    for (String key : keys) {
      String value = conf.get(addKeySuffixes(key, logicalName, jtId));
      if (value != null) {
        conf.set(key, value);
        continue;
      }
      value = conf.get(addKeySuffixes(key, logicalName));
      if (value != null) {
        conf.set(key, value);
      }
    }
  }

  /**
   * A prefix put before the jobtracker address inside the "service" field
   * of a delegation token, indicating that the address is a logical (HA)
   * address.
   */
  public static final String HA_DT_SERVICE_PREFIX = "ha-jt:";

  private static final DelegationTokenSelector tokenSelector =
    new DelegationTokenSelector();

  /**
   * Get the service name used in the delegation token for the given logical
   * HA service.
   * @param haJtAddress the logical URI of the cluster
   * @return the service name
   */
  public static Text buildTokenServiceForLogicalAddress(String haJtAddress) {
    return new Text(HA_DT_SERVICE_PREFIX + haJtAddress);
  }

  /**
   * @return true if this token corresponds to a logical address
   * rather than a specific jobtracker.
   */
  public static boolean isTokenForLogicalAddress(
    Token<?> token) {
    return token.getService().toString().startsWith(HA_DT_SERVICE_PREFIX);
  }

  public static String getServiceAddressFromToken(Token<?> token) {
    String service = token.getService().toString();
    return isTokenForLogicalAddress(token) 
      ? service.substring(HA_DT_SERVICE_PREFIX.length()) : service;
  }
  
  public static void cloneDelegationTokenForLogicalAddress(
    UserGroupInformation ugi, String haJtAddress,
    Collection<InetSocketAddress> jtAddresses) {
    Text haService = HAUtil.buildTokenServiceForLogicalAddress(haJtAddress);
    Token<DelegationTokenIdentifier> haToken =
      tokenSelector.selectToken(haService, ugi.getTokens());
    
    if (haToken != null) {
      for (InetSocketAddress singleJtAddr : jtAddresses) {
        Token<DelegationTokenIdentifier> specificToken =
          new Token<DelegationTokenIdentifier>(haToken);
        SecurityUtil.setTokenService(specificToken, singleJtAddr);
        ugi.addToken(specificToken);
        LOG.debug("Mapped HA service delegation token for logical address " +
          haJtAddress + " to jt " + singleJtAddr);
      }
    } else {
      LOG.debug("No HA service delegation token found for logical address " +
        haJtAddress);
    }
  }

}

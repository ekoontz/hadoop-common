package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

public class NetUtils2 {
  static final Log LOG = LogFactory.getLog(NetUtils.class);
  
  /**
   * Handle the transition from pairs of attributes specifying a host and port
   * to a single colon separated one.
   * @param conf the configuration to check
   * @param oldBindAddressName the old address attribute name
   * @param oldPortName the old port attribute name
   * @param newBindAddressName the new combined name
   * @return the complete address from the configuration
   */
  @Deprecated
  public static String getServerAddress(Configuration conf,
                                        String oldBindAddressName,
                                        String oldPortName,
                                        String newBindAddressName) {
    String oldAddr = conf.get(oldBindAddressName);
    String oldPort = conf.get(oldPortName);
    String newAddrPort = conf.get(newBindAddressName);
    if (oldAddr == null && oldPort == null) {
      return newAddrPort;
    }
    String[] newAddrPortParts = newAddrPort.split(":",2);
    if (newAddrPortParts.length != 2) {
      throw new IllegalArgumentException("Invalid address/port: " + 
                                         newAddrPort);
    }
    if (oldAddr == null) {
      oldAddr = newAddrPortParts[0];
    } else {
      LOG.warn("Configuration parameter " + oldBindAddressName +
               " is deprecated. Use " + newBindAddressName + " instead.");
    }
    if (oldPort == null) {
      oldPort = newAddrPortParts[1];
    } else {
      LOG.warn("Configuration parameter " + oldPortName +
               " is deprecated. Use " + newBindAddressName + " instead.");      
    }
    return oldAddr + ":" + oldPort;
  }

}

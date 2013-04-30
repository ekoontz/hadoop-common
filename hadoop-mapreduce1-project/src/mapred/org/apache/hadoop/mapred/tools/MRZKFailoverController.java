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
package org.apache.hadoop.mapred.tools;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.HAUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.JobTrackerHAServiceTarget;
import org.apache.hadoop.mapred.MapReducePolicyProvider;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.InvalidProtocolBufferException;

@InterfaceAudience.Private
public class MRZKFailoverController extends ZKFailoverController {

  private static final Log LOG =
    LogFactory.getLog(MRZKFailoverController.class);
  private AccessControlList adminAcl;
  /* the same as superclass's localTarget, but with the more specfic JT type */
  private final JobTrackerHAServiceTarget localJTTarget;

  @Override
  protected HAServiceTarget dataToTarget(byte[] data) {
    ActiveNodeInfo proto;
    try {
      proto = ActiveNodeInfo.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Invalid data in ZK: " +
          StringUtils.byteToHexString(data));
    }
    JobTrackerHAServiceTarget ret = new JobTrackerHAServiceTarget(
        conf, proto.getNamenodeId());
    InetSocketAddress addressFromProtobuf = new InetSocketAddress(
        proto.getHostname(), proto.getPort());
    
    if (!addressFromProtobuf.equals(ret.getAddress())) {
      throw new RuntimeException("Mismatched address stored in ZK for " +
          ret + ": Stored protobuf was " + addressFromProtobuf + ", address from our own " +
          "configuration for this JobTracker was " + ret.getAddress());
    }
    
    ret.setZkfcPort(proto.getZkfcPort());
    return ret;
  }

  @Override
  protected byte[] targetToData(HAServiceTarget target) {
    InetSocketAddress addr = target.getAddress();

    return ActiveNodeInfo.newBuilder()
      .setHostname(addr.getHostName())
      .setPort(addr.getPort())
      .setZkfcPort(target.getZKFCAddress().getPort())
      .setNameserviceId(localJTTarget.getLogicalName())
      .setNamenodeId(localJTTarget.getJobTrackerId())
      .build()
      .toByteArray();
  }
  
  @Override
  protected InetSocketAddress getRpcAddressToBindTo() {
    int zkfcPort = getZkfcPort(conf);
    return new InetSocketAddress(localTarget.getAddress().getAddress(),
          zkfcPort);
  }
  

  @Override
  protected PolicyProvider getPolicyProvider() {
    return new MapReducePolicyProvider();
  }
  
  public static int getZkfcPort(Configuration conf) {
    return conf.getInt(HAUtil.MR_HA_ZKFC_PORT_KEY,
        HAUtil.MR_HA_ZKFC_PORT_DEFAULT);
  }
  
  public static MRZKFailoverController create(Configuration conf) {
    Configuration localJTConf = MRHAAdmin.addSecurityConfiguration(conf);

    String logicalName = HAUtil.getLogicalName(localJTConf);
    if (!HAUtil.isHAEnabled(localJTConf, logicalName)) {
      throw new HadoopIllegalArgumentException(
          "HA is not enabled for this jobtracker.");
    }
    String jtId = HAUtil.getJobTrackerId(localJTConf);
    HAUtil.setGenericConf(localJTConf, logicalName, jtId, HAUtil.JOB_TRACKER_SPECIFIC_KEYS);
    HAUtil.setGenericConf(localJTConf, logicalName, jtId, ZKFC_CONF_KEYS);
    
    JobTrackerHAServiceTarget localTarget = new JobTrackerHAServiceTarget(
        localJTConf, jtId);
    return new MRZKFailoverController(localJTConf, localTarget);
  }

  private MRZKFailoverController(Configuration conf,
      JobTrackerHAServiceTarget localTarget) {
    super(conf, localTarget);
    this.localJTTarget = localTarget;
    // Setup ACLs
    adminAcl = new AccessControlList(
        conf.get("mapreduce.cluster.administrators", " "));
    LOG.info("Failover controller configured for JobTracker " +
        localTarget);
  }
  
  
  @Override
  protected void initRPC() throws IOException {
    super.initRPC();
    localJTTarget.setZkfcPort(rpcServer.getAddress().getPort());
  }

  @Override
  public void loginAsFCUser() throws IOException {
    InetSocketAddress socAddr = NetUtils.createSocketAddr(
        conf.get(HAUtil.MR_JOBTRACKER_RPC_ADDRESS_KEY));
    SecurityUtil.login(conf, JobTracker.JT_KEYTAB_FILE,
        JobTracker.JT_USER_NAME, socAddr.getHostName());
  }
  
  @Override
  protected String getScopeInsideParentNode() {
    return HAUtil.getLogicalName(conf);
  }

  public static void main(String args[])
      throws Exception {
    if (DFSUtil.parseHelpArgument(args, 
        ZKFailoverController.USAGE, System.out, true)) {
      System.exit(0);
    }
    
    GenericOptionsParser parser = new GenericOptionsParser(
        new JobConf(), args);
    MRZKFailoverController zkfc = MRZKFailoverController.create(
        parser.getConfiguration());
    
    System.exit(zkfc.run(parser.getRemainingArgs()));
  }

  @Override
  protected void checkRpcAdminAccess() throws IOException, AccessControlException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation zkfcUgi = UserGroupInformation.getLoginUser();
    if (adminAcl.isUserAllowed(ugi) ||
        ugi.getShortUserName().equals(zkfcUgi.getShortUserName())) {
      LOG.info("Allowed RPC access from " + ugi + " at " + Server.getRemoteAddress());
      return;
    }
    String msg = "Disallowed RPC access from " + ugi + " at " +
        Server.getRemoteAddress() + ". Not listed in " +
        "mapreduce.cluster.administrators"; 
    LOG.warn(msg);
    throw new AccessControlException(msg);
  }
}

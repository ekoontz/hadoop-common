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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class YarnClientImpl extends YarnClient {

  private static final Log LOG = LogFactory.getLog(YarnClientImpl.class);

  protected ApplicationClientProtocol rmClient;
  protected InetSocketAddress rmAddress;
  protected long statePollIntervalMillis;

  private static final String ROOT = "root";

  public YarnClientImpl() {
    super(YarnClientImpl.class.getName());
  }

  private static InetSocketAddress getRmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.rmAddress = getRmAddress(conf);
    statePollIntervalMillis = conf.getLong(
        YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
        YarnConfiguration.DEFAULT_YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      rmClient = ClientRMProxy.createRMProxy(getConfig(),
            ApplicationClientProtocol.class);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.rmClient != null) {
      RPC.stopProxy(this.rmClient);
    }
    super.serviceStop();
  }

  private GetNewApplicationResponse getNewApplication()
      throws YarnException, IOException {
    GetNewApplicationRequest request =
        Records.newRecord(GetNewApplicationRequest.class);
    return rmClient.getNewApplication(request);
  }

  @Override
  public YarnClientApplication createApplication()
      throws YarnException, IOException {
    ApplicationSubmissionContext context = Records.newRecord
        (ApplicationSubmissionContext.class);
    GetNewApplicationResponse newApp = getNewApplication();
    ApplicationId appId = newApp.getApplicationId();
    context.setApplicationId(appId);
    return new YarnClientApplication(newApp, context);
  }

  @Override
  public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnException, IOException {
    ApplicationId applicationId = appContext.getApplicationId();
    appContext.setApplicationId(applicationId);
    SubmitApplicationRequest request =
        Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(appContext);
    rmClient.submitApplication(request);

    int pollCount = 0;
    while (true) {
      YarnApplicationState state =
          getApplicationReport(applicationId).getYarnApplicationState();
      if (!state.equals(YarnApplicationState.NEW) &&
          !state.equals(YarnApplicationState.NEW_SAVING)) {
        break;
      }
      // Notify the client through the log every 10 poll, in case the client
      // is blocked here too long.
      if (++pollCount % 10 == 0) {
        LOG.info("Application submission is not finished, " +
            "submitted application " + applicationId +
            " is still in " + state);
      }
      try {
        Thread.sleep(statePollIntervalMillis);
      } catch (InterruptedException ie) {
      }
    }


    LOG.info("Submitted application " + applicationId + " to ResourceManager"
        + " at " + rmAddress);
    return applicationId;
  }

  @Override
  public void killApplication(ApplicationId applicationId)
      throws YarnException, IOException {
    LOG.info("Killing application " + applicationId);
    KillApplicationRequest request =
        Records.newRecord(KillApplicationRequest.class);
    request.setApplicationId(applicationId);
    rmClient.forceKillApplication(request);
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    GetApplicationReportRequest request =
        Records.newRecord(GetApplicationReportRequest.class);
    request.setApplicationId(appId);
    GetApplicationReportResponse response =
        rmClient.getApplicationReport(request);
    return response.getApplicationReport();
  }

  public org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
      getAMRMToken(ApplicationId appId) throws YarnException, IOException {
    Token token = getApplicationReport(appId).getAMRMToken();
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        null;
    if (token != null) {
      amrmToken = ConverterUtils.convertFromYarn(token, null);
    }
    return amrmToken;
  }

  @Override
  public List<ApplicationReport> getApplications() throws YarnException,
      IOException {
    return getApplications(null);
  }

  @Override
  public List<ApplicationReport> getApplications(
      Set<String> applicationTypes) throws YarnException, IOException {
    GetApplicationsRequest request =
        applicationTypes == null ? GetApplicationsRequest.newInstance()
            : GetApplicationsRequest.newInstance(applicationTypes);
    GetApplicationsResponse response = rmClient.getApplications(request);
    return response.getApplicationList();
  }

  @Override
  public YarnClusterMetrics getYarnClusterMetrics() throws YarnException,
      IOException {
    GetClusterMetricsRequest request =
        Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse response = rmClient.getClusterMetrics(request);
    return response.getClusterMetrics();
  }

  @Override
  public List<NodeReport> getNodeReports(NodeState... states) throws YarnException,
      IOException {
    EnumSet<NodeState> statesSet = (states.length == 0) ?
        EnumSet.allOf(NodeState.class) : EnumSet.noneOf(NodeState.class);
    for (NodeState state : states) {
      statesSet.add(state);
    }
    GetClusterNodesRequest request = GetClusterNodesRequest
        .newInstance(statesSet);
    GetClusterNodesResponse response = rmClient.getClusterNodes(request);
    return response.getNodeReports();
  }

  @Override
  public Token getRMDelegationToken(Text renewer)
      throws YarnException, IOException {
    /* get the token from RM */
    GetDelegationTokenRequest rmDTRequest =
        Records.newRecord(GetDelegationTokenRequest.class);
    rmDTRequest.setRenewer(renewer.toString());
    GetDelegationTokenResponse response =
        rmClient.getDelegationToken(rmDTRequest);
    return response.getRMDelegationToken();
  }


  private GetQueueInfoRequest
      getQueueInfoRequest(String queueName, boolean includeApplications,
          boolean includeChildQueues, boolean recursive) {
    GetQueueInfoRequest request = Records.newRecord(GetQueueInfoRequest.class);
    request.setQueueName(queueName);
    request.setIncludeApplications(includeApplications);
    request.setIncludeChildQueues(includeChildQueues);
    request.setRecursive(recursive);
    return request;
  }

  @Override
  public QueueInfo getQueueInfo(String queueName) throws YarnException,
      IOException {
    GetQueueInfoRequest request =
        getQueueInfoRequest(queueName, true, false, false);
    Records.newRecord(GetQueueInfoRequest.class);
    return rmClient.getQueueInfo(request).getQueueInfo();
  }

  @Override
  public List<QueueUserACLInfo> getQueueAclsInfo() throws YarnException,
      IOException {
    GetQueueUserAclsInfoRequest request =
        Records.newRecord(GetQueueUserAclsInfoRequest.class);
    return rmClient.getQueueUserAcls(request).getUserAclsInfoList();
  }

  @Override
  public List<QueueInfo> getAllQueues() throws YarnException,
      IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo rootQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true))
          .getQueueInfo();
    getChildQueues(rootQueue, queues, true);
    return queues;
  }

  @Override
  public List<QueueInfo> getRootQueueInfos() throws YarnException,
      IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo rootQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true))
          .getQueueInfo();
    getChildQueues(rootQueue, queues, false);
    return queues;
  }

  @Override
  public List<QueueInfo> getChildQueueInfos(String parent)
      throws YarnException, IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo parentQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(parent, false, true, false))
          .getQueueInfo();
    getChildQueues(parentQueue, queues, true);
    return queues;
  }

  private void getChildQueues(QueueInfo parent, List<QueueInfo> queues,
      boolean recursive) {
    List<QueueInfo> childQueues = parent.getChildQueues();

    for (QueueInfo child : childQueues) {
      queues.add(child);
      if (recursive) {
        getChildQueues(child, queues, recursive);
      }
    }
  }

  @Private
  @VisibleForTesting
  public void setRMClient(ApplicationClientProtocol rmClient) {
    this.rmClient = rmClient;
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;


/**
 * The client interface to the Resource Manager. This module handles all the rpc
 * interfaces to the resource manager from the client.
 */
public class ClientRMService extends AbstractService implements
    ClientRMProtocol {
  private static final ArrayList<ApplicationReport> EMPTY_APPS_REPORT = new ArrayList<ApplicationReport>();

  private static final Log LOG = LogFactory.getLog(ClientRMService.class);

  final private AtomicInteger applicationCounter = new AtomicInteger(0);
  final private YarnScheduler scheduler;
  final private RMContext rmContext;
  private final RMAppManager rmAppManager;

  private Server server;
  private RMDelegationTokenSecretManager rmDTSecretManager;

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  InetSocketAddress clientBindAddress;

  private final ApplicationACLsManager applicationsACLsManager;

  public ClientRMService(RMContext rmContext, YarnScheduler scheduler,
      RMAppManager rmAppManager, ApplicationACLsManager applicationACLsManager,
      RMDelegationTokenSecretManager rmDTSecretManager) {
    super(ClientRMService.class.getName());
    this.scheduler = scheduler;
    this.rmContext = rmContext;
    this.rmAppManager = rmAppManager;
    this.applicationsACLsManager = applicationACLsManager;
    this.rmDTSecretManager = rmDTSecretManager;
  }
  
  @Override
  public void init(Configuration conf) {
    clientBindAddress = conf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
    super.init(conf);
  }
  
  @Override
  public void start() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server =   
      rpc.getServer(ClientRMProtocol.class, this,
            clientBindAddress,
            conf, this.rmDTSecretManager,
            conf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new RMPolicyProvider());
    }
    
    this.server.start();
    clientBindAddress = conf.updateConnectAddr(YarnConfiguration.RM_ADDRESS,
                                               server.getListenerAddress());
    super.start();
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return clientBindAddress;
  }

  /**
   * check if the calling user has the access to application information.
   * @param callerUGI
   * @param owner
   * @param operationPerformed
   * @param applicationId
   * @return
   */
  private boolean checkAccess(UserGroupInformation callerUGI, String owner,
      ApplicationAccessType operationPerformed, ApplicationId applicationId) {
    return applicationsACLsManager.checkAccess(callerUGI, operationPerformed,
        owner, applicationId);
  }

  ApplicationId getNewApplicationId() {
    ApplicationId applicationId = org.apache.hadoop.yarn.util.BuilderUtils
        .newApplicationId(recordFactory, ResourceManager.clusterTimeStamp,
            applicationCounter.incrementAndGet());
    LOG.info("Allocated new applicationId: " + applicationId.getId());
    return applicationId;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnRemoteException {
    GetNewApplicationResponse response = recordFactory
        .newRecordInstance(GetNewApplicationResponse.class);
    response.setApplicationId(getNewApplicationId());
    // Pick up min/max resource from scheduler...
    response.setMinimumResourceCapability(scheduler
        .getMinimumResourceCapability());
    response.setMaximumResourceCapability(scheduler
        .getMaximumResourceCapability());       
    
    return response;
  }
  
  /**
   * It gives response which includes application report if the application
   * present otherwise gives response with application report as null.
   */
  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnRemoteException {
    ApplicationId applicationId = request.getApplicationId();

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      // If the RM doesn't have the application, provide the response with
      // application report as null and let the clients to handle.
      return recordFactory
          .newRecordInstance(GetApplicationReportResponse.class);
    }

    boolean allowAccess = checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.VIEW_APP, applicationId);
    ApplicationReport report =
        application.createAndGetApplicationReport(allowAccess);

    GetApplicationReportResponse response = recordFactory
        .newRecordInstance(GetApplicationReportResponse.class);
    response.setApplicationReport(report);
    return response;
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnRemoteException {
    ApplicationSubmissionContext submissionContext = request
        .getApplicationSubmissionContext();
    ApplicationId applicationId = submissionContext.getApplicationId();
    String user = submissionContext.getUser();
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
      if (rmContext.getRMApps().get(applicationId) != null) {
        throw new IOException("Application with id " + applicationId
            + " is already present! Cannot add a duplicate!");
      }

      // Safety 
      submissionContext.setUser(user);

      // This needs to be synchronous as the client can query 
      // immediately following the submission to get the application status.
      // So call handle directly and do not send an event.
      rmAppManager.handle(new RMAppManagerSubmitEvent(submissionContext, System
          .currentTimeMillis()));

      LOG.info("Application with id " + applicationId.getId() + 
          " submitted by user " + user);
      RMAuditLogger.logSuccess(user, AuditConstants.SUBMIT_APP_REQUEST,
          "ClientRMService", applicationId);
    } catch (IOException ie) {
      LOG.info("Exception in submitting application", ie);
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST, 
          ie.getMessage(), "ClientRMService",
          "Exception in submitting application", applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    SubmitApplicationResponse response = recordFactory
        .newRecordInstance(SubmitApplicationResponse.class);
    return response;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnRemoteException {

    ApplicationId applicationId = request.getApplicationId();

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      RMAuditLogger.logFailure("UNKNOWN", AuditConstants.KILL_APP_REQUEST,
          "UNKNOWN", "ClientRMService" , "Error getting UGI",
          applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      RMAuditLogger.logFailure(callerUGI.getUserName(),
          AuditConstants.KILL_APP_REQUEST, "UNKNOWN", "ClientRMService",
          "Trying to kill an absent application", applicationId);
      throw RPCUtil
          .getRemoteException("Trying to kill an absent application "
              + applicationId);
    }

    if (!checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.MODIFY_APP, applicationId)) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.KILL_APP_REQUEST,
          "User doesn't have permissions to "
              + ApplicationAccessType.MODIFY_APP.toString(), "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER, applicationId);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationAccessType.MODIFY_APP.name() + " on " + applicationId));
    }

    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMAppEvent(applicationId, RMAppEventType.KILL));

    RMAuditLogger.logSuccess(callerUGI.getShortUserName(), 
        AuditConstants.KILL_APP_REQUEST, "ClientRMService" , applicationId);
    KillApplicationResponse response = recordFactory
        .newRecordInstance(KillApplicationResponse.class);
    return response;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnRemoteException {
    GetClusterMetricsResponse response = recordFactory
        .newRecordInstance(GetClusterMetricsResponse.class);
    YarnClusterMetrics ymetrics = recordFactory
        .newRecordInstance(YarnClusterMetrics.class);
    ymetrics.setNumNodeManagers(this.rmContext.getRMNodes().size());
    response.setClusterMetrics(ymetrics);
    return response;
  }
  
  @Override
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    List<ApplicationReport> reports = new ArrayList<ApplicationReport>();
    for (RMApp application : this.rmContext.getRMApps().values()) {
      boolean allowAccess = checkAccess(callerUGI, application.getUser(),
          ApplicationAccessType.VIEW_APP, application.getApplicationId());
      reports.add(application.createAndGetApplicationReport(allowAccess));
    }

    GetAllApplicationsResponse response = 
      recordFactory.newRecordInstance(GetAllApplicationsResponse.class);
    response.setApplicationList(reports);
    return response;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnRemoteException {
    GetClusterNodesResponse response = 
      recordFactory.newRecordInstance(GetClusterNodesResponse.class);
    Collection<RMNode> nodes = this.rmContext.getRMNodes().values();
    List<NodeReport> nodeReports = new ArrayList<NodeReport>(nodes.size());
    for (RMNode nodeInfo : nodes) {
      nodeReports.add(createNodeReports(nodeInfo));
    }
    response.setNodeReports(nodeReports);
    return response;
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnRemoteException {
    GetQueueInfoResponse response =
      recordFactory.newRecordInstance(GetQueueInfoResponse.class);
    try {
      QueueInfo queueInfo = 
        scheduler.getQueueInfo(request.getQueueName(),  
            request.getIncludeChildQueues(), 
            request.getRecursive());
      List<ApplicationReport> appReports = EMPTY_APPS_REPORT;
      if (request.getIncludeApplications()) {
        Collection<RMApp> apps = this.rmContext.getRMApps().values();
        appReports = new ArrayList<ApplicationReport>(
            apps.size());
        for (RMApp app : apps) {
          if (app.getQueue().equals(queueInfo.getQueueName())) {
            appReports.add(app.createAndGetApplicationReport(true));
          }
        }
      }
      queueInfo.setApplications(appReports);
      response.setQueueInfo(queueInfo);
    } catch (IOException ioe) {
      LOG.info("Failed to getQueueInfo for " + request.getQueueName(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
    
    return response;
  }

  private NodeReport createNodeReports(RMNode rmNode) {    
    SchedulerNodeReport schedulerNodeReport = 
        scheduler.getNodeReport(rmNode.getNodeID());
    Resource used = BuilderUtils.newResource(0);
    int numContainers = 0;
    if (schedulerNodeReport != null) {
      used = schedulerNodeReport.getUsedResource();
      numContainers = schedulerNodeReport.getNumContainers();
    } 
    
    NodeReport report = BuilderUtils.newNodeReport(rmNode.getNodeID(),
        rmNode.getState(),
        rmNode.getHttpAddress(), rmNode.getRackName(), used,
        rmNode.getTotalCapability(), numContainers,
        rmNode.getNodeHealthStatus());

    return report;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
    GetQueueUserAclsInfoResponse response = 
      recordFactory.newRecordInstance(GetQueueUserAclsInfoResponse.class);
    response.setUserAclsInfoList(scheduler.getQueueUserAclInfo());
    return response;
  }


  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnRemoteException {
    try {

      // Verify that the connection is kerberos authenticated
      AuthenticationMethod authMethod = UserGroupInformation
        .getRealAuthenticationMethod(UserGroupInformation.getCurrentUser());
      if (UserGroupInformation.isSecurityEnabled()
          && (authMethod != AuthenticationMethod.KERBEROS)) {
        throw new IOException(
          "Delegation Token can be issued only with kerberos authentication");
      }

      GetDelegationTokenResponse response =
          recordFactory.newRecordInstance(GetDelegationTokenResponse.class);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      Text owner = new Text(ugi.getUserName());
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      RMDelegationTokenIdentifier tokenIdentifier =
          new RMDelegationTokenIdentifier(owner, new Text(request.getRenewer()), 
              realUser);
      Token<RMDelegationTokenIdentifier> realRMDTtoken =
          new Token<RMDelegationTokenIdentifier>(tokenIdentifier,
              this.rmDTSecretManager);
      response.setRMDelegationToken(
          BuilderUtils.newDelegationToken(
              realRMDTtoken.getIdentifier(),
              realRMDTtoken.getKind().toString(),
              realRMDTtoken.getPassword(),
              realRMDTtoken.getService().toString()
              ));
      return response;
    } catch(IOException io) {
      throw RPCUtil.getRemoteException(io);
    }
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }
  
  @Override
  public void stop() {
    if (this.server != null) {
        this.server.stop();
    }
    super.stop();
  }
  
}

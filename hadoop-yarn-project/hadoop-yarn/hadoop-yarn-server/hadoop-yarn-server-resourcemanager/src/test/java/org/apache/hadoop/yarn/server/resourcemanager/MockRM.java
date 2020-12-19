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

import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

@SuppressWarnings("unchecked")
public class MockRM extends ResourceManager {

  public MockRM() {
    this(new YarnConfiguration());
  }

  public MockRM(Configuration conf) {
    super(StoreFactory.getStore(conf));    
    init(conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf));
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
  }

  public void waitForState(ApplicationId appId, RMAppState finalState)
      throws Exception {
    RMApp app = getRMContext().getRMApps().get(appId);
    Assert.assertNotNull("app shouldn't be null", app);
    int timeoutSecs = 0;
    while (!finalState.equals(app.getState()) && timeoutSecs++ < 20) {
      System.out.println("App State is : " + app.getState()
          + " Waiting for state : " + finalState);
      Thread.sleep(500);
    }
    System.out.println("App State is : " + app.getState());
    Assert.assertEquals("App state is not correct (timedout)", finalState,
        app.getState());
  }

  // get new application id
  public GetNewApplicationResponse getNewAppId() throws Exception {
    ClientRMProtocol client = getClientRMService();
    return client.getNewApplication(Records
        .newRecord(GetNewApplicationRequest.class));
  }

  public RMApp submitApp(int masterMemory) throws Exception {
    return submitApp(masterMemory, "", "");
  }

  // client
  public RMApp submitApp(int masterMemory, String name, String user) throws Exception {
    return submitApp(masterMemory, name, user, null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls) throws Exception {
    ClientRMProtocol client = getClientRMService();
    GetNewApplicationResponse resp = client.getNewApplication(Records
        .newRecord(GetNewApplicationRequest.class));
    ApplicationId appId = resp.getApplicationId();

    SubmitApplicationRequest req = Records
        .newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub = Records
        .newRecord(ApplicationSubmissionContext.class);
    sub.setApplicationId(appId);
    sub.setApplicationName(name);
    sub.setUser(user);
    ContainerLaunchContext clc = Records
        .newRecord(ContainerLaunchContext.class);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(masterMemory);
    clc.setResource(capability);
    clc.setApplicationACLs(acls);
    sub.setAMContainerSpec(clc);
    req.setApplicationSubmissionContext(sub);

    client.submitApplication(req);
    // make sure app is immediately available after submit
    waitForState(appId, RMAppState.ACCEPTED);
    return getRMContext().getRMApps().get(appId);
  }

  public MockNM registerNode(String nodeIdStr, int memory) throws Exception {
    MockNM nm = new MockNM(nodeIdStr, memory, getResourceTrackerService());
    nm.registerNode();
    return nm;
  }

  public void sendNodeStarted(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    node.handle(new RMNodeEvent(nm.getNodeId(), RMNodeEventType.STARTED));
  }
  
  public void sendNodeLost(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    node.handle(new RMNodeEvent(nm.getNodeId(), RMNodeEventType.EXPIRE));
  }

  public void NMwaitForState(NodeId nodeid, NodeState finalState)
      throws Exception {
    RMNode node = getRMContext().getRMNodes().get(nodeid);
    Assert.assertNotNull("node shouldn't be null", node);
    int timeoutSecs = 0;
    while (!finalState.equals(node.getState()) && timeoutSecs++ < 20) {
      System.out.println("Node State is : " + node.getState()
          + " Waiting for state : " + finalState);
      Thread.sleep(500);
    }
    System.out.println("Node State is : " + node.getState());
    Assert.assertEquals("Node state is not correct (timedout)", finalState,
        node.getState());
  }

  public void killApp(ApplicationId appId) throws Exception {
    ClientRMProtocol client = getClientRMService();
    KillApplicationRequest req = Records
        .newRecord(KillApplicationRequest.class);
    req.setApplicationId(appId);
    client.forceKillApplication(req);
  }

  // from AMLauncher
  public MockAM sendAMLaunched(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    am.waitForState(RMAppAttemptState.ALLOCATED);
    getRMContext()
        .getDispatcher()
        .getEventHandler()
        .handle(
            new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.LAUNCHED));
    return am;
  }

  public void sendAMLaunchFailed(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    am.waitForState(RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler()
        .handle(new RMAppAttemptLaunchFailedEvent(appAttemptId, "Failed"));
  }

  @Override
  protected ClientRMService createClientRMService() {
    return new ClientRMService(getRMContext(), getResourceScheduler(),
        rmAppManager, applicationACLsManager, null) {
      @Override
      public void start() {
        // override to not start rpc handler
      }

      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(getRMContext(), nodesListManager,
        this.nmLivelinessMonitor, this.containerTokenSecretManager) {
      @Override
      public void start() {
        // override to not start rpc handler
      }

      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(getRMContext(), scheduler) {
      @Override
      public void start() {
        // override to not start rpc handler
      }

      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(getRMContext()) {
      @Override
      public void start() {
        // override to not start rpc handler
      }

      @Override
      public void handle(AMLauncherEvent appEvent) {
        // don't do anything
      }

      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected AdminService createAdminService(ClientRMService clientRMService,
      ApplicationMasterService applicationMasterService,
      ResourceTrackerService resourceTrackerService) {
    return new AdminService(getConfig(), scheduler, getRMContext(),
        this.nodesListManager, clientRMService, applicationMasterService,
        resourceTrackerService) {
      @Override
      public void start() {
        // override to not start rpc handler
      }

      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  @Override
  protected void startWepApp() {
    // override to disable webapp
  }

}

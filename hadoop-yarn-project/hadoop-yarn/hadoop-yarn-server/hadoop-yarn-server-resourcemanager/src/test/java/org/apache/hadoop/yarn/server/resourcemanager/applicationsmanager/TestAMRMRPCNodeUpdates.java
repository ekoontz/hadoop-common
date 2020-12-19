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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

public class TestAMRMRPCNodeUpdates {
  private MockRM rm;
  ApplicationMasterService amService = null;
  DrainDispatcher dispatcher = null;

  @Before
  public void setUp() {
    dispatcher = new DrainDispatcher();
    this.rm = new MockRM() {
      @Override
      protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        return new SchedulerEventDispatcher(this.scheduler) {
          @Override
          public void handle(SchedulerEvent event) {
            scheduler.handle(event);
          }
        };
      }

      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
    amService = rm.getApplicationMasterService();
  }
  
  @After
  public void tearDown() {
    if (rm != null) {
      this.rm.stop();
    }
  }
  
  private void syncNodeHeartbeat(MockNM nm, boolean health) throws Exception {
    nm.nodeHeartbeat(health);
    dispatcher.await();
  }
  
  private void syncNodeLost(MockNM nm) throws Exception {
    rm.sendNodeStarted(nm);
    rm.NMwaitForState(nm.getNodeId(), NodeState.RUNNING);
    rm.sendNodeLost(nm);
    dispatcher.await();
  }

  @Test
  public void testAMRMUnusableNodes() throws Exception {
    
    MockNM nm1 = rm.registerNode("h1:1234", 10000);
    MockNM nm2 = rm.registerNode("h2:1234", 10000);
    MockNM nm3 = rm.registerNode("h3:1234", 10000);
    MockNM nm4 = rm.registerNode("h4:1234", 10000);

    RMApp app1 = rm.submitApp(2000);

    // Trigger the scheduling so the AM gets 'launched' on nm1
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    
    // register AM returns no unusable node
    am1.registerAppAttempt();

    // allocate request returns no updated node
    AllocateRequest allocateRequest1 = BuilderUtils.newAllocateRequest(attempt1
        .getAppAttemptId(), 0, 0F, null, null);
    AMResponse response1 = amService.allocate(allocateRequest1).getAMResponse();
    List<NodeReport> updatedNodes = response1.getUpdatedNodes();
    Assert.assertEquals(0, updatedNodes.size());

    syncNodeHeartbeat(nm4, false);
    
    // allocate request returns updated node
    allocateRequest1 = BuilderUtils.newAllocateRequest(attempt1
        .getAppAttemptId(), response1.getResponseId(), 0F, null, null);
    response1 = amService.allocate(allocateRequest1).getAMResponse();
    updatedNodes = response1.getUpdatedNodes();
    Assert.assertEquals(1, updatedNodes.size());
    NodeReport nr = updatedNodes.iterator().next();
    Assert.assertEquals(nm4.getNodeId(), nr.getNodeId());
    Assert.assertEquals(NodeState.UNHEALTHY, nr.getNodeState());
    
    // resending the allocate request returns the same result
    response1 = amService.allocate(allocateRequest1).getAMResponse();
    updatedNodes = response1.getUpdatedNodes();
    Assert.assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    Assert.assertEquals(nm4.getNodeId(), nr.getNodeId());
    Assert.assertEquals(NodeState.UNHEALTHY, nr.getNodeState());

    syncNodeLost(nm3);
    
    // subsequent allocate request returns delta
    allocateRequest1 = BuilderUtils.newAllocateRequest(attempt1
        .getAppAttemptId(), response1.getResponseId(), 0F, null, null);
    response1 = amService.allocate(allocateRequest1).getAMResponse();
    updatedNodes = response1.getUpdatedNodes();
    Assert.assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    Assert.assertEquals(nm3.getNodeId(), nr.getNodeId());
    Assert.assertEquals(NodeState.LOST, nr.getNodeState());
        
    // registering another AM gives it the complete failed list
    RMApp app2 = rm.submitApp(2000);
    // Trigger nm2 heartbeat so that AM gets launched on it
    nm2.nodeHeartbeat(true);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    
    // register AM returns all unusable nodes
    am2.registerAppAttempt();
    
    // allocate request returns no updated node
    AllocateRequest allocateRequest2 = BuilderUtils.newAllocateRequest(attempt2
        .getAppAttemptId(), 0, 0F, null, null);
    AMResponse response2 = amService.allocate(allocateRequest2).getAMResponse();
    updatedNodes = response2.getUpdatedNodes();
    Assert.assertEquals(0, updatedNodes.size());
    
    syncNodeHeartbeat(nm4, true);
    
    // both AM's should get delta updated nodes
    allocateRequest1 = BuilderUtils.newAllocateRequest(attempt1
        .getAppAttemptId(), response1.getResponseId(), 0F, null, null);
    response1 = amService.allocate(allocateRequest1).getAMResponse();
    updatedNodes = response1.getUpdatedNodes();
    Assert.assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    Assert.assertEquals(nm4.getNodeId(), nr.getNodeId());
    Assert.assertEquals(NodeState.RUNNING, nr.getNodeState());
    
    allocateRequest2 = BuilderUtils.newAllocateRequest(attempt2
        .getAppAttemptId(), response2.getResponseId(), 0F, null, null);
    response2 = amService.allocate(allocateRequest2).getAMResponse();
    updatedNodes = response2.getUpdatedNodes();
    Assert.assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    Assert.assertEquals(nm4.getNodeId(), nr.getNodeId());
    Assert.assertEquals(NodeState.RUNNING, nr.getNodeState());

    // subsequent allocate calls should return no updated nodes
    allocateRequest2 = BuilderUtils.newAllocateRequest(attempt2
        .getAppAttemptId(), response2.getResponseId(), 0F, null, null);
    response2 = amService.allocate(allocateRequest2).getAMResponse();
    updatedNodes = response2.getUpdatedNodes();
    Assert.assertEquals(0, updatedNodes.size());
    
    // how to do the above for LOST node
  
  }
}

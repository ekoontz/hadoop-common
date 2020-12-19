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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestApplicationCleanup {

  private static final Log LOG = LogFactory
    .getLog(TestApplicationCleanup.class);

  @Test
  public void testAppCleanup() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();

    MockNM nm1 = rm.registerNode("h1:1234", 5000);

    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    
    //request for containers
    int request = 2;
    am.allocate("h1" , 1000, request, 
        new ArrayList<ContainerId>());
    
    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    int waitCount = 0;
    while (contReceived < request && waitCount++ < 200) {
      LOG.info("Got " + contReceived + " containers. Waiting to get "
               + request);
      Thread.sleep(100);
      conts = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
      contReceived += conts.size();
    }
    Assert.assertEquals(request, contReceived);
    
    am.unregisterAppAttempt();
    HeartbeatResponse resp = nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1,
        ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);

    //currently only containers are cleaned via this
    //AM container is cleaned via container launcher
    resp = nm1.nodeHeartbeat(true);
    List<ContainerId> contsToClean = resp.getContainersToCleanupList();
    List<ApplicationId> apps = resp.getApplicationsToCleanupList();
    int cleanedConts = contsToClean.size();
    int cleanedApps = apps.size();
    waitCount = 0;
    while ((cleanedConts < 2 || cleanedApps < 1) && waitCount++ < 200) {
      LOG.info("Waiting to get cleanup events.. cleanedConts: "
          + cleanedConts + " cleanedApps: " + cleanedApps);
      Thread.sleep(100);
      resp = nm1.nodeHeartbeat(true);
      contsToClean = resp.getContainersToCleanupList();
      apps = resp.getApplicationsToCleanupList();
      cleanedConts += contsToClean.size();
      cleanedApps += apps.size();
    }
    
    Assert.assertEquals(1, apps.size());
    Assert.assertEquals(app.getApplicationId(), apps.get(0));
    Assert.assertEquals(1, cleanedApps);
    Assert.assertEquals(2, cleanedConts);

    rm.stop();
  }

  @Test
  public void testContainerCleanup() throws Exception {

    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    final DrainDispatcher dispatcher = new DrainDispatcher();
    MockRM rm = new MockRM() {
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

    MockNM nm1 = rm.registerNode("h1:1234", 5000);

    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    
    //request for containers
    int request = 2;
    am.allocate("h1" , 1000, request, 
        new ArrayList<ContainerId>());
    dispatcher.await();
    
    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    int waitCount = 0;
    while (contReceived < request && waitCount++ < 200) {
      LOG.info("Got " + contReceived + " containers. Waiting to get "
               + request);
      Thread.sleep(100);
      conts = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
      dispatcher.await();
      contReceived += conts.size();
    }
    Assert.assertEquals(request, contReceived);

    // Release a container.
    ArrayList<ContainerId> release = new ArrayList<ContainerId>();
    release.add(conts.get(0).getId());
    am.allocate(new ArrayList<ResourceRequest>(), release);
    dispatcher.await();

    // Send one more heartbeat with a fake running container. This is to
    // simulate the situation that can happen if the NM reports that container
    // is running in the same heartbeat when the RM asks it to clean it up.
    Map<ApplicationId, List<ContainerStatus>> containerStatuses =
        new HashMap<ApplicationId, List<ContainerStatus>>();
    ArrayList<ContainerStatus> containerStatusList =
        new ArrayList<ContainerStatus>();
    containerStatusList.add(BuilderUtils.newContainerStatus(conts.get(0)
      .getId(), ContainerState.RUNNING, "nothing", 0));
    containerStatuses.put(app.getApplicationId(), containerStatusList);

    HeartbeatResponse resp = nm1.nodeHeartbeat(containerStatuses, true);
    dispatcher.await();
    List<ContainerId> contsToClean = resp.getContainersToCleanupList();
    int cleanedConts = contsToClean.size();
    waitCount = 0;
    while (cleanedConts < 1 && waitCount++ < 200) {
      LOG.info("Waiting to get cleanup events.. cleanedConts: " + cleanedConts);
      Thread.sleep(100);
      resp = nm1.nodeHeartbeat(true);
      dispatcher.await();
      contsToClean = resp.getContainersToCleanupList();
      cleanedConts += contsToClean.size();
    }
    LOG.info("Got cleanup for " + contsToClean.get(0));
    Assert.assertEquals(1, cleanedConts);

    // Now to test the case when RM already gave cleanup, and NM suddenly
    // realizes that the container is running.
    LOG.info("Testing container launch much after release and "
        + "NM getting cleanup");
    containerStatuses.clear();
    containerStatusList.clear();
    containerStatusList.add(BuilderUtils.newContainerStatus(conts.get(0)
      .getId(), ContainerState.RUNNING, "nothing", 0));
    containerStatuses.put(app.getApplicationId(), containerStatusList);

    resp = nm1.nodeHeartbeat(containerStatuses, true);
    dispatcher.await();
    contsToClean = resp.getContainersToCleanupList();
    cleanedConts = contsToClean.size();
    // The cleanup list won't be instantaneous as it is given out by scheduler
    // and not RMNodeImpl.
    waitCount = 0;
    while (cleanedConts < 1 && waitCount++ < 200) {
      LOG.info("Waiting to get cleanup events.. cleanedConts: " + cleanedConts);
      Thread.sleep(100);
      resp = nm1.nodeHeartbeat(true);
      dispatcher.await();
      contsToClean = resp.getContainersToCleanupList();
      cleanedConts += contsToClean.size();
    }
    LOG.info("Got cleanup for " + contsToClean.get(0));
    Assert.assertEquals(1, cleanedConts);

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestApplicationCleanup t = new TestApplicationCleanup();
    t.testAppCleanup();
  }
}
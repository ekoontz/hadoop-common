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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;

public class RMAppImpl implements RMApp, Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppImpl.class);
  private static final String UNAVAILABLE = "N/A";

  // Immutable fields
  private final ApplicationId applicationId;
  private final RMContext rmContext;
  private final Configuration conf;
  private final String user;
  private final String name;
  private final ApplicationSubmissionContext submissionContext;
  private final Dispatcher dispatcher;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;
  private final StringBuilder diagnostics = new StringBuilder();
  private final int maxAppAttempts;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Map<ApplicationAttemptId, RMAppAttempt> attempts
      = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
  private final long submitTime;
  private final Set<RMNode> updatedNodes = new HashSet<RMNode>();
  private final String applicationType;

  // Mutable fields
  private long startTime;
  private long finishTime;
  private RMAppAttempt currentAttempt;
  private String queue;
  @SuppressWarnings("rawtypes")
  private EventHandler handler;
  private static final FinalTransition FINAL_TRANSITION = new FinalTransition();
  private static final AppFinishedTransition FINISHED_TRANSITION =
      new AppFinishedTransition();

  private static final StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent> stateMachineFactory
                               = new StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent>(RMAppState.NEW)


     // Transitions from NEW state
    .addTransition(RMAppState.NEW, RMAppState.NEW,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.NEW, RMAppState.NEW_SAVING,
        RMAppEventType.START, new RMAppSavingTransition())
    .addTransition(RMAppState.NEW, RMAppState.SUBMITTED,
        RMAppEventType.RECOVER, new StartAppAttemptTransition())
    .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
        new AppKilledTransition())
    .addTransition(RMAppState.NEW, RMAppState.FAILED,
        RMAppEventType.APP_REJECTED, new AppRejectedTransition())

    // Transitions from NEW_SAVING state
    .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.NEW_SAVING, RMAppState.SUBMITTED,
        RMAppEventType.APP_SAVED, new StartAppAttemptTransition())
    .addTransition(RMAppState.NEW_SAVING, RMAppState.KILLED,
        RMAppEventType.KILL, new AppKilledTransition())
    .addTransition(RMAppState.NEW_SAVING, RMAppState.FAILED,
        RMAppEventType.APP_REJECTED, new AppRejectedTransition())

     // Transitions from SUBMITTED state
    .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.FAILED,
        RMAppEventType.APP_REJECTED, new AppRejectedTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
        RMAppEventType.APP_ACCEPTED)
    .addTransition(RMAppState.SUBMITTED, RMAppState.KILLED,
        RMAppEventType.KILL, new KillAppAndAttemptTransition())

     // Transitions from ACCEPTED state
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
        RMAppEventType.ATTEMPT_REGISTERED)
    .addTransition(RMAppState.ACCEPTED,
        EnumSet.of(RMAppState.SUBMITTED, RMAppState.FAILED),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.SUBMITTED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.KILLED,
        RMAppEventType.KILL, new KillAppAndAttemptTransition())

     // Transitions from RUNNING state
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.FINISHING,
        RMAppEventType.ATTEMPT_FINISHING, new RMAppFinishingTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.RUNNING,
        EnumSet.of(RMAppState.SUBMITTED, RMAppState.FAILED),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.SUBMITTED))
    .addTransition(RMAppState.RUNNING, RMAppState.KILLED,
        RMAppEventType.KILL, new KillAppAndAttemptTransition())

     // Transitions from FINISHING state
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
        RMAppEventType.KILL, new KillAppAndAttemptTransition())
    // ignorable transitions
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
        RMAppEventType.NODE_UPDATE)

     // Transitions from FINISHED state
    .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
        RMAppEventType.KILL)
     // ignorable transitions
    .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
        EnumSet.of(
            RMAppEventType.NODE_UPDATE,
            RMAppEventType.ATTEMPT_FINISHING,
            RMAppEventType.ATTEMPT_FINISHED))

     // Transitions from FAILED state
    .addTransition(RMAppState.FAILED, RMAppState.FAILED,
        EnumSet.of(RMAppEventType.KILL, RMAppEventType.APP_SAVED))
     // ignorable transitions
    .addTransition(RMAppState.FAILED, RMAppState.FAILED, 
        RMAppEventType.NODE_UPDATE)

     // Transitions from KILLED state
    .addTransition(
        RMAppState.KILLED,
        RMAppState.KILLED,
        EnumSet.of(RMAppEventType.APP_ACCEPTED,
            RMAppEventType.APP_REJECTED, RMAppEventType.KILL,
            RMAppEventType.ATTEMPT_FINISHED, RMAppEventType.ATTEMPT_FAILED,
            RMAppEventType.ATTEMPT_KILLED, RMAppEventType.APP_SAVED))
     // ignorable transitions
    .addTransition(RMAppState.KILLED, RMAppState.KILLED,
        RMAppEventType.NODE_UPDATE)

     .installTopology();

  private final StateMachine<RMAppState, RMAppEventType, RMAppEvent>
                                                                 stateMachine;

  private static final ApplicationResourceUsageReport
    DUMMY_APPLICATION_RESOURCE_USAGE_REPORT =
      BuilderUtils.newApplicationResourceUsageReport(-1, -1,
          Resources.createResource(-1, -1), Resources.createResource(-1, -1),
          Resources.createResource(-1, -1));
  private static final int DUMMY_APPLICATION_ATTEMPT_NUMBER = -1;
  
  public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
      Configuration config, String name, String user, String queue,
      ApplicationSubmissionContext submissionContext,
      YarnScheduler scheduler,
      ApplicationMasterService masterService, long submitTime, String applicationType) {

    this.applicationId = applicationId;
    this.name = name;
    this.rmContext = rmContext;
    this.dispatcher = rmContext.getDispatcher();
    this.handler = dispatcher.getEventHandler();
    this.conf = config;
    this.user = user;
    this.queue = queue;
    this.submissionContext = submissionContext;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.submitTime = submitTime;
    this.startTime = System.currentTimeMillis();
    this.applicationType = applicationType;

    int globalMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    int individualMaxAppAttempts = submissionContext.getMaxAppAttempts();
    if (individualMaxAppAttempts <= 0 ||
        individualMaxAppAttempts > globalMaxAppAttempts) {
      this.maxAppAttempts = globalMaxAppAttempts;
      LOG.warn("The specific max attempts: " + individualMaxAppAttempts
          + " for application: " + applicationId.getId()
          + " is invalid, because it is out of the range [1, "
          + globalMaxAppAttempts + "]. Use the global max attempts instead.");
    } else {
      this.maxAppAttempts = individualMaxAppAttempts;
    }

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }
  
  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    return this.submissionContext;
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    this.readLock.lock();
    try {
      // finish state is obtained based on the state machine's current state 
      // as a fall-back in case the application has not been unregistered 
      // ( or if the app never unregistered itself )
      // when the report is requested
      if (currentAttempt != null 
          && currentAttempt.getFinalApplicationStatus() != null) {
        return currentAttempt.getFinalApplicationStatus();   
      }
      return 
          createFinalApplicationStatus(this.stateMachine.getCurrentState());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getUser() {
    return this.user;
  }

  @Override
  public float getProgress() {
    this.readLock.lock();

    try {
      if (this.currentAttempt != null) {
        return this.currentAttempt.getProgress();
      }
      return 0;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId) {
    this.readLock.lock();

    try {
      return this.attempts.get(appAttemptId);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getQueue() {
    return this.queue;
  }
  
  @Override
  public void setQueue(String queue) {
    this.queue = queue;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public RMAppAttempt getCurrentAppAttempt() {
    this.readLock.lock();

    try {
      return this.currentAttempt;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts() {
    this.readLock.lock();

    try {
      return Collections.unmodifiableMap(this.attempts);
    } finally {
      this.readLock.unlock();
    }
  }

  private FinalApplicationStatus createFinalApplicationStatus(RMAppState state) {
    switch(state) {
    case NEW:
    case NEW_SAVING:
    case SUBMITTED:
    case ACCEPTED:
    case RUNNING:
      return FinalApplicationStatus.UNDEFINED;    
    // finished without a proper final state is the same as failed  
    case FINISHING:
    case FINISHED:
    case FAILED:
      return FinalApplicationStatus.FAILED;
    case KILLED:
      return FinalApplicationStatus.KILLED;
    }
    throw new YarnRuntimeException("Unknown state passed!");
  }

  @Override
  public int pullRMNodeUpdates(Collection<RMNode> updatedNodes) {
    this.writeLock.lock();
    try {
      int updatedNodeCount = this.updatedNodes.size();
      updatedNodes.addAll(this.updatedNodes);
      this.updatedNodes.clear();
      return updatedNodeCount;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Override
  public ApplicationReport createAndGetApplicationReport(boolean allowAccess) {
    this.readLock.lock();

    try {
      ApplicationAttemptId currentApplicationAttemptId = null;
      org.apache.hadoop.yarn.api.records.Token clientToAMToken = null;
      String trackingUrl = UNAVAILABLE;
      String host = UNAVAILABLE;
      String origTrackingUrl = UNAVAILABLE;
      int rpcPort = -1;
      ApplicationResourceUsageReport appUsageReport =
          DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
      FinalApplicationStatus finishState = getFinalApplicationStatus();
      String diags = UNAVAILABLE;
      float progress = 0.0f;
      org.apache.hadoop.yarn.api.records.Token amrmToken = null;
      if (allowAccess) {
        if (this.currentAttempt != null) {
          currentApplicationAttemptId = this.currentAttempt.getAppAttemptId();
          trackingUrl = this.currentAttempt.getTrackingUrl();
          origTrackingUrl = this.currentAttempt.getOriginalTrackingUrl();
          Token<ClientToAMTokenIdentifier> attemptClientToAMToken =
              this.currentAttempt.getClientToAMToken();
          if (attemptClientToAMToken != null) {
            clientToAMToken =
                BuilderUtils.newClientToAMToken(
                    attemptClientToAMToken.getIdentifier(),
                    attemptClientToAMToken.getKind().toString(),
                    attemptClientToAMToken.getPassword(),
                    attemptClientToAMToken.getService().toString());
          }
          host = this.currentAttempt.getHost();
          rpcPort = this.currentAttempt.getRpcPort();
          appUsageReport = currentAttempt.getApplicationResourceUsageReport();
          progress = currentAttempt.getProgress();
        }
        diags = this.diagnostics.toString();

        if (currentAttempt != null && 
            currentAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
          try {
            if (getApplicationSubmissionContext().getUnmanagedAM() &&
              getUser().equals(UserGroupInformation.getCurrentUser().getUserName())) {
              Token<AMRMTokenIdentifier> token = currentAttempt.getAMRMToken();
              if (token != null) {
                amrmToken = BuilderUtils.newAMRMToken(token.getIdentifier(),
                    token.getKind().toString(), token.getPassword(),
                    token.getService().toString());
              }
            }
          } catch (IOException ex) {
            LOG.warn("UserGroupInformation.getCurrentUser() error: " + 
              ex.toString(), ex);
          }          
        }
      }

      if (currentApplicationAttemptId == null) {
        currentApplicationAttemptId = 
            BuilderUtils.newApplicationAttemptId(this.applicationId, 
                DUMMY_APPLICATION_ATTEMPT_NUMBER);
      }

      return BuilderUtils.newApplicationReport(this.applicationId,
          currentApplicationAttemptId, this.user, this.queue,
          this.name, host, rpcPort, clientToAMToken,
          RMServerUtils.createApplicationState(this.stateMachine.getCurrentState()), diags,
          trackingUrl, this.startTime, this.finishTime, finishState,
          appUsageReport, origTrackingUrl, progress, this.applicationType, 
          amrmToken);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    this.readLock.lock();

    try {
      return this.finishTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getStartTime() {
    this.readLock.lock();

    try {
      return this.startTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getSubmitTime() {
    return this.submitTime;
  }

  @Override
  public String getTrackingUrl() {
    this.readLock.lock();
    
    try {
      if (this.currentAttempt != null) {
        return this.currentAttempt.getTrackingUrl();
      }
      return null;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public StringBuilder getDiagnostics() {
    this.readLock.lock();

    try {
      return this.diagnostics;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public int getMaxAppAttempts() {
    return this.maxAppAttempts;
  }

  @Override
  public void handle(RMAppEvent event) {

    this.writeLock.lock();

    try {
      ApplicationId appID = event.getApplicationId();
      LOG.debug("Processing event for " + appID + " of type "
          + event.getType());
      final RMAppState oldState = getState();
      try {
        /* keep the master in sync with the state machine */
        this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      if (oldState != getState()) {
        LOG.info(appID + " State change from " + oldState + " to "
            + getState());
      }
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Override
  public void recover(RMState state) {
    ApplicationState appState = state.getApplicationState().get(getApplicationId());
    LOG.info("Recovering app: " + getApplicationId() + " with " + 
            + appState.getAttemptCount() + " attempts");
    for(int i=0; i<appState.getAttemptCount(); ++i) {
      // create attempt
      createNewAttempt(false);
      // recover attempt
      ((RMAppAttemptImpl) currentAttempt).recover(state);
    }
  }

  @SuppressWarnings("unchecked")
  private void createNewAttempt(boolean startAttempt) {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, attempts.size() + 1);
    RMAppAttempt attempt =
        new RMAppAttemptImpl(appAttemptId, rmContext, scheduler, masterService,
          submissionContext, conf, user);
    attempts.put(appAttemptId, attempt);
    currentAttempt = attempt;
    if(startAttempt) {
      handler.handle(
          new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.START));
    }
  }
  
  private void processNodeUpdate(RMAppNodeUpdateType type, RMNode node) {
    NodeState nodeState = node.getState();
    updatedNodes.add(node);
    LOG.debug("Received node update event:" + type + " for node:" + node
        + " with state:" + nodeState);
  }

  private static class RMAppTransition implements
      SingleArcTransition<RMAppImpl, RMAppEvent> {
    public void transition(RMAppImpl app, RMAppEvent event) {
    };

  }

  private static final class RMAppNodeUpdateTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppNodeUpdateEvent nodeUpdateEvent = (RMAppNodeUpdateEvent) event;
      app.processNodeUpdate(nodeUpdateEvent.getUpdateType(),
          nodeUpdateEvent.getNode());
    };
  }
  
  private static final class StartAppAttemptTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      if (event.getType().equals(RMAppEventType.APP_SAVED)) {
        assert app.getState().equals(RMAppState.NEW_SAVING);
        RMAppStoredEvent storeEvent = (RMAppStoredEvent) event;
        if(storeEvent.getStoredException() != null) {
          // For HA this exception needs to be handled by giving up
          // master status if we got fenced
          LOG.error("Failed to store application: "
              + storeEvent.getApplicationId(),
              storeEvent.getStoredException());
          ExitUtil.terminate(1, storeEvent.getStoredException());
        }
      }

      app.createNewAttempt(true);
    };
  }

  private static final class RMAppFinishingTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.finishTime = System.currentTimeMillis();
    }
  }

  private static final class RMAppSavingTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      // If recovery is enabled then store the application information in a
      // non-blocking call so make sure that RM has stored the information
      // needed to restart the AM after RM restart without further client
      // communication
      LOG.info("Storing application with id " + app.applicationId);
      app.rmContext.getStateStore().storeApplication(app);
    }
  }

  private static class AppFinishedTransition extends FinalTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppFinishedAttemptEvent finishedEvent =
          (RMAppFinishedAttemptEvent)event;
      app.diagnostics.append(finishedEvent.getDiagnostics());
      super.transition(app, event);
    };
  }

  private static class AppKilledTransition extends FinalTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append("Application killed by user.");
      super.transition(app, event);
    };
  }

  private static class KillAppAndAttemptTransition extends AppKilledTransition {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.handler.handle(new RMAppAttemptEvent(app.currentAttempt.getAppAttemptId(),
          RMAppAttemptEventType.KILL));
      super.transition(app, event);
    }
  }
  private static final class AppRejectedTransition extends
      FinalTransition{
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppRejectedEvent rejectedEvent = (RMAppRejectedEvent)event;
      app.diagnostics.append(rejectedEvent.getMessage());
      super.transition(app, event);
    };
  }

  private static class FinalTransition extends RMAppTransition {

    private Set<NodeId> getNodesOnWhichAttemptRan(RMAppImpl app) {
      Set<NodeId> nodes = new HashSet<NodeId>();
      for (RMAppAttempt attempt : app.attempts.values()) {
        nodes.addAll(attempt.getRanNodes());
      }
      return nodes;
    }

    @SuppressWarnings("unchecked")
    public void transition(RMAppImpl app, RMAppEvent event) {
      Set<NodeId> nodes = getNodesOnWhichAttemptRan(app);
      for (NodeId nodeId : nodes) {
        app.handler.handle(
            new RMNodeCleanAppEvent(nodeId, app.applicationId));
      }
      if (app.getState() != RMAppState.FINISHING) {
        app.finishTime = System.currentTimeMillis();
      }
      app.handler.handle(
          new RMAppManagerEvent(app.applicationId,
          RMAppManagerEventType.APP_COMPLETED));
    };
  }

  private static final class AttemptFailedTransition implements
      MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    private final RMAppState initialState;

    public AttemptFailedTransition(RMAppState initialState) {
      this.initialState = initialState;
    }

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {

      RMAppFailedAttemptEvent failedEvent = ((RMAppFailedAttemptEvent) event);
      boolean retryApp = true;
      String msg = null;
      if (app.submissionContext.getUnmanagedAM()) {
        // RM does not manage the AM. Do not retry
        retryApp = false;
        msg = "Unmanaged application " + app.getApplicationId()
            + " failed due to " + failedEvent.getDiagnostics()
            + ". Failing the application.";
      } else if (app.attempts.size() >= app.maxAppAttempts) {
        retryApp = false;
        msg = "Application " + app.getApplicationId() + " failed "
            + app.maxAppAttempts + " times due to " + failedEvent.getDiagnostics()
            + ". Failing the application.";
      }

      if (retryApp) {
        app.createNewAttempt(true);
        return initialState;
      } else {
        LOG.info(msg);
        app.diagnostics.append(msg);
        // Inform the node for app-finish
        FINAL_TRANSITION.transition(app, event);
        return RMAppState.FAILED;
      }
    }

  }

  @Override
  public String getApplicationType() {
    return this.applicationType;
  }
}

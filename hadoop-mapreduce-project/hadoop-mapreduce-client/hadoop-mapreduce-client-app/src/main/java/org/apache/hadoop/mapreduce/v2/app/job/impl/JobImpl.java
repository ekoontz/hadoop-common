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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobInfoChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/** Implementation of Job interface. Maintains the state machines of Job.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class JobImpl implements org.apache.hadoop.mapreduce.v2.app.job.Job, 
  EventHandler<JobEvent> {

  private static final TaskAttemptCompletionEvent[]
    EMPTY_TASK_ATTEMPT_COMPLETION_EVENTS = new TaskAttemptCompletionEvent[0];

  private static final Log LOG = LogFactory.getLog(JobImpl.class);

  //The maximum fraction of fetch failures allowed for a map
  private static final double MAX_ALLOWED_FETCH_FAILURES_FRACTION = 0.5;

  // Maximum no. of fetch-failure notifications after which map task is failed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;
  
  //final fields
  private final ApplicationAttemptId applicationAttemptId;
  private final Clock clock;
  private final JobACLsManager aclsManager;
  private final String username;
  private final OutputCommitter committer;
  private final Map<JobACL, AccessControlList> jobACLs;
  private float setupWeight = 0.05f;
  private float cleanupWeight = 0.05f;
  private float mapWeight = 0.0f;
  private float reduceWeight = 0.0f;
  private final Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
  private final List<AMInfo> amInfos;
  private final Lock readLock;
  private final Lock writeLock;
  private final JobId jobId;
  private final String jobName;
  private final boolean newApiCommitter;
  private final org.apache.hadoop.mapreduce.JobID oldJobId;
  private final TaskAttemptListener taskAttemptListener;
  private final Object tasksSyncHandle = new Object();
  private final Set<TaskId> mapTasks = new LinkedHashSet<TaskId>();
  private final Set<TaskId> reduceTasks = new LinkedHashSet<TaskId>();
  /**
   * maps nodes to tasks that have run on those nodes
   */
  private final HashMap<NodeId, List<TaskAttemptId>> 
    nodesToSucceededTaskAttempts = new HashMap<NodeId, List<TaskAttemptId>>();

  private final EventHandler eventHandler;
  private final MRAppMetrics metrics;
  private final String userName;
  private final String queueName;
  private final long appSubmitTime;
  private final AppContext appContext;

  private boolean lazyTasksCopyNeeded = false;
  volatile Map<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task>();
  private Counters jobCounters = new Counters();
  private Object fullCountersLock = new Object();
  private Counters fullCounters = null;
  private Counters finalMapCounters = null;
  private Counters finalReduceCounters = null;
    // FIXME:  
    //
    // Can then replace task-level uber counters (MR-2424) with job-level ones
    // sent from LocalContainerLauncher, and eventually including a count of
    // of uber-AM attempts (probably sent from MRAppMaster).
  public JobConf conf;

  //fields initialized in init
  private FileSystem fs;
  private Path remoteJobSubmitDir;
  public Path remoteJobConfFile;
  private JobContext jobContext;
  private int allowedMapFailuresPercent = 0;
  private int allowedReduceFailuresPercent = 0;
  private List<TaskAttemptCompletionEvent> taskAttemptCompletionEvents;
  private List<TaskAttemptCompletionEvent> mapAttemptCompletionEvents;
  private final List<String> diagnostics = new ArrayList<String>();
  
  //task/attempt related datastructures
  private final Map<TaskId, Integer> successAttemptCompletionEventNoMap = 
    new HashMap<TaskId, Integer>();
  private final Map<TaskAttemptId, Integer> fetchFailuresMapping = 
    new HashMap<TaskAttemptId, Integer>();

  private static final DiagnosticsUpdateTransition
      DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final TaskAttemptCompletedEventTransition
      TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new TaskAttemptCompletedEventTransition();
  private static final CounterUpdateTransition COUNTER_UPDATE_TRANSITION =
      new CounterUpdateTransition();
  private static final UpdatedNodesTransition UPDATED_NODES_TRANSITION =
      new UpdatedNodesTransition();

  protected static final
    StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent> 
       stateMachineFactory
     = new StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent>
              (JobStateInternal.NEW)

          // Transitions from NEW state
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition
              (JobStateInternal.NEW,
              EnumSet.of(JobStateInternal.INITED, JobStateInternal.FAILED),
              JobEventType.JOB_INIT,
              new InitTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KillNewJobTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_UPDATED_NODES)
              
          // Transitions from INITED state
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.RUNNING,
              JobEventType.JOB_START,
              new StartTransition())
          .addTransition(JobStateInternal.INITED, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KillInitedJobTransition())
          .addTransition(JobStateInternal.INITED, JobStateInternal.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_UPDATED_NODES)
              
          // Transitions from RUNNING state
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition
              (JobStateInternal.RUNNING,
              EnumSet.of(JobStateInternal.RUNNING, JobStateInternal.SUCCEEDED, JobStateInternal.FAILED),
              JobEventType.JOB_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition
              (JobStateInternal.RUNNING,
              EnumSet.of(JobStateInternal.RUNNING, JobStateInternal.SUCCEEDED, JobStateInternal.FAILED),
              JobEventType.JOB_COMPLETED,
              new JobNoTasksCompletedTransition())
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_KILL, new KillTasksTransition())
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_UPDATED_NODES,
              UPDATED_NODES_TRANSITION)
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_MAP_TASK_RESCHEDULED,
              new MapTaskRescheduledTransition())
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
              new TaskAttemptFetchFailureTransition())
          .addTransition(
              JobStateInternal.RUNNING,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from KILL_WAIT state.
          .addTransition
              (JobStateInternal.KILL_WAIT,
              EnumSet.of(JobStateInternal.KILL_WAIT, JobStateInternal.KILLED),
              JobEventType.JOB_TASK_COMPLETED,
              new KillWaitTaskCompletedTransition())
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.KILL_WAIT,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              EnumSet.of(JobEventType.JOB_KILL,
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from SUCCEEDED state
          .addTransition(JobStateInternal.SUCCEEDED, JobStateInternal.SUCCEEDED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.SUCCEEDED, JobStateInternal.SUCCEEDED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.SUCCEEDED,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.SUCCEEDED, JobStateInternal.SUCCEEDED,
              EnumSet.of(JobEventType.JOB_KILL, 
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from FAILED state
          .addTransition(JobStateInternal.FAILED, JobStateInternal.FAILED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.FAILED, JobStateInternal.FAILED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.FAILED,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.FAILED, JobStateInternal.FAILED,
              EnumSet.of(JobEventType.JOB_KILL, 
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from KILLED state
          .addTransition(JobStateInternal.KILLED, JobStateInternal.KILLED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.KILLED, JobStateInternal.KILLED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.KILLED,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.KILLED, JobStateInternal.KILLED,
              EnumSet.of(JobEventType.JOB_KILL, 
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              JobStateInternal.ERROR,
              JobStateInternal.ERROR,
              EnumSet.of(JobEventType.JOB_INIT,
                  JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_DIAGNOSTIC_UPDATE,
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.INTERNAL_ERROR))
          .addTransition(JobStateInternal.ERROR, JobStateInternal.ERROR,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          // create the topology tables
          .installTopology();
 
  private final StateMachine<JobStateInternal, JobEventType, JobEvent> stateMachine;

  //changing fields while the job is running
  private int numMapTasks;
  private int numReduceTasks;
  private int completedTaskCount = 0;
  private int succeededMapTaskCount = 0;
  private int succeededReduceTaskCount = 0;
  private int failedMapTaskCount = 0;
  private int failedReduceTaskCount = 0;
  private int killedMapTaskCount = 0;
  private int killedReduceTaskCount = 0;
  private long startTime;
  private long finishTime;
  private float setupProgress;
  private float mapProgress;
  private float reduceProgress;
  private float cleanupProgress;
  private boolean isUber = false;

  private Credentials fsTokens;
  private Token<JobTokenIdentifier> jobToken;
  private JobTokenSecretManager jobTokenSecretManager;

  public JobImpl(JobId jobId, ApplicationAttemptId applicationAttemptId,
      Configuration conf, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials fsTokenCredentials, Clock clock,
      Map<TaskId, TaskInfo> completedTasksFromPreviousRun, MRAppMetrics metrics,
      OutputCommitter committer, boolean newApiCommitter, String userName,
      long appSubmitTime, List<AMInfo> amInfos, AppContext appContext) {
    this.applicationAttemptId = applicationAttemptId;
    this.jobId = jobId;
    this.jobName = conf.get(JobContext.JOB_NAME, "<missing job name>");
    this.conf = new JobConf(conf);
    this.metrics = metrics;
    this.clock = clock;
    this.completedTasksFromPreviousRun = completedTasksFromPreviousRun;
    this.amInfos = amInfos;
    this.appContext = appContext;
    this.userName = userName;
    this.queueName = conf.get(MRJobConfig.QUEUE_NAME, "default");
    this.appSubmitTime = appSubmitTime;
    this.oldJobId = TypeConverter.fromYarn(jobId);
    this.newApiCommitter = newApiCommitter;

    this.taskAttemptListener = taskAttemptListener;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.fsTokens = fsTokenCredentials;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.committer = committer;

    this.aclsManager = new JobACLsManager(conf);
    this.username = System.getProperty("user.name");
    this.jobACLs = aclsManager.constructJobACLs(conf);
    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  protected StateMachine<JobStateInternal, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  // Getter methods that make unit testing easier (package-scoped)
  OutputCommitter getCommitter() {
    return this.committer;
  }

  EventHandler getEventHandler() {
    return this.eventHandler;
  }

  JobContext getJobContext() {
    return this.jobContext;
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, 
      JobACL jobOperation) {
    AccessControlList jobACL = jobACLs.get(jobOperation);
    if (jobACL == null) {
      return true;
    }
    return aclsManager.checkAccess(callerUGI, jobOperation, username, jobACL);
  }

  @Override
  public Task getTask(TaskId taskID) {
    readLock.lock();
    try {
      return tasks.get(taskID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getCompletedMaps() {
    readLock.lock();
    try {
      return succeededMapTaskCount + failedMapTaskCount + killedMapTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getCompletedReduces() {
    readLock.lock();
    try {
      return succeededReduceTaskCount + failedReduceTaskCount 
                  + killedReduceTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isUber() {
    return isUber;
  }

  @Override
  public Counters getAllCounters() {

    readLock.lock();

    try {
      JobStateInternal state = getInternalState();
      if (state == JobStateInternal.ERROR || state == JobStateInternal.FAILED
          || state == JobStateInternal.KILLED || state == JobStateInternal.SUCCEEDED) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      Counters counters = new Counters();
      counters.incrAllCounters(jobCounters);
      return incrTaskCounters(counters, tasks.values());

    } finally {
      readLock.unlock();
    }
  }

  public static Counters incrTaskCounters(
      Counters counters, Collection<Task> tasks) {
    for (Task task : tasks) {
      counters.incrAllCounters(task.getCounters());
    }
    return counters;
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    return getAttemptCompletionEvents(taskAttemptCompletionEvents,
        fromEventId, maxEvents);
  }

  @Override
  public TaskAttemptCompletionEvent[] getMapAttemptCompletionEvents(
      int startIndex, int maxEvents) {
    return getAttemptCompletionEvents(mapAttemptCompletionEvents,
        startIndex, maxEvents);
  }

  private TaskAttemptCompletionEvent[] getAttemptCompletionEvents(
      List<TaskAttemptCompletionEvent> eventList,
      int startIndex, int maxEvents) {
    TaskAttemptCompletionEvent[] events = EMPTY_TASK_ATTEMPT_COMPLETION_EVENTS;
    readLock.lock();
    try {
      if (eventList.size() > startIndex) {
        int actualMax = Math.min(maxEvents,
            (eventList.size() - startIndex));
        events = eventList.subList(startIndex,
            actualMax + startIndex).toArray(events);
      }
      return events;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public JobReport getReport() {
    readLock.lock();
    try {
      JobState state = getState();

      // jobFile can be null if the job is not yet inited.
      String jobFile =
          remoteJobConfFile == null ? "" : remoteJobConfFile.toString();

      StringBuilder diagsb = new StringBuilder();
      for (String s : getDiagnostics()) {
        diagsb.append(s).append("\n");
      }

      if (getInternalState() == JobStateInternal.NEW) {
        return MRBuilderUtils.newJobReport(jobId, jobName, username, state,
            appSubmitTime, startTime, finishTime, setupProgress, 0.0f, 0.0f,
            cleanupProgress, jobFile, amInfos, isUber, diagsb.toString());
      }

      computeProgress();
      JobReport report = MRBuilderUtils.newJobReport(jobId, jobName, username,
          state, appSubmitTime, startTime, finishTime, setupProgress,
          this.mapProgress, this.reduceProgress,
          cleanupProgress, jobFile, amInfos, isUber, diagsb.toString());
      return report;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();
    try {
      computeProgress();
      return (this.setupProgress * this.setupWeight + this.cleanupProgress
          * this.cleanupWeight + this.mapProgress * this.mapWeight + this.reduceProgress
          * this.reduceWeight);
    } finally {
      this.readLock.unlock();
    }
  }

  private void computeProgress() {
    this.readLock.lock();
    try {
      float mapProgress = 0f;
      float reduceProgress = 0f;
      for (Task task : this.tasks.values()) {
        if (task.getType() == TaskType.MAP) {
          mapProgress += (task.isFinished() ? 1f : task.getProgress());
        } else {
          reduceProgress += (task.isFinished() ? 1f : task.getProgress());
        }
      }
      if (this.numMapTasks != 0) {
        mapProgress = mapProgress / this.numMapTasks;
      }
      if (this.numReduceTasks != 0) {
        reduceProgress = reduceProgress / this.numReduceTasks;
      }
      this.mapProgress = mapProgress;
      this.reduceProgress = reduceProgress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    synchronized (tasksSyncHandle) {
      lazyTasksCopyNeeded = true;
      return Collections.unmodifiableMap(tasks);
    }
  }

  @Override
  public Map<TaskId,Task> getTasks(TaskType taskType) {
    Map<TaskId, Task> localTasksCopy = tasks;
    Map<TaskId, Task> result = new HashMap<TaskId, Task>();
    Set<TaskId> tasksOfGivenType = null;
    readLock.lock();
    try {
      if (TaskType.MAP == taskType) {
        tasksOfGivenType = mapTasks;
      } else {
        tasksOfGivenType = reduceTasks;
      }
      for (TaskId taskID : tasksOfGivenType)
      result.put(taskID, localTasksCopy.get(taskID));
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public JobState getState() {
    readLock.lock();
    try {
      return getExternalState(getStateMachine().getCurrentState());
    } finally {
      readLock.unlock();
    }
  }

  protected void scheduleTasks(Set<TaskId> taskIDs) {
    for (TaskId taskID : taskIDs) {
      eventHandler.handle(new TaskEvent(taskID, 
          TaskEventType.T_SCHEDULE));
    }
  }

  @Override
  /**
   * The only entry point to change the Job.
   */
  public void handle(JobEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getJobId() + " of type "
          + event.getType());
    }
    try {
      writeLock.lock();
      JobStateInternal oldState = getInternalState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        addDiagnostic("Invalid event " + event.getType() + 
            " on Job " + this.jobId);
        eventHandler.handle(new JobEvent(this.jobId,
            JobEventType.INTERNAL_ERROR));
      }
      //notify the eventhandler of state change
      if (oldState != getInternalState()) {
        LOG.info(jobId + "Job Transitioned from " + oldState + " to "
                 + getInternalState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  @Private
  public JobStateInternal getInternalState() {
    readLock.lock();
    try {
     return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }
  
  private static JobState getExternalState(JobStateInternal smState) {
    if (smState == JobStateInternal.KILL_WAIT) {
      return JobState.KILLED;
    } else {
      return JobState.valueOf(smState.name());
    }
  }
  
  
  //helpful in testing
  protected void addTask(Task task) {
    synchronized (tasksSyncHandle) {
      if (lazyTasksCopyNeeded) {
        Map<TaskId, Task> newTasks = new LinkedHashMap<TaskId, Task>();
        newTasks.putAll(tasks);
        tasks = newTasks;
        lazyTasksCopyNeeded = false;
      }
    }
    tasks.put(task.getID(), task);
    if (task.getType() == TaskType.MAP) {
      mapTasks.add(task.getID());
    } else if (task.getType() == TaskType.REDUCE) {
      reduceTasks.add(task.getID());
    }
    metrics.waitingTask(task);
  }

  void setFinishTime() {
    finishTime = clock.getTime();
  }

  void logJobHistoryFinishedEvent() {
    this.setFinishTime();
    JobFinishedEvent jfe = createJobFinishedEvent(this);
    LOG.info("Calling handler for JobFinishedEvent ");
    this.getEventHandler().handle(new JobHistoryEvent(this.jobId, jfe));    
  }
  
  /**
   * Create the default file System for this job.
   * @param conf the conf object
   * @return the default filesystem for this job
   * @throws IOException
   */
  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }
  
  static JobStateInternal checkJobCompleteSuccess(JobImpl job) {
    // check for Job success
    if (job.completedTaskCount == job.tasks.size()) {
      try {
        // Commit job & do cleanup
        job.getCommitter().commitJob(job.getJobContext());
      } catch (IOException e) {
        LOG.error("Could not do commit for Job", e);
        job.addDiagnostic("Job commit failed: " + e.getMessage());
        job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
        return job.finished(JobStateInternal.FAILED);
      }
      job.logJobHistoryFinishedEvent();
      return job.finished(JobStateInternal.SUCCEEDED);
    }
    return null;
  }

  JobStateInternal finished(JobStateInternal finalState) {
    if (getInternalState() == JobStateInternal.RUNNING) {
      metrics.endRunningJob(this);
    }
    if (finishTime == 0) setFinishTime();
    eventHandler.handle(new JobFinishEvent(jobId));

    switch (finalState) {
      case KILLED:
        metrics.killedJob(this);
        break;
      case ERROR:
      case FAILED:
        metrics.failedJob(this);
        break;
      case SUCCEEDED:
        metrics.completedJob(this);
        break;
      default:
        throw new IllegalArgumentException("Illegal job state: " + finalState);
    }
    return finalState;
  }

  @Override
  public String getUserName() {
    return userName;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.job.Job#getConfFile()
   */
  @Override
  public Path getConfFile() {
    return remoteJobConfFile;
  }
  
  @Override
  public String getName() {
    return jobName;
  }

  @Override
  public int getTotalMaps() {
    return mapTasks.size();  //FIXME: why indirection? return numMapTasks...
                             // unless race?  how soon can this get called?
  }

  @Override
  public int getTotalReduces() {
    return reduceTasks.size();  //FIXME: why indirection? return numReduceTasks
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.job.Job#getJobACLs()
   */
  @Override
  public Map<JobACL, AccessControlList> getJobACLs() {
    return Collections.unmodifiableMap(jobACLs);
  }
  
  @Override
  public List<AMInfo> getAMInfos() {
    return amInfos;
  }

  /**
   * Decide whether job can be run in uber mode based on various criteria.
   * @param dataInputLength Total length for all splits
   */
  private void makeUberDecision(long dataInputLength) {
    //FIXME:  need new memory criterion for uber-decision (oops, too late here;
    // until AM-resizing supported,
    // must depend on job client to pass fat-slot needs)
    // these are no longer "system" settings, necessarily; user may override
    int sysMaxMaps = conf.getInt(MRJobConfig.JOB_UBERTASK_MAXMAPS, 9);

    //FIXME: handling multiple reduces within a single AM does not seem to
    //work.
    int sysMaxReduces = conf.getInt(MRJobConfig.JOB_UBERTASK_MAXREDUCES, 1);
    boolean isValidUberMaxReduces = (sysMaxReduces == 0)
        || (sysMaxReduces == 1);

    long sysMaxBytes = conf.getLong(MRJobConfig.JOB_UBERTASK_MAXBYTES,
        fs.getDefaultBlockSize(this.remoteJobSubmitDir)); // FIXME: this is wrong; get FS from
                                   // [File?]InputFormat and default block size
                                   // from that

    long sysMemSizeForUberSlot =
        conf.getInt(MRJobConfig.MR_AM_VMEM_MB,
            MRJobConfig.DEFAULT_MR_AM_VMEM_MB);

    boolean uberEnabled =
        conf.getBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    boolean smallNumMapTasks = (numMapTasks <= sysMaxMaps);
    boolean smallNumReduceTasks = (numReduceTasks <= sysMaxReduces);
    boolean smallInput = (dataInputLength <= sysMaxBytes);
    // ignoring overhead due to UberAM and statics as negligible here:
    boolean smallMemory =
        ( (Math.max(conf.getLong(MRJobConfig.MAP_MEMORY_MB, 0),
            conf.getLong(MRJobConfig.REDUCE_MEMORY_MB, 0))
            <= sysMemSizeForUberSlot)
            || (sysMemSizeForUberSlot == JobConf.DISABLED_MEMORY_LIMIT));
    boolean notChainJob = !isChainJob(conf);

    // User has overall veto power over uberization, or user can modify
    // limits (overriding system settings and potentially shooting
    // themselves in the head).  Note that ChainMapper/Reducer are
    // fundamentally incompatible with MR-1220; they employ a blocking
    // queue between the maps/reduces and thus require parallel execution,
    // while "uber-AM" (MR AM + LocalContainerLauncher) loops over tasks
    // and thus requires sequential execution.
    isUber = uberEnabled && smallNumMapTasks && smallNumReduceTasks
        && smallInput && smallMemory && notChainJob && isValidUberMaxReduces;

    if (isUber) {
      LOG.info("Uberizing job " + jobId + ": " + numMapTasks + "m+"
          + numReduceTasks + "r tasks (" + dataInputLength
          + " input bytes) will run sequentially on single node.");

      // make sure reduces are scheduled only after all map are completed
      conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART,
                        1.0f);
      // uber-subtask attempts all get launched on same node; if one fails,
      // probably should retry elsewhere, i.e., move entire uber-AM:  ergo,
      // limit attempts to 1 (or at most 2?  probably not...)
      conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
      conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 1);

      // disable speculation
      conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
      conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
    } else {
      StringBuilder msg = new StringBuilder();
      msg.append("Not uberizing ").append(jobId).append(" because:");
      if (!uberEnabled)
        msg.append(" not enabled;");
      if (!smallNumMapTasks)
        msg.append(" too many maps;");
      if (!smallNumReduceTasks)
        msg.append(" too many reduces;");
      if (!smallInput)
        msg.append(" too much input;");
      if (!smallMemory)
        msg.append(" too much RAM;");
      if (!notChainJob)
        msg.append(" chainjob;");
      if (!isValidUberMaxReduces)
        msg.append(" not supported uber max reduces");
      LOG.info(msg.toString());
    }
  }
  
  /**
   * ChainMapper and ChainReducer must execute in parallel, so they're not
   * compatible with uberization/LocalContainerLauncher (100% sequential).
   */
  private boolean isChainJob(Configuration conf) {
    boolean isChainJob = false;
    try {
      String mapClassName = conf.get(MRJobConfig.MAP_CLASS_ATTR);
      if (mapClassName != null) {
        Class<?> mapClass = Class.forName(mapClassName);
        if (ChainMapper.class.isAssignableFrom(mapClass))
          isChainJob = true;
      }
    } catch (ClassNotFoundException cnfe) {
      // don't care; assume it's not derived from ChainMapper
    }
    try {
      String reduceClassName = conf.get(MRJobConfig.REDUCE_CLASS_ATTR);
      if (reduceClassName != null) {
        Class<?> reduceClass = Class.forName(reduceClassName);
        if (ChainReducer.class.isAssignableFrom(reduceClass))
          isChainJob = true;
      }
    } catch (ClassNotFoundException cnfe) {
      // don't care; assume it's not derived from ChainReducer
    }
    return isChainJob;
  }
  
  private void actOnUnusableNode(NodeId nodeId, NodeState nodeState) {
    // rerun previously successful map tasks
    List<TaskAttemptId> taskAttemptIdList = nodesToSucceededTaskAttempts.get(nodeId);
    if(taskAttemptIdList != null) {
      String mesg = "TaskAttempt killed because it ran on unusable node "
          + nodeId;
      for(TaskAttemptId id : taskAttemptIdList) {
        if(TaskType.MAP == id.getTaskId().getTaskType()) {
          // reschedule only map tasks because their outputs maybe unusable
          LOG.info(mesg + ". AttemptId:" + id);
          eventHandler.handle(new TaskAttemptKillEvent(id, mesg));
        }
      }
    }
    // currently running task attempts on unusable nodes are handled in
    // RMContainerAllocator
  }

  /*
  private int getBlockSize() {
    String inputClassName = conf.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR);
    if (inputClassName != null) {
      Class<?> inputClass - Class.forName(inputClassName);
      if (FileInputFormat<K, V>)
    }
  }
  */

  public static class InitTransition 
      implements MultipleArcTransition<JobImpl, JobEvent, JobStateInternal> {

    /**
     * Note that this transition method is called directly (and synchronously)
     * by MRAppMaster's init() method (i.e., no RPC, no thread-switching;
     * just plain sequential call within AM context), so we can trigger
     * modifications in AM state from here (at least, if AM is written that
     * way; MR version is).
     */
    @Override
    public JobStateInternal transition(JobImpl job, JobEvent event) {
      job.metrics.submittedJob(job);
      job.metrics.preparingJob(job);
      try {
        setup(job);
        job.fs = job.getFileSystem(job.conf);

        //log to job history
        JobSubmittedEvent jse = new JobSubmittedEvent(job.oldJobId,
              job.conf.get(MRJobConfig.JOB_NAME, "test"), 
            job.conf.get(MRJobConfig.USER_NAME, "mapred"),
            job.appSubmitTime,
            job.remoteJobConfFile.toString(),
            job.jobACLs, job.queueName);
        job.eventHandler.handle(new JobHistoryEvent(job.jobId, jse));
        //TODO JH Verify jobACLs, UserName via UGI?

        TaskSplitMetaInfo[] taskSplitMetaInfo = createSplits(job, job.jobId);
        job.numMapTasks = taskSplitMetaInfo.length;
        job.numReduceTasks = job.conf.getInt(MRJobConfig.NUM_REDUCES, 0);

        if (job.numMapTasks == 0 && job.numReduceTasks == 0) {
          job.addDiagnostic("No of maps and reduces are 0 " + job.jobId);
        } else if (job.numMapTasks == 0) {
          job.reduceWeight = 0.9f;
        } else if (job.numReduceTasks == 0) {
          job.mapWeight = 0.9f;
        } else {
          job.mapWeight = job.reduceWeight = 0.45f;
        }

        checkTaskLimits();

        if (job.newApiCommitter) {
          job.jobContext = new JobContextImpl(job.conf,
              job.oldJobId);
        } else {
          job.jobContext = new org.apache.hadoop.mapred.JobContextImpl(
              job.conf, job.oldJobId);
        }
        
        long inputLength = 0;
        for (int i = 0; i < job.numMapTasks; ++i) {
          inputLength += taskSplitMetaInfo[i].getInputDataLength();
        }

        job.makeUberDecision(inputLength);
        
        job.taskAttemptCompletionEvents =
            new ArrayList<TaskAttemptCompletionEvent>(
                job.numMapTasks + job.numReduceTasks + 10);
        job.mapAttemptCompletionEvents =
            new ArrayList<TaskAttemptCompletionEvent>(job.numMapTasks + 10);

        job.allowedMapFailuresPercent =
            job.conf.getInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 0);
        job.allowedReduceFailuresPercent =
            job.conf.getInt(MRJobConfig.REDUCE_FAILURES_MAXPERCENT, 0);

        // do the setup
        job.committer.setupJob(job.jobContext);
        job.setupProgress = 1.0f;

        // create the Tasks but don't start them yet
        createMapTasks(job, inputLength, taskSplitMetaInfo);
        createReduceTasks(job);

        job.metrics.endPreparingJob(job);
        return JobStateInternal.INITED;
        //TODO XXX Should JobInitedEvent be generated here (instead of in StartTransition)

      } catch (IOException e) {
        LOG.warn("Job init failed", e);
        job.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
        job.metrics.endPreparingJob(job);
        return job.finished(JobStateInternal.FAILED);
      }
    }

    protected void setup(JobImpl job) throws IOException {

      String oldJobIDString = job.oldJobId.toString();
      String user = 
        UserGroupInformation.getCurrentUser().getShortUserName();
      Path path = MRApps.getStagingAreaDir(job.conf, user);
      if(LOG.isDebugEnabled()) {
        LOG.debug("startJobs: parent=" + path + " child=" + oldJobIDString);
      }

      job.remoteJobSubmitDir =
          FileSystem.get(job.conf).makeQualified(
              new Path(path, oldJobIDString));
      job.remoteJobConfFile =
          new Path(job.remoteJobSubmitDir, MRJobConfig.JOB_CONF_FILE);

      // Prepare the TaskAttemptListener server for authentication of Containers
      // TaskAttemptListener gets the information via jobTokenSecretManager.
      JobTokenIdentifier identifier =
          new JobTokenIdentifier(new Text(oldJobIDString));
      job.jobToken =
          new Token<JobTokenIdentifier>(identifier, job.jobTokenSecretManager);
      job.jobToken.setService(identifier.getJobId());
      // Add it to the jobTokenSecretManager so that TaskAttemptListener server
      // can authenticate containers(tasks)
      job.jobTokenSecretManager.addTokenForJob(oldJobIDString, job.jobToken);
      LOG.info("Adding job token for " + oldJobIDString
          + " to jobTokenSecretManager");

      // Upload the jobTokens onto the remote FS so that ContainerManager can
      // localize it to be used by the Containers(tasks)
      Credentials tokenStorage = new Credentials();
      TokenCache.setJobToken(job.jobToken, tokenStorage);

      if (UserGroupInformation.isSecurityEnabled()) {
        tokenStorage.addAll(job.fsTokens);
      }
    }

    private void createMapTasks(JobImpl job, long inputLength,
                                TaskSplitMetaInfo[] splits) {
      for (int i=0; i < job.numMapTasks; ++i) {
        TaskImpl task =
            new MapTaskImpl(job.jobId, i,
                job.eventHandler, 
                job.remoteJobConfFile, 
                job.conf, splits[i], 
                job.taskAttemptListener, 
                job.committer, job.jobToken, job.fsTokens,
                job.clock, job.completedTasksFromPreviousRun, 
                job.applicationAttemptId.getAttemptId(),
                job.metrics, job.appContext);
        job.addTask(task);
      }
      LOG.info("Input size for job " + job.jobId + " = " + inputLength
          + ". Number of splits = " + splits.length);
    }

    private void createReduceTasks(JobImpl job) {
      for (int i = 0; i < job.numReduceTasks; i++) {
        TaskImpl task =
            new ReduceTaskImpl(job.jobId, i,
                job.eventHandler, 
                job.remoteJobConfFile, 
                job.conf, job.numMapTasks, 
                job.taskAttemptListener, job.committer, job.jobToken,
                job.fsTokens, job.clock,
                job.completedTasksFromPreviousRun, 
                job.applicationAttemptId.getAttemptId(),
                job.metrics, job.appContext);
        job.addTask(task);
      }
      LOG.info("Number of reduces for job " + job.jobId + " = "
          + job.numReduceTasks);
    }

    protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobId jobId) {
      TaskSplitMetaInfo[] allTaskSplitMetaInfo;
      try {
        allTaskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
            job.oldJobId, job.fs, 
            job.conf, 
            job.remoteJobSubmitDir);
      } catch (IOException e) {
        throw new YarnException(e);
      }
      return allTaskSplitMetaInfo;
    }

    /**
     * If the number of tasks are greater than the configured value
     * throw an exception that will fail job initialization
     */
    private void checkTaskLimits() {
      // no code, for now
    }
  } // end of InitTransition

  public static class StartTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    /**
     * This transition executes in the event-dispatcher thread, though it's
     * triggered in MRAppMaster's startJobs() method.
     */
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.startTime = job.clock.getTime();
      job.scheduleTasks(job.mapTasks);  // schedule (i.e., start) the maps
      job.scheduleTasks(job.reduceTasks);
      JobInitedEvent jie =
        new JobInitedEvent(job.oldJobId,
             job.startTime,
             job.numMapTasks, job.numReduceTasks,
             job.getState().toString(),
             job.isUber()); //Will transition to state running. Currently in INITED
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jie));
      JobInfoChangeEvent jice = new JobInfoChangeEvent(job.oldJobId,
          job.appSubmitTime, job.startTime);
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jice));
      job.metrics.runningJob(job);

			// If we have no tasks, just transition to job completed
      if (job.numReduceTasks == 0 && job.numMapTasks == 0) {
        job.eventHandler.handle(new JobEvent(job.jobId, JobEventType.JOB_COMPLETED));
      }
    }
  }

  protected void abortJob(
      org.apache.hadoop.mapreduce.JobStatus.State finalState) {
    try {
      committer.abortJob(jobContext, finalState);
    } catch (IOException e) {
      LOG.warn("Could not abortJob", e);
    }
    if (finishTime == 0) setFinishTime();
    cleanupProgress = 1.0f;
    JobUnsuccessfulCompletionEvent unsuccessfulJobEvent =
      new JobUnsuccessfulCompletionEvent(oldJobId,
          finishTime,
          succeededMapTaskCount,
          succeededReduceTaskCount,
          finalState.toString());
    eventHandler.handle(new JobHistoryEvent(jobId, unsuccessfulJobEvent));
  }
    
  // JobFinishedEvent triggers the move of the history file out of the staging
  // area. May need to create a new event type for this if JobFinished should 
  // not be generated for KilledJobs, etc.
  private static JobFinishedEvent createJobFinishedEvent(JobImpl job) {

    job.mayBeConstructFinalFullCounters();

    JobFinishedEvent jfe = new JobFinishedEvent(
        job.oldJobId, job.finishTime,
        job.succeededMapTaskCount, job.succeededReduceTaskCount,
        job.failedMapTaskCount, job.failedReduceTaskCount,
        job.finalMapCounters,
        job.finalReduceCounters,
        job.fullCounters);
    return jfe;
  }

  private void mayBeConstructFinalFullCounters() {
    // Calculating full-counters. This should happen only once for the job.
    synchronized (this.fullCountersLock) {
      if (this.fullCounters != null) {
        // Already constructed. Just return.
        return;
      }
      this.constructFinalFullcounters();
    }
  }

  @Private
  public void constructFinalFullcounters() {
    this.fullCounters = new Counters();
    this.finalMapCounters = new Counters();
    this.finalReduceCounters = new Counters();
    this.fullCounters.incrAllCounters(jobCounters);
    for (Task t : this.tasks.values()) {
      Counters counters = t.getCounters();
      switch (t.getType()) {
      case MAP:
        this.finalMapCounters.incrAllCounters(counters);
        break;
      case REDUCE:
        this.finalReduceCounters.incrAllCounters(counters);
        break;
      default:
        throw new IllegalStateException("Task type neither map nor reduce: " + 
            t.getType());
      }
      this.fullCounters.incrAllCounters(counters);
    }
  }

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class KillNewJobTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.setFinishTime();
      JobUnsuccessfulCompletionEvent failedEvent =
          new JobUnsuccessfulCompletionEvent(job.oldJobId,
              job.finishTime, 0, 0,
              JobStateInternal.KILLED.toString());
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(JobStateInternal.KILLED);
    }
  }

  private static class KillInitedJobTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.KILLED);
      job.addDiagnostic("Job received Kill in INITED state.");
      job.finished(JobStateInternal.KILLED);
    }
  }

  private static class KillTasksTransition
      implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.addDiagnostic("Job received Kill while in RUNNING state.");
      for (Task task : job.tasks.values()) {
        job.eventHandler.handle(
            new TaskEvent(task.getID(), TaskEventType.T_KILL));
      }
      job.metrics.endRunningJob(job);
    }
  }

  private static class TaskAttemptCompletedEventTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      TaskAttemptCompletionEvent tce = 
        ((JobTaskAttemptCompletedEvent) event).getCompletionEvent();
      // Add the TaskAttemptCompletionEvent
      //eventId is equal to index in the arraylist
      tce.setEventId(job.taskAttemptCompletionEvents.size());
      job.taskAttemptCompletionEvents.add(tce);
      if (TaskType.MAP.equals(tce.getAttemptId().getTaskId().getTaskType())) {
        job.mapAttemptCompletionEvents.add(tce);
      }
      
      TaskAttemptId attemptId = tce.getAttemptId();
      TaskId taskId = attemptId.getTaskId();
      //make the previous completion event as obsolete if it exists
      Object successEventNo = 
        job.successAttemptCompletionEventNoMap.remove(taskId);
      if (successEventNo != null) {
        TaskAttemptCompletionEvent successEvent = 
          job.taskAttemptCompletionEvents.get((Integer) successEventNo);
        successEvent.setStatus(TaskAttemptCompletionEventStatus.OBSOLETE);
      }
      
      // if this attempt is not successful then why is the previous successful 
      // attempt being removed above - MAPREDUCE-4330
      if (TaskAttemptCompletionEventStatus.SUCCEEDED.equals(tce.getStatus())) {
        job.successAttemptCompletionEventNoMap.put(taskId, tce.getEventId());
        
        // here we could have simply called Task.getSuccessfulAttempt() but
        // the event that triggers this code is sent before
        // Task.successfulAttempt is set and so there is no guarantee that it
        // will be available now
        Task task = job.tasks.get(taskId);
        TaskAttempt attempt = task.getAttempt(attemptId);
        NodeId nodeId = attempt.getNodeId();
        assert (nodeId != null); // node must exist for a successful event
        List<TaskAttemptId> taskAttemptIdList = job.nodesToSucceededTaskAttempts
            .get(nodeId);
        if (taskAttemptIdList == null) {
          taskAttemptIdList = new ArrayList<TaskAttemptId>();
          job.nodesToSucceededTaskAttempts.put(nodeId, taskAttemptIdList);
        }
        taskAttemptIdList.add(attempt.getID());
      }
    }
  }

  private static class TaskAttemptFetchFailureTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobTaskAttemptFetchFailureEvent fetchfailureEvent = 
        (JobTaskAttemptFetchFailureEvent) event;
      for (org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId mapId : 
            fetchfailureEvent.getMaps()) {
        Integer fetchFailures = job.fetchFailuresMapping.get(mapId);
        fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures+1);
        job.fetchFailuresMapping.put(mapId, fetchFailures);
        
        //get number of shuffling reduces
        int shufflingReduceTasks = 0;
        for (TaskId taskId : job.reduceTasks) {
          Task task = job.tasks.get(taskId);
          if (TaskState.RUNNING.equals(task.getState())) {
            for(TaskAttempt attempt : task.getAttempts().values()) {
              if(attempt.getReport().getPhase() == Phase.SHUFFLE) {
                shufflingReduceTasks++;
                break;
              }
            }
          }
        }
        
        float failureRate = shufflingReduceTasks == 0 ? 1.0f : 
          (float) fetchFailures / shufflingReduceTasks;
        // declare faulty if fetch-failures >= max-allowed-failures
        boolean isMapFaulty =
            (failureRate >= MAX_ALLOWED_FETCH_FAILURES_FRACTION);
        if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS && isMapFaulty) {
          LOG.info("Too many fetch-failures for output of task attempt: " + 
              mapId + " ... raising fetch failure to map");
          job.eventHandler.handle(new TaskAttemptEvent(mapId, 
              TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE));
          job.fetchFailuresMapping.remove(mapId);
        }
      }
    }
  }

  private static class TaskCompletedTransition implements
      MultipleArcTransition<JobImpl, JobEvent, JobStateInternal> {

    @Override
    public JobStateInternal transition(JobImpl job, JobEvent event) {
      job.completedTaskCount++;
      LOG.info("Num completed Tasks: " + job.completedTaskCount);
      JobTaskEvent taskEvent = (JobTaskEvent) event;
      Task task = job.tasks.get(taskEvent.getTaskID());
      if (taskEvent.getState() == TaskState.SUCCEEDED) {
        taskSucceeded(job, task);
      } else if (taskEvent.getState() == TaskState.FAILED) {
        taskFailed(job, task);
      } else if (taskEvent.getState() == TaskState.KILLED) {
        taskKilled(job, task);
      }

      return checkJobForCompletion(job);
    }

    protected JobStateInternal checkJobForCompletion(JobImpl job) {
      //check for Job failure
      if (job.failedMapTaskCount*100 > 
        job.allowedMapFailuresPercent*job.numMapTasks ||
        job.failedReduceTaskCount*100 > 
        job.allowedReduceFailuresPercent*job.numReduceTasks) {
        job.setFinishTime();

        String diagnosticMsg = "Job failed as tasks failed. " +
            "failedMaps:" + job.failedMapTaskCount + 
            " failedReduces:" + job.failedReduceTaskCount;
        LOG.info(diagnosticMsg);
        job.addDiagnostic(diagnosticMsg);
        job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
        return job.finished(JobStateInternal.FAILED);
      }
      
      JobStateInternal jobCompleteSuccess = JobImpl.checkJobCompleteSuccess(job);
      if (jobCompleteSuccess != null) {
        return jobCompleteSuccess;
      }
      
      //return the current state, Job not finished yet
      return job.getInternalState();
    }

    private void taskSucceeded(JobImpl job, Task task) {
      if (task.getType() == TaskType.MAP) {
        job.succeededMapTaskCount++;
      } else {
        job.succeededReduceTaskCount++;
      }
      job.metrics.completedTask(task);
    }
  
    private void taskFailed(JobImpl job, Task task) {
      if (task.getType() == TaskType.MAP) {
        job.failedMapTaskCount++;
      } else if (task.getType() == TaskType.REDUCE) {
        job.failedReduceTaskCount++;
      }
      job.addDiagnostic("Task failed " + task.getID());
      job.metrics.failedTask(task);
    }

    private void taskKilled(JobImpl job, Task task) {
      if (task.getType() == TaskType.MAP) {
        job.killedMapTaskCount++;
      } else if (task.getType() == TaskType.REDUCE) {
        job.killedReduceTaskCount++;
      }
      job.metrics.killedTask(task);
    }
  }

  // Transition class for handling jobs with no tasks
  static class JobNoTasksCompletedTransition implements
  MultipleArcTransition<JobImpl, JobEvent, JobStateInternal> {

    @Override
    public JobStateInternal transition(JobImpl job, JobEvent event) {
      JobStateInternal jobCompleteSuccess = JobImpl.checkJobCompleteSuccess(job);
      if (jobCompleteSuccess != null) {
        return jobCompleteSuccess;
      }
      
      // Return the current state, Job not finished yet
      return job.getInternalState();
    }
  }

  private static class MapTaskRescheduledTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      //succeeded map task is restarted back
      job.completedTaskCount--;
      job.succeededMapTaskCount--;
    }
  }

  private static class KillWaitTaskCompletedTransition extends  
      TaskCompletedTransition {
    @Override
    protected JobStateInternal checkJobForCompletion(JobImpl job) {
      if (job.completedTaskCount == job.tasks.size()) {
        job.setFinishTime();
        job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.KILLED);
        return job.finished(JobStateInternal.KILLED);
      }
      //return the current state, Job not finished yet
      return job.getInternalState();
    }
  }

  protected void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }
  
  private static class DiagnosticsUpdateTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.addDiagnostic(((JobDiagnosticsUpdateEvent) event)
          .getDiagnosticUpdate());
    }
  }
  
  private static class CounterUpdateTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobCounterUpdateEvent jce = (JobCounterUpdateEvent) event;
      for (JobCounterUpdateEvent.CounterIncrementalUpdate ci : jce
          .getCounterUpdates()) {
        job.jobCounters.findCounter(ci.getCounterKey()).increment(
          ci.getIncrementValue());
      }
    }
  }
  
  private static class UpdatedNodesTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobUpdatedNodesEvent updateEvent = (JobUpdatedNodesEvent) event;
      for(NodeReport nr: updateEvent.getUpdatedNodes()) {
        NodeState nodeState = nr.getNodeState();
        if(nodeState.isUnusable()) {
          // act on the updates
          job.actOnUnusableNode(nr.getNodeId(), nodeState);
        }
      }
    }
  }
  
  private static class InternalErrorTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      //TODO Is this JH event required.
      job.setFinishTime();
      JobUnsuccessfulCompletionEvent failedEvent =
          new JobUnsuccessfulCompletionEvent(job.oldJobId,
              job.finishTime, 0, 0,
              JobStateInternal.ERROR.toString());
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(JobStateInternal.ERROR);
    }
  }

  @Override
  public Configuration loadConfFile() throws IOException {
    Path confPath = getConfFile();
    FileContext fc = FileContext.getFileContext(confPath.toUri(), conf);
    Configuration jobConf = new Configuration(false);
    jobConf.addResource(fc.open(confPath), confPath.toString());
    return jobConf;
  }
}

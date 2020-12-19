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

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;

@SuppressWarnings({ "rawtypes" })
public class ReduceTaskImpl extends TaskImpl {
  
  private final int numMapTasks;

  public ReduceTaskImpl(JobId jobId, int partition,
      EventHandler eventHandler, Path jobFile, JobConf conf,
      int numMapTasks, TaskAttemptListener taskAttemptListener,
      OutputCommitter committer, Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      Map<TaskId, TaskInfo> completedTasksFromPreviousRun, int startCount,
      MRAppMetrics metrics, AppContext appContext) {
    super(jobId, TaskType.REDUCE, partition, eventHandler, jobFile, conf,
        taskAttemptListener, committer, jobToken, credentials, clock,
        completedTasksFromPreviousRun, startCount, metrics, appContext);
    this.numMapTasks = numMapTasks;
  }

  @Override
  protected int getMaxAttempts() {
    return conf.getInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 4);
  }

  @Override
  protected TaskAttemptImpl createAttempt() {
    return new ReduceTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, numMapTasks, conf, taskAttemptListener,
        committer, jobToken, credentials, clock, appContext);
  }

  @Override
  public TaskType getType() {
    return TaskType.REDUCE;
  }

}

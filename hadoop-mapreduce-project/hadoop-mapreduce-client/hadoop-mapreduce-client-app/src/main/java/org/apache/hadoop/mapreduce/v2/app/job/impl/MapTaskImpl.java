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
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
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
public class MapTaskImpl extends TaskImpl {

  private final TaskSplitMetaInfo taskSplitMetaInfo;

  public MapTaskImpl(JobId jobId, int partition, EventHandler eventHandler,
      Path remoteJobConfFile, JobConf conf,
      TaskSplitMetaInfo taskSplitMetaInfo,
      TaskAttemptListener taskAttemptListener, OutputCommitter committer,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      Map<TaskId, TaskInfo> completedTasksFromPreviousRun, int startCount,
      MRAppMetrics metrics, AppContext appContext) {
    super(jobId, TaskType.MAP, partition, eventHandler, remoteJobConfFile,
        conf, taskAttemptListener, committer, jobToken, credentials, clock,
        completedTasksFromPreviousRun, startCount, metrics, appContext);
    this.taskSplitMetaInfo = taskSplitMetaInfo;
  }

  @Override
  protected int getMaxAttempts() {
    return conf.getInt(MRJobConfig.MAP_MAX_ATTEMPTS, 4);
  }

  @Override
  protected TaskAttemptImpl createAttempt() {
    return new MapTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, taskSplitMetaInfo, conf, taskAttemptListener,
        committer, jobToken, credentials, clock, appContext);
  }

  @Override
  public TaskType getType() {
    return TaskType.MAP;
  }

  protected TaskSplitMetaInfo getTaskSplitMetaInfo() {
    return this.taskSplitMetaInfo;
  }

  /**
   * @return a String formatted as a comma-separated list of splits.
   */
  @Override
  protected String getSplitsAsString() {
    String[] splits = getTaskSplitMetaInfo().getLocations();
    if (splits == null || splits.length == 0)
    return "";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < splits.length; i++) {
      if (i != 0) sb.append(",");
      sb.append(splits[i]);
    }
    return sb.toString();
  }
}

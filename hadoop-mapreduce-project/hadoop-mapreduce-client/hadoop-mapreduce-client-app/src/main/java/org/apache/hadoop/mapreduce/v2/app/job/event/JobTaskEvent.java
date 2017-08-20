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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;


public class JobTaskEvent extends JobEvent {

  private TaskId taskID;
  private TaskState taskState;
  private boolean recompute = false;

  public JobTaskEvent(TaskId taskID, TaskState taskState) {
    super(taskID.getJobId(), JobEventType.JOB_TASK_COMPLETED);
    this.taskID = taskID;
    this.taskState = taskState;
  }

  public TaskId getTaskID() {
    return taskID;
  }

  public TaskState getState() {
    return taskState;
  }

  public boolean isRecompute() {
    return recompute;
  }

  public void setRecompute(boolean recompute) {
    this.recompute = recompute;
  }
}

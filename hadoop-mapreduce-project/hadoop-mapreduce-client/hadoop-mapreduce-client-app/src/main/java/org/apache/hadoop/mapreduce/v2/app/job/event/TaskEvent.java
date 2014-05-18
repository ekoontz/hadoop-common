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

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.mapred.MapSpillInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

/**
 * this class encapsulates task related events.
 *
 */
public class TaskEvent extends AbstractEvent<TaskEventType> {

  private TaskId taskID;
  
  private MapSpillInfo spillInfo;
  
  private TaskAttemptID spillMapAttemptID;
  
  private String host;
  private int port;

  public TaskEvent(TaskId taskID, TaskEventType type) {
    super(type);
    this.taskID = taskID;
  }

  public TaskId getTaskID() {
    return taskID;
  }

  public MapSpillInfo getSpillInfo() {
    return spillInfo;
  }

  public void setSpillInfo(MapSpillInfo spillInfo) {
    this.spillInfo = spillInfo;
  }

  public TaskAttemptID getSpillMapAttemptID() {
    return spillMapAttemptID;
  }

  public void setSpillMapAttemptID(TaskAttemptID spillMapAttemptID) {
    this.spillMapAttemptID = spillMapAttemptID;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }
  
  
  
}

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

package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.apache.avro.util.Utf8;

/**
 * Event to record start of a task attempt
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptStartedEvent implements HistoryEvent {
  private TaskAttemptStarted datum = new TaskAttemptStarted();

  /**
   * Create an event to record the start of an attempt
   * @param attemptId Id of the attempt
   * @param taskType Type of task
   * @param startTime Start time of the attempt
   * @param trackerName Name of the Task Tracker where attempt is running
   * @param httpPort The port number of the tracker
   * @param shufflePort The shuffle port number of the container
   * @param containerId The containerId for the task attempt.
   */
  public TaskAttemptStartedEvent( TaskAttemptID attemptId,  
      TaskType taskType, long startTime, String trackerName,
      int httpPort, int shufflePort, ContainerId containerId) {
    datum.attemptId = new Utf8(attemptId.toString());
    datum.taskid = new Utf8(attemptId.getTaskID().toString());
    datum.startTime = startTime;
    datum.taskType = new Utf8(taskType.name());
    datum.trackerName = new Utf8(trackerName);
    datum.httpPort = httpPort;
    datum.shufflePort = shufflePort;
    datum.containerId = new Utf8(containerId.toString());
  }

  // TODO Remove after MrV1 is removed.
  // Using a dummy containerId to prevent jobHistory parse failures.
  public TaskAttemptStartedEvent(TaskAttemptID attemptId, TaskType taskType,
      long startTime, String trackerName, int httpPort, int shufflePort) {
    this(attemptId, taskType, startTime, trackerName, httpPort, shufflePort,
        ConverterUtils.toContainerId("container_-1_-1_-1_-1"));
  }

  TaskAttemptStartedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (TaskAttemptStarted)datum;
  }

  /** Get the task id */
  public TaskID getTaskId() { return TaskID.forName(datum.taskid.toString()); }
  /** Get the tracker name */
  public String getTrackerName() { return datum.trackerName.toString(); }
  /** Get the start time */
  public long getStartTime() { return datum.startTime; }
  /** Get the task type */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.taskType.toString());
  }
  /** Get the HTTP port */
  public int getHttpPort() { return datum.httpPort; }
  /** Get the shuffle port */
  public int getShufflePort() { return datum.shufflePort; }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() {
    return TaskAttemptID.forName(datum.attemptId.toString());
  }
  /** Get the event type */
  public EventType getEventType() {
    // Note that the task type can be setup/map/reduce/cleanup but the 
    // attempt-type can only be map/reduce.
   return getTaskId().getTaskType() == TaskType.MAP 
           ? EventType.MAP_ATTEMPT_STARTED 
           : EventType.REDUCE_ATTEMPT_STARTED;
  }
  /** Get the ContainerId */
  public ContainerId getContainerId() {
    return ConverterUtils.toContainerId(datum.containerId.toString());
  }
}

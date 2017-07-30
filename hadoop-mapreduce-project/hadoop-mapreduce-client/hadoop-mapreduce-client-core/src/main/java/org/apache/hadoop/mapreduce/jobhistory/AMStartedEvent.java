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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.apache.avro.util.Utf8;

/**
 * Event to record start of a task attempt
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AMStartedEvent implements HistoryEvent {
  private AMStarted datum = new AMStarted();

  /**
   * Create an event to record the start of an MR AppMaster
   * 
   * @param appAttemptId
   *          the application attempt id.
   * @param startTime
   *          the start time of the AM.
   * @param containerId
   *          the containerId of the AM.
   * @param nodeManagerHost
   *          the node on which the AM is running.
   * @param nodeManagerPort
   *          the port on which the AM is running.
   * @param nodeManagerHttpPort
   *          the httpPort for the node running the AM.
   */
  public AMStartedEvent(ApplicationAttemptId appAttemptId, long startTime,
      ContainerId containerId, String nodeManagerHost, int nodeManagerPort,
      int nodeManagerHttpPort) {
    datum.applicationAttemptId = new Utf8(appAttemptId.toString());
    datum.startTime = startTime;
    datum.containerId = new Utf8(containerId.toString());
    datum.nodeManagerHost = new Utf8(nodeManagerHost);
    datum.nodeManagerPort = nodeManagerPort;
    datum.nodeManagerHttpPort = nodeManagerHttpPort;
  }

  AMStartedEvent() {
  }

  public Object getDatum() {
    return datum;
  }

  public void setDatum(Object datum) {
    this.datum = (AMStarted) datum;
  }

  /**
   * @return the ApplicationAttemptId
   */
  public ApplicationAttemptId getAppAttemptId() {
    return ConverterUtils.toApplicationAttemptId(datum.applicationAttemptId
        .toString());
  }

  /**
   * @return the start time for the MRAppMaster
   */
  public long getStartTime() {
    return datum.startTime;
  }

  /**
   * @return the ContainerId for the MRAppMaster.
   */
  public ContainerId getContainerId() {
    return ConverterUtils.toContainerId(datum.containerId.toString());
  }

  /**
   * @return the node manager host.
   */
  public String getNodeManagerHost() {
    return datum.nodeManagerHost.toString();
  }

  /**
   * @return the node manager port.
   */
  public int getNodeManagerPort() {
    return datum.nodeManagerPort;
  }
  
  /**
   * @return the http port for the tracker.
   */
  public int getNodeManagerHttpPort() {
    return datum.nodeManagerHttpPort;
  }

  /** Get the attempt id */

  @Override
  public EventType getEventType() {
    return EventType.AM_STARTED;
  }
}

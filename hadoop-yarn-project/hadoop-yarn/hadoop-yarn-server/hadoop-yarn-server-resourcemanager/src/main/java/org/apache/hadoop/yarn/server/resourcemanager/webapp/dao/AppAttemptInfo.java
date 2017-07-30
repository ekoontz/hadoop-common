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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.util.ConverterUtils;

@XmlRootElement(name = "appAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptInfo {

  protected int id;
  protected long startTime;
  protected String containerId;
  protected String nodeHttpAddress;
  protected String nodeId;
  protected String logsLink;

  public AppAttemptInfo() {
  }

  public AppAttemptInfo(RMAppAttempt attempt, String user) {
    this.startTime = 0;
    this.containerId = "";
    this.nodeHttpAddress = "";
    this.nodeId = "";
    this.logsLink = "";
    if (attempt != null) {
      this.id = attempt.getAppAttemptId().getAttemptId();
      this.startTime = attempt.getStartTime();
      Container masterContainer = attempt.getMasterContainer();
      if (masterContainer != null) {
        this.containerId = masterContainer.getId().toString();
        this.nodeHttpAddress = masterContainer.getNodeHttpAddress();
        this.nodeId = masterContainer.getNodeId().toString();
        this.logsLink = join(HttpConfig.getSchemePrefix(),
            masterContainer.getNodeHttpAddress(),
            "/node", "/containerlogs/",
            ConverterUtils.toString(masterContainer.getId()), "/", user);
      }
    }
  }

  public int getAttemptId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public String getNodeHttpAddress() {
    return this.nodeHttpAddress;
  }

  public String getLogsLink() {
    return this.logsLink;
  }
}

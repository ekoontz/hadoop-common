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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <em>For internal use only...</em>
 */
@Private
@Unstable
public interface ApplicationMaster {
  ApplicationId getApplicationId();
  void setApplicationId(ApplicationId appId);

  String getHost();
  void setHost(String host);

  int getRpcPort();
  void setRpcPort(int rpcPort);

  String getTrackingUrl();
  void setTrackingUrl(String url);

  ApplicationStatus getStatus();
  void setStatus(ApplicationStatus status);

  YarnApplicationState getState();
  void setState(YarnApplicationState state);

  String getClientToken();
  void setClientToken(String clientToken);

  int getAMFailCount();
  void setAMFailCount(int amFailCount);

  int getContainerCount();
  void setContainerCount(int containerCount);

  String getDiagnostics();
  void setDiagnostics(String diagnostics);
}


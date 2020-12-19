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
package org.apache.hadoop.yarn.server.api.records;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;


public interface NodeStatus {
  
  public abstract NodeId getNodeId();
  public abstract int getResponseId();
  
  public abstract List<ContainerStatus> getContainersStatuses();
  public abstract void setContainersStatuses(
      List<ContainerStatus> containersStatuses);

  public abstract List<ApplicationId> getKeepAliveApplications();
  public abstract void setKeepAliveApplications(List<ApplicationId> appIds);
  
  NodeHealthStatus getNodeHealthStatus();
  void setNodeHealthStatus(NodeHealthStatus healthStatus);

  public abstract void setNodeId(NodeId nodeId);
  public abstract void setResponseId(int responseId);
}

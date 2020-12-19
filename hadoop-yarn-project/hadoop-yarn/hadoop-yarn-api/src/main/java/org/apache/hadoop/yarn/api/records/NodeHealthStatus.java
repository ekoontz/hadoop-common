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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;

/**
 * <p><code>NodeHealthStatus</code> is a summary of the health status of the
 * node.</p>
 *
 * <p>It includes information such as:
 *   <ul>
 *     <li>
 *       An indicator of whether the node is healthy, as determined by the 
 *       health-check script.
 *     </li>
 *     <li>The previous time at which the health status was reported.</li>
 *     <li>A diagnostic report on the health status.</li>
 *   </ul>
 * </p>
 * 
 * @see NodeReport
 * @see ClientRMProtocol#getClusterNodes(org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest)
 */
@Public
@Stable
public interface NodeHealthStatus {

  /**
   * Is the node healthy?
   * @return <code>true</code> if the node is healthy, else <code>false</code>
   */
  @Public
  @Stable
  boolean getIsNodeHealthy();

  @Private
  @Unstable
  void setIsNodeHealthy(boolean isNodeHealthy);

  /**
   * Get the <em>diagnostic health report</em> of the node.
   * @return <em>diagnostic health report</em> of the node
   */
  @Public
  @Stable
  String getHealthReport();

  @Private
  @Unstable
  void setHealthReport(String healthReport);

  /**
   * Get the <em>last timestamp</em> at which the health report was received.
   * @return <em>last timestamp</em> at which the health report was received
   */
  @Public
  @Stable
  long getLastHealthReportTime();

  @Private
  @Unstable
  void setLastHealthReportTime(long lastHealthReport);
}
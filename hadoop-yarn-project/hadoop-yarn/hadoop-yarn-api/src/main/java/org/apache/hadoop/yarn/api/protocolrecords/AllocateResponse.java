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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;

/**
 * <p>The response sent by the <code>ResourceManager</code> the  
 * <code>ApplicationMaster</code> during resource negotiation.</p>
 *
 * <p>The response, via {@link AMResponse}, includes:
 *   <ul>
 *     <li>Response ID to track duplicate responses.</li>
 *     <li>
 *       A reboot flag to let the <code>ApplicationMaster</code> know that its 
 *       horribly out of sync and needs to reboot.</li>
 *     <li>A list of newly allocated {@link Container}.</li>
 *     <li>A list of completed {@link Container}.</li>
 *     <li>
 *       The available headroom for resources in the cluster for the
 *       application. 
 *     </li>
 *   </ul>
 * </p>
 * 
 * @see AMRMProtocol#allocate(AllocateRequest)
 */
@Public
@Stable
public interface AllocateResponse {
  /**
   * Get the {@link AMResponse} sent by the <code>ResourceManager</code>.
   * @return <code>AMResponse</code> sent by the <code>ResourceManager</code>
   */
  @Public
  @Stable
  public abstract AMResponse getAMResponse();

  @Private
  @Unstable
  public abstract void setAMResponse(AMResponse amResponse);
  
  
  /**
   * Get the number of hosts available on the cluster.
   * @return the available host count.
   */
  @Public
  @Stable
  public int getNumClusterNodes();
  
  @Private
  @Unstable
  public void setNumClusterNodes(int numNodes);
}

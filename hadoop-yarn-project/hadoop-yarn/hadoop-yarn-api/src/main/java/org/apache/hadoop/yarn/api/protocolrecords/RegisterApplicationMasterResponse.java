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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * <p>The response sent by the <code>ResourceManager</code> to a new 
 * <code>ApplicationMaster</code> on registration.</p>
 * 
 * <p>The response contains critical details such as:
 * <ul>
 *   <li>Minimum capability for allocated resources in the cluster.</li>
 *   <li>Maximum capability for allocated resources in the cluster.</li>
 * </ul>
 * </p>
 * 
 * @see AMRMProtocol#registerApplicationMaster(RegisterApplicationMasterRequest)
 */
@Public
@Stable
public interface RegisterApplicationMasterResponse {
  
  /**
   * Get the minimum capability for any {@link Resource} allocated by the 
   * <code>ResourceManager</code> in the cluster.
   * @return minimum capability of allocated resources in the cluster
   */
  @Public
  @Stable
  public Resource getMinimumResourceCapability();
  
  @Private
  @Unstable
  public void setMinimumResourceCapability(Resource capability);
  
  /**
   * Get the maximum capability for any {@link Resource} allocated by the 
   * <code>ResourceManager</code> in the cluster.
   * @return maximum capability of allocated resources in the cluster
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability();
  
  @Private
  @Unstable
  public void setMaximumResourceCapability(Resource capability);

  /**
   * Get the <code>ApplicationACL</code>s for the application. 
   * @return all the <code>ApplicationACL</code>s
   */
  @Public
  @Stable
  public Map<ApplicationAccessType, String> getApplicationACLs();

  /**
   * Set the <code>ApplicationACL</code>s for the application. 
   * @param acls
   */
  @Private
  @Unstable
  public void setApplicationACLs(Map<ApplicationAccessType, String> acls);
}

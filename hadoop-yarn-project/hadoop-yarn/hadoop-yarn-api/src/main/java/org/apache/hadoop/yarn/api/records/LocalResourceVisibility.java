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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ContainerManager;

/**
 * <p><code>LocalResourceVisibility</code> specifies the <em>visibility</em>
 * of a resource localized by the <code>NodeManager</code>.</p>
 * 
 * <p>The <em>visibility</em> can be one of:
 *   <ul>
 *     <li>{@link #PUBLIC} - Shared by all users on the node.</li>
 *     <li>
 *       {@link #PRIVATE} - Shared among all applications of the 
 *       <em>same user</em> on the node.
 *     </li>
 *     <li>
 *       {@link #APPLICATION} - Shared only among containers of the 
 *       <em>same application</em> on the node.
 *     </li>
 *   </ul>
 * </p>
 * 
 * @see LocalResource
 * @see ContainerLaunchContext
 * @see ApplicationSubmissionContext
 * @see ContainerManager#startContainer(org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest)
 */
@Public
@Stable
public enum LocalResourceVisibility {
  /** 
   * Shared by all users on the node.
   */
  PUBLIC, 
  
  /** 
   * Shared among all applications of the <em>same user</em> on the node.
   */
  PRIVATE, 
  
  /** 
   * Shared only among containers of the <em>same application</em> on the node.
   */
  APPLICATION
}

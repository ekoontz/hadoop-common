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

/**
 * <p><code>ContainerStatus</code> represents the current status of a 
 * <code>Container</code>.</p>
 * 
 * <p>It provides details such as:
 *   <ul>
 *     <li><code>ContainerId</code> of the container.</li>
 *     <li><code>ContainerState</code> of the container.</li>
 *     <li><em>Exit status</em> of a completed container.</li>
 *     <li><em>Diagnostic</em> message for a failed container.</li>
 *   </ul>
 * </p>
 */
@Public
@Stable
public interface ContainerStatus {
  /**
   * Get the <code>ContainerId</code> of the container.
   * @return <code>ContainerId</code> of the container
   */
  @Public
  @Stable
  ContainerId getContainerId();
  
  @Private
  @Unstable
  void setContainerId(ContainerId containerId);

  /**
   * Get the <code>ContainerState</code> of the container.
   * @return <code>ContainerState</code> of the container
   */
  @Public
  @Stable
  ContainerState getState();
  
  @Private
  @Unstable
  void setState(ContainerState state);

  /**
   * <p>Get the <em>exit status</em> for the container.</p>
   *  
   * <p>Note: This is valid only for completed containers i.e. containers
   * with state {@link ContainerState#COMPLETE}. 
   * Otherwise, it returns an invalid exit code equal to {@literal -1000};</p>
   * 
   * <p>Container killed by the framework, either due to being released by
   * the application or being 'lost' due to node failures etc. have a special
   * exit code of {@literal -100}.</p>
   * 
   * <p>When threshold number of the nodemanager-local-directories or
   * threshold number of the nodemanager-log-directories become bad, then
   * container is not launched and is exited with exit status of
   * {@literal -101}.</p>
   *  
   * @return <em>exit status</em> for the container
   */
  @Public
  @Stable
  int getExitStatus();
  
  @Private
  @Unstable
  void setExitStatus(int exitStatus);

  /**
   * Get <em>diagnostic messages</em> for failed containers.
   * @return <em>diagnostic messages</em> for failed containers
   */
  @Public
  @Stable
  String getDiagnostics();
  
  @Private
  @Unstable
  void setDiagnostics(String diagnostics);
}

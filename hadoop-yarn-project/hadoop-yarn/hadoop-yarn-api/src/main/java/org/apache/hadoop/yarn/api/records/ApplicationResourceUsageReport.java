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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Contains various scheduling metrics to be reported by UI and CLI.
 */
@Public
@Stable
public interface ApplicationResourceUsageReport {

  /**
   * Get the number of used containers
   * @return the number of used containers
   */
  @Public
  @Stable
  int getNumUsedContainers();

  /**
   * Set the number of used containers
   * @param num_containers the number of used containers
   */
  @Private
  @Unstable
  void setNumUsedContainers(int num_containers);

  /**
   * Get the number of reserved containers
   * @return the number of reserved containers
   */
  @Public
  @Stable
  int getNumReservedContainers();

  /**
   * Set the number of reserved containers
   * @param num_reserved_containers the number of reserved containers
   */
  @Private
  @Unstable
  void setNumReservedContainers(int num_reserved_containers);

  /**
   * Get the used <code>Resource</code>
   * @return the used <code>Resource</code>
   */
  @Public
  @Stable
  Resource getUsedResources();

  @Private
  @Unstable
  void setUsedResources(Resource resources);

  /**
   * Get the reserved <code>Resource</code>
   * @return the reserved <code>Resource</code>
   */
  @Public
  @Stable
  Resource getReservedResources();

  @Private
  @Unstable
  void setReservedResources(Resource reserved_resources);

  /**
   * Get the needed <code>Resource</code>
   * @return the needed <code>Resource</code>
   */
  @Public
  @Stable
  Resource getNeededResources();

  @Private
  @Unstable
  void setNeededResources(Resource needed_resources);
}

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

package org.apache.hadoop.yarn.util;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * Interface class to obtain process resource usage
 *
 */
@Private
public abstract class ResourceCalculatorProcessTree extends Configured {
  static final Log LOG = LogFactory
      .getLog(ResourceCalculatorProcessTree.class);

  /**
   * Create process-tree instance with specified root process.
   *
   * Subclass must override this.
   * @param root process-tree root-process
   */
  public ResourceCalculatorProcessTree(String root) {
  }

  /**
   * Update the process-tree with latest state.
   *
   * Each call to this function should increment the age of the running
   * processes that already exist in the process tree. Age is used other API's
   * of the interface.
   *
   */
  public abstract void updateProcessTree();

  /**
   * Get a dump of the process-tree.
   *
   * @return a string concatenating the dump of information of all the processes
   *         in the process-tree
   */
  public abstract String getProcessTreeDump();

  /**
   * Get the cumulative virtual memory used by all the processes in the
   * process-tree.
   *
   * @return cumulative virtual memory used by the process-tree in bytes.
   */
  public long getCumulativeVmem() {
    return getCumulativeVmem(0);
  }

  /**
   * Get the cumulative resident set size (rss) memory used by all the processes
   * in the process-tree.
   *
   * @return cumulative rss memory used by the process-tree in bytes. return 0
   *         if it cannot be calculated
   */
  public long getCumulativeRssmem() {
    return getCumulativeRssmem(0);
  }

  /**
   * Get the cumulative virtual memory used by all the processes in the
   * process-tree that are older than the passed in age.
   *
   * @param olderThanAge processes above this age are included in the
   *                      memory addition
   * @return cumulative virtual memory used by the process-tree in bytes,
   *          for processes older than this age.
   */
  public abstract long getCumulativeVmem(int olderThanAge);

  /**
   * Get the cumulative resident set size (rss) memory used by all the processes
   * in the process-tree that are older than the passed in age.
   *
   * @param olderThanAge processes above this age are included in the
   *                      memory addition
   * @return cumulative rss memory used by the process-tree in bytes,
   *          for processes older than this age. return 0 if it cannot be
   *          calculated
   */
  public abstract long getCumulativeRssmem(int olderThanAge);

  /**
   * Get the CPU time in millisecond used by all the processes in the
   * process-tree since the process-tree created
   *
   * @return cumulative CPU time in millisecond since the process-tree created
   *         return 0 if it cannot be calculated
   */
  public abstract long getCumulativeCpuTime();

  /** Verify that the tree process id is same as its process group id.
   * @return true if the process id matches else return false.
   */
  public abstract boolean checkPidPgrpidForMatch();

  /**
   * Create the ResourceCalculatorProcessTree rooted to specified process 
   * from the class name and configure it. If class name is null, this method
   * will try and return a process tree plugin available for this system.
   *
   * @param pid process pid of the root of the process tree
   * @param clazz class-name
   * @param conf configure the plugin with this.
   *
   * @return ResourceCalculatorProcessTree or null if ResourceCalculatorPluginTree
   *         is not available for this system.
   */
  public static ResourceCalculatorProcessTree getResourceCalculatorProcessTree(
    String pid, Class<? extends ResourceCalculatorProcessTree> clazz, Configuration conf) {

    if (clazz != null) {
      try {
        Constructor <? extends ResourceCalculatorProcessTree> c = clazz.getConstructor(String.class);
        ResourceCalculatorProcessTree rctree = c.newInstance(pid);
        rctree.setConf(conf);
        return rctree;
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }

    // No class given, try a os specific class
    if (ProcfsBasedProcessTree.isAvailable()) {
      return new ProcfsBasedProcessTree(pid);
    }
    if (WindowsBasedProcessTree.isAvailable()) {
      return new WindowsBasedProcessTree(pid);
    }

    // Not supported on this system.
    return null;
  }
}

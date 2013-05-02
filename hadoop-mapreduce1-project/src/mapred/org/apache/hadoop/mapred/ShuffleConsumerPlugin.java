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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.Task.TaskReporter;

@InterfaceAudience.LimitedPrivate("MapReduce")
@InterfaceStability.Unstable
public interface ShuffleConsumerPlugin {
  /**
   * To initialize the reduce copier plugin.
   * @param context reduce copier context.
   */
  public void init(Context context)
    throws ClassNotFoundException, IOException;

  /**
   * To fetch the map outputs.
   * @return true if the fetch was successful; false otherwise.
   */
  public boolean fetchOutputs() throws IOException;

  /**
   * To create a key-value iterator to read the merged output.
   * @return an iterator for merged key-value pairs.
   */
  public RawKeyValueIterator createKVIterator() throws IOException;

  /**
   * close and clean any resource associated with this object.
   */
  public void close();
  
  /**
   * To get any exception from merge.
   */
  public Throwable getMergeThrowable();

  public static class Context {
    private final TaskUmbilicalProtocol umbilical;
    private final JobConf jobConf;
    private final TaskReporter reporter;
    private final ReduceTask reduceTask;

    public Context(TaskUmbilicalProtocol umbilical, JobConf conf,
                   TaskReporter reporter, ReduceTask reduceTask) {
      this.umbilical = umbilical;
      this.jobConf = conf;
      this.reporter = reporter;
      this.reduceTask = reduceTask;
    }

    public TaskUmbilicalProtocol getUmbilical() {
      return umbilical;
    }

    public JobConf getJobConf() {
      return jobConf;
    }

    public TaskReporter getReporter() {
      return reporter;
    }

    public ReduceTask getReduceTask() {
      return reduceTask;
    }
  }
}

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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MRJobConfig {

  public static final String COUNTERS_MAX_KEY = "mapreduce.job.counters.max";
  public static final int COUNTERS_MAX_DEFAULT = 120;

  public static final String COUNTER_GROUP_NAME_MAX_KEY = "mapreduce.job.counters.group.name.max";
  public static final int COUNTER_GROUP_NAME_MAX_DEFAULT = 128;

  public static final String COUNTER_NAME_MAX_KEY = "mapreduce.job.counters.counter.name.max";
  public static final int COUNTER_NAME_MAX_DEFAULT = 64;

  public static final String COUNTER_GROUPS_MAX_KEY = "mapreduce.job.counters.groups.max";
  public static final int COUNTER_GROUPS_MAX_DEFAULT = 50;

  /* Configs for tracking ids of tokens used by a job */
  public static final String JOB_TOKEN_TRACKING_IDS_ENABLED =
      "mapreduce.job.token.tracking.ids.enabled";
  public static final boolean DEFAULT_JOB_TOKEN_TRACKING_IDS_ENABLED = false;
  public static final String JOB_TOKEN_TRACKING_IDS =
      "mapreduce.job.token.tracking.ids";
}

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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 */
public interface JobContext {
  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  public static final String INPUT_FORMAT_CLASS_ATTR = 
    "mapreduce.inputformat.class";
  public static final String MAP_CLASS_ATTR = "mapreduce.map.class";
  public static final String MAP_OUTPUT_COLLECTOR_CLASS_ATTR = 
    "mapreduce.job.map.output.collector.class";
  public static final String COMBINE_CLASS_ATTR = "mapreduce.combine.class";
  public static final String SHUFFLE_CONSUMER_PLUGIN_ATTR =
    "mapreduce.job.reduce.shuffle.consumer.plugin.class";
  public static final String REDUCE_CLASS_ATTR = "mapreduce.reduce.class";
  public static final String OUTPUT_FORMAT_CLASS_ATTR = 
    "mapreduce.outputformat.class";
  public static final String PARTITIONER_CLASS_ATTR = 
    "mapreduce.partitioner.class";
  public static final String JAR_UNPACK_PATTERN = "mapreduce.job.jar.unpack.pattern";

  public static final String JOB_NAMENODES = "mapreduce.job.hdfs-servers";

  public static final String JOB_ACL_VIEW_JOB = "mapreduce.job.acl-view-job";
  public static final String JOB_ACL_MODIFY_JOB =
    "mapreduce.job.acl-modify-job";

  public static final String CACHE_FILE_VISIBILITIES = 
    "mapreduce.job.cache.files.visibilities";
  public static final String CACHE_ARCHIVES_VISIBILITIES = 
    "mapreduce.job.cache.archives.visibilities";
  
  public static final String JOB_CANCEL_DELEGATION_TOKEN = 
    "mapreduce.job.complete.cancel.delegation.tokens";
  public static final String USER_LOG_RETAIN_HOURS = 
    "mapred.userlog.retain.hours";
  public static final String MAPREDUCE_TASK_CLASSPATH_PRECEDENCE = 
    "mapreduce.task.classpath.user.precedence";
  
  public static final String MAP_MEMORY_PHYSICAL_MB =
    "mapreduce.map.memory.physical.mb";
  public static final String REDUCE_MEMORY_PHYSICAL_MB = 
     "mapreduce.reduce.memory.physical.mb";

  /**
   * Return the configuration for the job.
   * @return the shared configuration object
   */
  public Configuration getConfiguration();
  
  /**
   * Get credentials for the job.
   * @return credentials for the job
   */
  public Credentials getCredentials();

  /**
   * Get the unique ID for the job.
   * @return the object with the job id
   */
  public JobID getJobID();
  
  /**
   * Get configured the number of reduce tasks for this job. Defaults to 
   * <code>1</code>.
   * @return the number of reduce tasks for this job.
   */
  public int getNumReduceTasks();
  
  /**
   * Get the current working directory for the default file system.
   * 
   * @return the directory name.
   */
  public Path getWorkingDirectory() throws IOException;
  
  /**
   * Get the key class for the job output data.
   * @return the key class for the job output data.
   */
  public Class<?> getOutputKeyClass();
  
  /**
   * Get the value class for job outputs.
   * @return the value class for job outputs.
   */
  public Class<?> getOutputValueClass();

  /**
   * Get the key class for the map output data. If it is not set, use the
   * (final) output key class. This allows the map output key class to be
   * different than the final output key class.
   * @return the map output key class.
   */
  public Class<?> getMapOutputKeyClass();

  /**
   * Get the value class for the map output data. If it is not set, use the
   * (final) output value class This allows the map output value class to be
   * different than the final output value class.
   *  
   * @return the map output value class.
   */
  public Class<?> getMapOutputValueClass();

  /**
   * Get the user-specified job name. This is only used to identify the 
   * job to the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getJobName();
  
  /**
   * Get the boolean value for the property that specifies which classpath
   * takes precedence when tasks are launched. True - user's classes takes
   * precedence. False - system's classes takes precedence.
   * @return true if user's classes should take precedence
   */
  public boolean userClassesTakesPrecedence();

  /**
   * Get the {@link InputFormat} class for the job.
   * 
   * @return the {@link InputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Mapper} class for the job.
   * 
   * @return the {@link Mapper} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException;

  /**
   * Get the combiner class for the job.
   * 
   * @return the combiner class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Reducer} class for the job.
   * 
   * @return the {@link Reducer} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link OutputFormat} class for the job.
   * 
   * @return the {@link OutputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Partitioner} class for the job.
   * 
   * @return the {@link Partitioner} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link RawComparator} comparator used to compare keys.
   * 
   * @return the {@link RawComparator} comparator used to compare keys.
   */
  public RawComparator<?> getSortComparator();

  /**
   * Get the pathname of the job's jar.
   * @return the pathname
   */
  public String getJar();

  /**
   * Get the user defined {@link RawComparator} comparator for
   * grouping keys of inputs to the combiner.
   *
   * @return comparator set by the user for grouping values.
   * @see Job#setCombinerKeyGroupingComparatorClass(Class)
   */
  public RawComparator<?> getCombinerKeyGroupingComparator();

  /** 
   * Get the user defined {@link RawComparator} comparator for 
   * grouping keys of inputs to the reduce.
   * 
   * @return comparator set by the user for grouping values.
   * @see Job#setGroupingComparatorClass(Class) for details.  
   * @see #getCombinerKeyGroupingComparator()
   */
  public RawComparator<?> getGroupingComparator();
  
  /**
   * Get whether job-setup and job-cleanup is needed for the job 
   * 
   * @return boolean 
   */
  public boolean getJobSetupCleanupNeeded();

  /**
   * Get whether the task profiling is enabled.
   * @return true if some tasks will be profiled
   */
  public boolean getProfileEnabled();
  
  /**
   * 
   * @return the parameters to pass to the task child to configure profiling
   */
  public String getProfileParams();

 /**
  * Get the reported username for this job.
  * 
  * @return the username
  */
  public String getUser();
 
 /**
  * This method checks to see if symlinks are to be create for the 
  * localized cache files in the current working directory 
  * @return true if symlinks are to be created- else return false
  */
  public boolean getSymlink();
 
 /**
  * Get the archive entries in classpath as an array of Path
  */
  public Path[] getArchiveClassPaths();

 /**
  * Get cache archives set in the Configuration
  * @return A URI array of the caches set in the Configuration
  * @throws IOException
  */
  public URI[] getCacheArchives() throws IOException;
  
  /**
   * Get cache files set in the Configuration
   * @throws IOException
   */
  
  public URI[] getCacheFiles() throws IOException;

  /**
   * Return the path array of the localized caches
   * @return A path array of localized caches
   * @throws IOException
   */
  public Path[] getLocalCacheArchives() throws IOException;

  /**
   * Return the path array of the localized files
   * @return A path array of localized files
   * @throws IOException
   */
  public Path[] getLocalCacheFiles() throws IOException;

  /**
   * Get the file entries in classpath as an array of Path
   */
  public Path[] getFileClassPaths();
  
  /**
   * Get the timestamps of the archives.  Used by internal
   * @return a string array of timestamps 
   * @throws IOException
   */
  public String[] getArchiveTimestamps();
  
  /**
   * Get the timestamps of the files.  Used by internal
   * @return a string array of timestamps 
   * @throws IOException
   */
  public String[] getFileTimestamps();

  /** 
   * Get the configured number of maximum attempts that will be made to run a
   *  
   * @return the max number of attempts per map task.
   */
  public int getMaxMapAttempts();

  /** 
   * Get the configured number of maximum attempts  that will be made to run a
   * 
   * @return the max number of attempts per reduce task.
   */
  public int getMaxReduceAttempts();

}

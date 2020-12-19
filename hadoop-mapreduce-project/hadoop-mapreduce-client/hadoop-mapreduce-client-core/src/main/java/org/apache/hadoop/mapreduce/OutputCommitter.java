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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
/**
 * <code>OutputCommitter</code> describes the commit of task output for a 
 * Map-Reduce job.
 *
 * <p>The Map-Reduce framework relies on the <code>OutputCommitter</code> of 
 * the job to:<p>
 * <ol>
 *   <li>
 *   Setup the job during initialization. For example, create the temporary 
 *   output directory for the job during the initialization of the job.
 *   </li>
 *   <li>
 *   Cleanup the job after the job completion. For example, remove the
 *   temporary output directory after the job completion. 
 *   </li>
 *   <li>
 *   Setup the task temporary output.
 *   </li> 
 *   <li>
 *   Check whether a task needs a commit. This is to avoid the commit
 *   procedure if a task does not need commit.
 *   </li>
 *   <li>
 *   Commit of the task output.
 *   </li>  
 *   <li>
 *   Discard the task commit.
 *   </li>
 * </ol>
 * The methods in this class can be called from several different processes and
 * from several different contexts.  It is important to know which process and
 * which context each is called from.  Each method should be marked accordingly
 * in its documentation.
 * 
 * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter 
 * @see JobContext
 * @see TaskAttemptContext 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class OutputCommitter {
  /**
   * For the framework to setup the job output during initialization.  This is
   * called from the application master process for the entire job.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException if temporary output could not be created
   */
  public abstract void setupJob(JobContext jobContext) throws IOException;

  /**
   * For cleaning up the job's output after job completion.  This is called
   * from the application master process for the entire job.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException
   * @deprecated Use {@link #commitJob(JobContext)} and
   *                 {@link #abortJob(JobContext, JobStatus.State)} instead.
   */
  @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException { }

  /**
   * For committing job's output after successful job completion. Note that this
   * is invoked for jobs with final runstate as SUCCESSFUL.  This is called
   * from the application master process for the entire job.	
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException
   */
  public void commitJob(JobContext jobContext) throws IOException {
    cleanupJob(jobContext);
  }

  
  /**
   * For aborting an unsuccessful job's output. Note that this is invoked for 
   * jobs with final runstate as {@link JobStatus.State#FAILED} or 
   * {@link JobStatus.State#KILLED}.  This is called from the application
   * master process for the entire job.
   *
   * @param jobContext Context of the job whose output is being written.
   * @param state final runstate of the job
   * @throws IOException
   */
  public void abortJob(JobContext jobContext, JobStatus.State state) 
  throws IOException {
    cleanupJob(jobContext);
  }
  
  /**
   * Sets up output for the task.  This is called from each individual task's
   * process that will output to HDFS, and it is called just for that task.
   * 
   * @param taskContext Context of the task whose output is being written.
   * @throws IOException
   */
  public abstract void setupTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * Check whether task needs a commit.  This is called from each individual
   * task's process that will output to HDFS, and it is called just for that
   * task.
   * 
   * @param taskContext
   * @return true/false
   * @throws IOException
   */
  public abstract boolean needsTaskCommit(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * To promote the task's temporary output to final output location.
   * If {@link #needsTaskCommit(TaskAttemptContext)} returns true and this
   * task is the task that the AM determines finished first, this method
   * is called to commit an individual task's output.  This is to mark
   * that tasks output as complete, as {@link #commitJob(JobContext)} will 
   * also be called later on if the entire job finished successfully. This
   * is called from a task's process.
   * 
   * @param taskContext Context of the task whose output is being written.
   * @throws IOException if commit is not successful. 
   */
  public abstract void commitTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * Discard the task output. This is called from a task's process to clean 
   * up a single task's output that can not yet been committed.
   * 
   * @param taskContext
   * @throws IOException
   */
  public abstract void abortTask(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * Is task output recovery supported for restarting jobs?
   * 
   * If task output recovery is supported, job restart can be done more 
   * efficiently.
   * 
   * @return <code>true</code> if task output recovery is supported,
   *         <code>false</code> otherwise
   * @see #recoverTask(TaskAttemptContext)         
   */
  public boolean isRecoverySupported() {
    return false;
  }
  
  /**
   * Recover the task output. 
   * 
   * The retry-count for the job will be passed via the 
   * {@link MRJobConfig#APPLICATION_ATTEMPT_ID} key in  
   * {@link TaskAttemptContext#getConfiguration()} for the 
   * <code>OutputCommitter</code>.  This is called from the application master
   * process, but it is called individually for each task.
   * 
   * If an exception is thrown the task will be attempted again. 
   * 
   * @param taskContext Context of the task whose output is being recovered
   * @throws IOException
   */
  public void recoverTask(TaskAttemptContext taskContext)
  throws IOException
  {}
}

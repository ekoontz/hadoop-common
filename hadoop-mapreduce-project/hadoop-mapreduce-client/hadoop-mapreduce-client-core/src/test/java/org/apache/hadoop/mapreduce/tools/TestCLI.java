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
package org.apache.hadoop.mapreduce.tools;

import static org.junit.Assert.*;

import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;

public class TestCLI {
  private static String jobIdStr = "job_1015298225799_0015";

  @Test
  public void testListAttemptIdsWithValidInput() throws Exception {
    JobID jobId = JobID.forName(jobIdStr);
    Cluster mockCluster = mock(Cluster.class);
    Job job = mock(Job.class);
    CLI cli = spy(new CLI());

    doReturn(mockCluster).when(cli).createCluster();
    when(job.getTaskReports(TaskType.MAP)).thenReturn(
        getTaskReports(jobId, TaskType.MAP));
    when(job.getTaskReports(TaskType.REDUCE)).thenReturn(
        getTaskReports(jobId, TaskType.REDUCE));
    when(mockCluster.getJob(jobId)).thenReturn(job);

    int retCode_MAP = cli.run(new String[] { "-list-attempt-ids", jobIdStr,
        "MAP", "running" });
    // testing case insensitive behavior
    int retCode_map = cli.run(new String[] { "-list-attempt-ids", jobIdStr,
        "map", "running" });

    int retCode_REDUCE = cli.run(new String[] { "-list-attempt-ids", jobIdStr,
        "REDUCE", "running" });

    int retCode_completed = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "REDUCE", "completed" });

    assertEquals("MAP is a valid input,exit code should be 0", 0, retCode_MAP);
    assertEquals("map is a valid input,exit code should be 0", 0, retCode_map);
    assertEquals("REDUCE is a valid input,exit code should be 0", 0,
        retCode_REDUCE);
    assertEquals(
        "REDUCE and completed are a valid inputs to -list-attempt-ids,exit code should be 0",
        0, retCode_completed);

    verify(job, times(2)).getTaskReports(TaskType.MAP);
    verify(job, times(2)).getTaskReports(TaskType.REDUCE);
  }

  @Test
  public void testListAttemptIdsWithInvalidInputs() throws Exception {
    JobID jobId = JobID.forName(jobIdStr);
    Cluster mockCluster = mock(Cluster.class);
    Job job = mock(Job.class);
    CLI cli = spy(new CLI());

    doReturn(mockCluster).when(cli).createCluster();
    when(mockCluster.getJob(jobId)).thenReturn(job);

    int retCode_JOB_SETUP = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "JOB_SETUP", "running" });

    int retCode_JOB_CLEANUP = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "JOB_CLEANUP", "running" });

    int retCode_invalidTaskState = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "REDUCE", "complete" });

    assertEquals("JOB_SETUP is an invalid input,exit code should be -1", -1,
        retCode_JOB_SETUP);
    assertEquals("JOB_CLEANUP is an invalid input,exit code should be -1", -1,
        retCode_JOB_CLEANUP);
    assertEquals("complete is an invalid input,exit code should be -1", -1,
        retCode_invalidTaskState);

  }

  private TaskReport[] getTaskReports(JobID jobId, TaskType type) {
    return new TaskReport[] { new TaskReport(), new TaskReport() };
  }
}

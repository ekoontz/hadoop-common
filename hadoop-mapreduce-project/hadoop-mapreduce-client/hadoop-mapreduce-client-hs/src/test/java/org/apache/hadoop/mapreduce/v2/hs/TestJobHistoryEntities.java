package org.apache.hadoop.mapreduce.v2.hs;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class TestJobHistoryEntities {

  private final String historyFileName =
      "job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
  private final String confFileName = "job_1329348432655_0001_conf.xml";
  private final Configuration conf = new Configuration();
  private final JobACLsManager jobAclsManager = new JobACLsManager(conf);
  private boolean loadTasks;
  private JobId jobId = MRBuilderUtils.newJobId(1329348432655l, 1, 1);
  Path fulleHistoryPath =
    new Path(this.getClass().getClassLoader().getResource(historyFileName)
        .getFile());
  Path fullConfPath =
    new Path(this.getClass().getClassLoader().getResource(confFileName)
        .getFile());
  private CompletedJob completedJob;

  public TestJobHistoryEntities(boolean loadTasks) throws Exception {
    this.loadTasks = loadTasks;
  }

  @Parameters
  public static Collection<Object[]> data() {
    List<Object[]> list = new ArrayList<Object[]>(2);
    list.add(new Object[] { true });
    list.add(new Object[] { false });
    return list;
  }

  /* Verify some expected values based on the history file */
  @Test
  public void testCompletedJob() throws Exception {
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    //Re-initialize to verify the delayed load.
    completedJob =
      new CompletedJob(conf, jobId, fulleHistoryPath, loadTasks, "user",
          info, jobAclsManager);
    //Verify tasks loaded based on loadTask parameter.
    assertEquals(loadTasks, completedJob.tasksLoaded.get());
    assertEquals(1, completedJob.getAMInfos().size());
    assertEquals(10, completedJob.getCompletedMaps());
    assertEquals(1, completedJob.getCompletedReduces());
    assertEquals(11, completedJob.getTasks().size());
    //Verify tasks loaded at this point.
    assertEquals(true, completedJob.tasksLoaded.get());
    assertEquals(10, completedJob.getTasks(TaskType.MAP).size());
    assertEquals(1, completedJob.getTasks(TaskType.REDUCE).size());
    assertEquals("user", completedJob.getUserName());
    assertEquals(JobState.SUCCEEDED, completedJob.getState());
    JobReport jobReport = completedJob.getReport();
    assertEquals("user", jobReport.getUser());
    assertEquals(JobState.SUCCEEDED, jobReport.getJobState());
  }
  
  @Test
  public void testCompletedTask() throws Exception {
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    completedJob =
      new CompletedJob(conf, jobId, fulleHistoryPath, loadTasks, "user",
          info, jobAclsManager);
    TaskId mt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    TaskId rt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    
    Map<TaskId, Task> mapTasks = completedJob.getTasks(TaskType.MAP);
    Map<TaskId, Task> reduceTasks = completedJob.getTasks(TaskType.REDUCE);
    assertEquals(10, mapTasks.size());
    assertEquals(1, reduceTasks.size());
    
    Task mt1 = mapTasks.get(mt1Id);
    assertEquals(1, mt1.getAttempts().size());
    assertEquals(TaskState.SUCCEEDED, mt1.getState());
    TaskReport mt1Report = mt1.getReport();
    assertEquals(TaskState.SUCCEEDED, mt1Report.getTaskState());
    assertEquals(mt1Id, mt1Report.getTaskId());
    Task rt1 = reduceTasks.get(rt1Id);
    assertEquals(1, rt1.getAttempts().size());
    assertEquals(TaskState.SUCCEEDED, rt1.getState());
    TaskReport rt1Report = rt1.getReport();
    assertEquals(TaskState.SUCCEEDED, rt1Report.getTaskState());
    assertEquals(rt1Id, rt1Report.getTaskId());
  }
  
  @Test
  public void testCompletedTaskAttempt() throws Exception {
    HistoryFileInfo info = mock(HistoryFileInfo.class);
    when(info.getConfFile()).thenReturn(fullConfPath);
    completedJob =
      new CompletedJob(conf, jobId, fulleHistoryPath, loadTasks, "user",
          info, jobAclsManager);
    TaskId mt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    TaskId rt1Id = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    TaskAttemptId mta1Id = MRBuilderUtils.newTaskAttemptId(mt1Id, 0);
    TaskAttemptId rta1Id = MRBuilderUtils.newTaskAttemptId(rt1Id, 0);
    
    Task mt1 = completedJob.getTask(mt1Id);
    Task rt1 = completedJob.getTask(rt1Id);
    
    TaskAttempt mta1 = mt1.getAttempt(mta1Id);
    assertEquals(TaskAttemptState.SUCCEEDED, mta1.getState());
    assertEquals("localhost:45454", mta1.getAssignedContainerMgrAddress());
    assertEquals("localhost:9999", mta1.getNodeHttpAddress());
    TaskAttemptReport mta1Report = mta1.getReport();
    assertEquals(TaskAttemptState.SUCCEEDED, mta1Report.getTaskAttemptState());
    assertEquals("localhost", mta1Report.getNodeManagerHost());
    assertEquals(45454, mta1Report.getNodeManagerPort());
    assertEquals(9999, mta1Report.getNodeManagerHttpPort());
    
    TaskAttempt rta1 = rt1.getAttempt(rta1Id);
    assertEquals(TaskAttemptState.SUCCEEDED, rta1.getState());
    assertEquals("localhost:45454", rta1.getAssignedContainerMgrAddress());
    assertEquals("localhost:9999", rta1.getNodeHttpAddress());
    TaskAttemptReport rta1Report = rta1.getReport();
    assertEquals(TaskAttemptState.SUCCEEDED, rta1Report.getTaskAttemptState());
    assertEquals("localhost", rta1Report.getNodeManagerHost());
    assertEquals(45454, rta1Report.getNodeManagerPort());
    assertEquals(9999, rta1Report.getNodeManagerHttpPort());
  }
}

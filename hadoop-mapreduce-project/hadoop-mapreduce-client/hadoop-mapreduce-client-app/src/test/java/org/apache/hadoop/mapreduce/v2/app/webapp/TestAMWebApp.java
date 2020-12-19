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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.APP_ID;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;

import com.google.inject.Injector;

public class TestAMWebApp {

  static class TestAppContext implements AppContext {
    final ApplicationAttemptId appAttemptID;
    final ApplicationId appID;
    final String user = MockJobs.newUserName();
    final Map<JobId, Job> jobs;
    final long startTime = System.currentTimeMillis();

    TestAppContext(int appid, int numJobs, int numTasks, int numAttempts) {
      appID = MockJobs.newAppID(appid);
      appAttemptID = MockJobs.newAppAttemptID(appID, 0);
      jobs = MockJobs.newJobs(appID, numJobs, numTasks, numAttempts);
    }

    TestAppContext() {
      this(0, 1, 1, 1);
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appID;
    }

    @Override
    public CharSequence getUser() {
      return user;
    }

    @Override
    public Job getJob(JobId jobID) {
      return jobs.get(jobID);
    }

    @Override
    public Map<JobId, Job> getAllJobs() {
      return jobs; // OK
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventHandler getEventHandler() {
      return null;
    }

    @Override
    public Clock getClock() {
      return null;
    }

    @Override
    public String getApplicationName() {
      return "TestApp";
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public ClusterInfo getClusterInfo() {
      return null;
    }
  }

  @Test public void testAppControllerIndex() {
    TestAppContext ctx = new TestAppContext();
    Injector injector = WebAppTests.createMockInjector(AppContext.class, ctx);
    AppController controller = injector.getInstance(AppController.class);
    controller.index();
    assertEquals(ctx.appID.toString(), controller.get(APP_ID,""));
  }

  @Test public void testAppView() {
    WebAppTests.testPage(AppView.class, AppContext.class, new TestAppContext());
  }


  
  @Test public void testJobView() {
    AppContext appContext = new TestAppContext();
    Map<String, String> params = getJobParams(appContext);
    WebAppTests.testPage(JobPage.class, AppContext.class, appContext, params);
  }

  @Test public void testTasksView() {
    AppContext appContext = new TestAppContext();
    Map<String, String> params = getTaskParams(appContext);
    WebAppTests.testPage(TasksPage.class, AppContext.class, appContext, params);
  }

  @Test public void testTaskView() {
    AppContext appContext = new TestAppContext();
    Map<String, String> params = getTaskParams(appContext);
    WebAppTests.testPage(TaskPage.class, AppContext.class, appContext, params);
  }

  public static Map<String, String> getJobParams(AppContext appContext) {
    JobId jobId = appContext.getAllJobs().entrySet().iterator().next().getKey();
    Map<String, String> params = new HashMap<String, String>();
    params.put(AMParams.JOB_ID, MRApps.toString(jobId));
    return params;
  }
  
  public static Map<String, String> getTaskParams(AppContext appContext) {
    JobId jobId = appContext.getAllJobs().entrySet().iterator().next().getKey();
    Entry<TaskId, Task> e = appContext.getJob(jobId).getTasks().entrySet().iterator().next();
    e.getValue().getType();
    Map<String, String> params = new HashMap<String, String>();
    params.put(AMParams.JOB_ID, MRApps.toString(jobId));
    params.put(AMParams.TASK_ID, MRApps.toString(e.getKey()));
    params.put(AMParams.TASK_TYPE, MRApps.taskSymbol(e.getValue().getType()));
    return params;
  }

  @Test public void testConfView() {
    WebAppTests.testPage(JobConfPage.class, AppContext.class,
                         new TestAppContext());
  }

  @Test public void testCountersView() {
    AppContext appContext = new TestAppContext();
    Map<String, String> params = getJobParams(appContext);
    WebAppTests.testPage(CountersPage.class, AppContext.class,
                         appContext, params);
  }
  
  @Test public void testSingleCounterView() {
    AppContext appContext = new TestAppContext();
    Map<String, String> params = getJobParams(appContext);
    params.put(AMParams.COUNTER_GROUP, 
        "org.apache.hadoop.mapreduce.FileSystemCounter");
    params.put(AMParams.COUNTER_NAME, "HDFS_WRITE_OPS");
    WebAppTests.testPage(SingleCounterPage.class, AppContext.class,
                         appContext, params);
  }

  @Test public void testTaskCountersView() {
    AppContext appContext = new TestAppContext();
    Map<String, String> params = getTaskParams(appContext);
    WebAppTests.testPage(CountersPage.class, AppContext.class,
                         appContext, params);
  }

  @Test public void testSingleTaskCounterView() {
    AppContext appContext = new TestAppContext(0, 1, 1, 2);
    Map<String, String> params = getTaskParams(appContext);
    params.put(AMParams.COUNTER_GROUP, 
        "org.apache.hadoop.mapreduce.FileSystemCounter");
    params.put(AMParams.COUNTER_NAME, "HDFS_WRITE_OPS");
    
    // remove counters from one task attempt
    // to test handling of missing counters
    TaskId taskID = MRApps.toTaskID(params.get(AMParams.TASK_ID));
    Job job = appContext.getJob(taskID.getJobId());
    Task task = job.getTask(taskID);
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    attempt.getReport().setCounters(null);
    
    WebAppTests.testPage(SingleCounterPage.class, AppContext.class,
                         appContext, params);
  }
  
  public static void main(String[] args) {
    WebApps.$for("yarn", AppContext.class, new TestAppContext(0, 8, 88, 4)).
        at(58888).inDevMode().start(new AMWebApp()).joinThread();
  }
}

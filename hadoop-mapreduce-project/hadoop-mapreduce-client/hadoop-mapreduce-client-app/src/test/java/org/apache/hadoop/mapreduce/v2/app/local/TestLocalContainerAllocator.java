package org.apache.hadoop.mapreduce.v2.app.local;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestLocalContainerAllocator {

  @Test
  public void testRMConnectionRetry() throws Exception {
    // verify the connection exception is thrown
    // if we haven't exhausted the retry interval
    Configuration conf = new Configuration();
    LocalContainerAllocator lca = new StubbedLocalContainerAllocator();
    lca.init(conf);
    lca.start();
    try {
      lca.heartbeat();
      Assert.fail("heartbeat was supposed to throw");
    } catch (YarnRemoteException e) {
      // YarnRemoteException is expected
    } finally {
      lca.stop();
    }

    // verify YarnException is thrown when the retry interval has expired
    conf.setLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS, 0);
    lca = new StubbedLocalContainerAllocator();
    lca.init(conf);
    lca.start();
    try {
      lca.heartbeat();
      Assert.fail("heartbeat was supposed to throw");
    } catch (YarnException e) {
      // YarnException is expected
    } finally {
      lca.stop();
    }
  }

  private static class StubbedLocalContainerAllocator
    extends LocalContainerAllocator {

    public StubbedLocalContainerAllocator() {
      super(mock(ClientService.class), createAppContext(),
          "nmhost", 1, 2, null);
    }

    @Override
    protected void register() {
    }

    @Override
    protected void startAllocatorThread() {
      allocatorThread = new Thread();
    }

    @Override
    protected AMRMProtocol createSchedulerProxy() {
      AMRMProtocol scheduler = mock(AMRMProtocol.class);
      try {
        when(scheduler.allocate(isA(AllocateRequest.class)))
          .thenThrow(RPCUtil.getRemoteException(new IOException("forcefail")));
      } catch (YarnRemoteException e) {
      }
      return scheduler;
    }

    private static AppContext createAppContext() {
      ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
      ApplicationAttemptId attemptId =
          BuilderUtils.newApplicationAttemptId(appId, 1);
      Job job = mock(Job.class);
      @SuppressWarnings("rawtypes")
      EventHandler eventHandler = mock(EventHandler.class);
      AppContext ctx = mock(AppContext.class);
      when(ctx.getApplicationID()).thenReturn(appId);
      when(ctx.getApplicationAttemptId()).thenReturn(attemptId);
      when(ctx.getJob(isA(JobId.class))).thenReturn(job);
      when(ctx.getClusterInfo()).thenReturn(
          new ClusterInfo(BuilderUtils.newResource(1024), BuilderUtils
              .newResource(10240)));
      when(ctx.getEventHandler()).thenReturn(eventHandler);
      return ctx;
    }
  }
}

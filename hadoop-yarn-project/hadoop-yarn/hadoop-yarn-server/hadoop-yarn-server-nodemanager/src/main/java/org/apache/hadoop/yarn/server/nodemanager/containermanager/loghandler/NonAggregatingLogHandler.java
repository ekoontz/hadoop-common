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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;
import org.apache.hadoop.yarn.service.AbstractService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Log Handler which schedules deletion of log files based on the configured log
 * retention time.
 */
public class NonAggregatingLogHandler extends AbstractService implements
    LogHandler {

  private static final Log LOG = LogFactory
      .getLog(NonAggregatingLogHandler.class);
  private final Dispatcher dispatcher;
  private final DeletionService delService;
  private final Map<ApplicationId, String> appOwners;

  private final LocalDirsHandlerService dirsHandler;
  private long deleteDelaySeconds;
  private ScheduledThreadPoolExecutor sched;

  public NonAggregatingLogHandler(Dispatcher dispatcher,
      DeletionService delService, LocalDirsHandlerService dirsHandler) {
    super(NonAggregatingLogHandler.class.getName());
    this.dispatcher = dispatcher;
    this.delService = delService;
    this.dirsHandler = dirsHandler;
    this.appOwners = new ConcurrentHashMap<ApplicationId, String>();
  }

  @Override
  public void init(Configuration conf) {
    // Default 3 hours.
    this.deleteDelaySeconds =
        conf.getLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 3 * 60 * 60);
    sched = createScheduledThreadPoolExecutor(conf);
    super.init(conf);
  }

  @Override
  public void stop() {
    if (sched != null) {
      sched.shutdown();
      boolean isShutdown = false;
      try {
        isShutdown = sched.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        sched.shutdownNow();
        isShutdown = true;
      }
      if (!isShutdown) {
        sched.shutdownNow();
      }
    }
    super.stop();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        LogHandlerAppStartedEvent appStartedEvent =
            (LogHandlerAppStartedEvent) event;
        this.appOwners.put(appStartedEvent.getApplicationId(),
            appStartedEvent.getUser());
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(appStartedEvent.getApplicationId(),
                ApplicationEventType.APPLICATION_LOG_HANDLING_INITED));
        break;
      case CONTAINER_FINISHED:
        // Ignore
        break;
      case APPLICATION_FINISHED:
        LogHandlerAppFinishedEvent appFinishedEvent =
            (LogHandlerAppFinishedEvent) event;
        // Schedule - so that logs are available on the UI till they're deleted.
        LOG.info("Scheduling Log Deletion for application: "
            + appFinishedEvent.getApplicationId() + ", with delay of "
            + this.deleteDelaySeconds + " seconds");
        sched.schedule(
            new LogDeleterRunnable(appOwners.remove(appFinishedEvent
                .getApplicationId()), appFinishedEvent.getApplicationId()),
            this.deleteDelaySeconds, TimeUnit.SECONDS);
        break;
      default:
        ; // Ignore
    }
  }

  ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor(
      Configuration conf) {
    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("LogDeleter #%d").build();
    sched =
        new ScheduledThreadPoolExecutor(conf.getInt(
            YarnConfiguration.NM_LOG_DELETION_THREADS_COUNT,
            YarnConfiguration.DEFAULT_NM_LOG_DELETE_THREAD_COUNT), tf);
    return sched;
  }

  class LogDeleterRunnable implements Runnable {
    private String user;
    private ApplicationId applicationId;

    public LogDeleterRunnable(String user, ApplicationId applicationId) {
      this.user = user;
      this.applicationId = applicationId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      List<String> rootLogDirs =
          NonAggregatingLogHandler.this.dirsHandler.getLogDirs();
      Path[] localAppLogDirs = new Path[rootLogDirs.size()];
      int index = 0;
      for (String rootLogDir : rootLogDirs) {
        localAppLogDirs[index] = new Path(rootLogDir, applicationId.toString());
        index++;
      }
      // Inform the application before the actual delete itself, so that links
      // to logs will no longer be there on NM web-UI. 
      NonAggregatingLogHandler.this.dispatcher.getEventHandler().handle(
          new ApplicationEvent(this.applicationId,
              ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
      NonAggregatingLogHandler.this.delService.delete(user, null,
          localAppLogDirs);
    }

    @Override
    public String toString() {
      return "LogDeleter for AppId " + this.applicationId.toString()
          + ", owned by " + user;
    }
  }
}
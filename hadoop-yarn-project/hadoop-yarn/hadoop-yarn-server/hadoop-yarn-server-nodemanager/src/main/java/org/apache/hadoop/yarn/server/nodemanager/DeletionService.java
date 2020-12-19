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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.service.AbstractService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DeletionService extends AbstractService {
  static final Log LOG = LogFactory.getLog(DeletionService.class);
  private int debugDelay;
  private final ContainerExecutor exec;
  private ScheduledThreadPoolExecutor sched;
  private final FileContext lfs = getLfs();
  static final FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  public DeletionService(ContainerExecutor exec) {
    super(DeletionService.class.getName());
    this.exec = exec;
    this.debugDelay = 0;
  }
  
  /**
   * 
  /**
   * Delete the path(s) as this user.
   * @param user The user to delete as, or the JVM user if null
   * @param subDir the sub directory name
   * @param baseDirs the base directories which contains the subDir's
   */
  public void delete(String user, Path subDir, Path... baseDirs) {
    // TODO if parent owned by NM, rename within parent inline
    if (debugDelay != -1) {
      sched.schedule(new FileDeletion(user, subDir, baseDirs), debugDelay,
          TimeUnit.SECONDS);
    }
  }

  @Override
  public void init(Configuration conf) {
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("DeletionService #%d")
      .build();
    if (conf != null) {
      sched = new ScheduledThreadPoolExecutor(
          conf.getInt(YarnConfiguration.NM_DELETE_THREAD_COUNT, YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT),
          tf);
      debugDelay = conf.getInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0);
    } else {
      sched = new ScheduledThreadPoolExecutor(YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT,
          tf);
    }
    sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    sched.setKeepAliveTime(60L, SECONDS);
    super.init(conf);
  }

  @Override
  public void stop() {
    sched.shutdown();
    boolean terminated = false;
    try {
      terminated = sched.awaitTermination(10, SECONDS);
    } catch (InterruptedException e) {
    }
    if (terminated != true) {
      sched.shutdownNow();
    }
    super.stop();
  }

  /**
   * Determine if the service has completely stopped.
   * Used only by unit tests
   * @return true if service has completely stopped
   */
  @Private
  public boolean isTerminated() {
    return getServiceState() == STATE.STOPPED && sched.isTerminated();
  }

  private class FileDeletion implements Runnable {
    final String user;
    final Path subDir;
    final Path[] baseDirs;
    FileDeletion(String user, Path subDir, Path[] baseDirs) {
      this.user = user;
      this.subDir = subDir;
      this.baseDirs = baseDirs;
    }
    @Override
    public void run() {
      if (null == user) {
        if (baseDirs == null || baseDirs.length == 0) {
          LOG.debug("NM deleting absolute path : " + subDir);
          try {
            lfs.delete(subDir, true);
          } catch (IOException e) {
            LOG.warn("Failed to delete " + subDir);
          }
          return;
        }
        for (Path baseDir : baseDirs) {
          Path del = subDir == null? baseDir : new Path(baseDir, subDir);
          LOG.debug("NM deleting path : " + del);
          try {
            lfs.delete(del, true);
          } catch (IOException e) {
            LOG.warn("Failed to delete " + subDir);
          }
        }
      } else {
        try {
          LOG.debug("Deleting path: [" + subDir + "] as user: [" + user + "]");
          exec.deleteAsUser(user, subDir, baseDirs);
        } catch (IOException e) {
          LOG.warn("Failed to delete as user " + user, e);
        } catch (InterruptedException e) {
          LOG.warn("Failed to delete as user " + user, e);
        }
      }
    }
  }
}

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

import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ExitUtil.ExitException;

public class JobTrackerHAServiceProtocol implements HAServiceProtocol {
  
  private static final Log LOG =
    LogFactory.getLog(JobTrackerHAServiceProtocol.class);

  public static final String SYSTEM_DIR_SEQUENCE_PREFIX = "seq-";
  
  private Configuration conf;
  private JobTrackerHADaemon.JobTrackerRunner jtRunner;
  private HAServiceState haState = HAServiceState.STANDBY;
  private FileSystem fs;
  private Path currentSysDir;
  private ScheduledExecutorService sysDirMonitorExecutor;
  private JobTrackerHAHttpRedirector httpRedirector;
  
  public JobTrackerHAServiceProtocol(Configuration conf, JobTrackerHADaemon.JobTrackerRunner jtRunner) {
    this.conf = conf;
    this.jtRunner = jtRunner;
    this.httpRedirector = new JobTrackerHAHttpRedirector(conf);
    try {
      this.fs = createFileSystem(conf);
      httpRedirector.start();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
  }
  
  private FileSystem createFileSystem(final Configuration conf)
      throws IOException, InterruptedException {
    ACLsManager aclsManager = new ACLsManager(conf, null, null);
    return aclsManager.getMROwner().doAs(
      new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return FileSystem.get(conf);
        }
      }
    );
  }

  @VisibleForTesting
  Thread getJobTrackerThread() {
    return jtRunner.getJobTrackerThread();
  }

  private class SystemDirectoryMonitor implements Runnable {
    @Override
    public void run() {
      try {
        if (!fs.exists(currentSysDir)) {
          throw new IOException("System directory " + currentSysDir +
              " no longer exists. New active has started.");
        }
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }
  }

  @Override
  public HAServiceStatus getServiceStatus() throws AccessControlException,
      IOException {
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (haState == HAServiceState.STANDBY || haState == HAServiceState.ACTIVE) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  }

  @Override
  public void monitorHealth() throws HealthCheckFailedException {
    if (haState == HAServiceState.ACTIVE && jtRunner.jtThreadIsNotAlive()) {
      throw new HealthCheckFailedException("The JobTracker thread is not running");
    }
  }

  @Override
  public void transitionToActive(StateChangeRequestInfo reqInfo)
      throws ServiceFailedException, AccessControlException, IOException {
    if (haState == HAServiceState.ACTIVE) {
      LOG.info("Already in active state.");
      return;
    }
    LOG.info("Transitioning to active");
    try {
      httpRedirector.stop();
      JobConf jtConf = new JobConf(conf);
      currentSysDir = rollSystemDirectory(jtConf);
      // Update the conf for the JT so the address is resolved
      HAUtil.setJtRpcAddress(jtConf);
      
      jtRunner.startJobTracker(jtConf);
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
    long activeCheckMillis = conf.getLong(HAUtil.MR_HA_ACTIVE_CHECK_MILLIS,
        HAUtil.MR_HA_ACTIVE_CHECK_MILLIS_DEFAULT);
    sysDirMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
    sysDirMonitorExecutor.scheduleWithFixedDelay(new SystemDirectoryMonitor(),
        activeCheckMillis, activeCheckMillis, TimeUnit.MILLISECONDS);
    haState = HAServiceState.ACTIVE;
    LOG.info("Transitioned to active");
  }

  /**
   * <p>
   * The system directory (mapred.system.dir) is modified so that it has a
   * strictly increasing sequence number as a part of its path. E.g. if it
   * is set to "/mapred/system" then this method will change it to
   * "/mapred/system/seq-&gt;counter&lt;", where the (zero-padded) counter is
   * one more than the counter for the previous system directory, or zero if
   * none existed before. In the first case the previous system directory is
   * renamed to the new one. If the old active JT is still active, then
   * it will notice that its system directory no longer exists and will
   * shut itself down.
   * </p>
   * @param jtConf
   * @return the new system directory
   * @throws IOException
   */
  @VisibleForTesting
  Path rollSystemDirectory(JobConf jtConf) throws IOException {
    // Find most recent system dir
    Path sysDir = new Path(jtConf.get("mapred.system.dir",
        "/tmp/hadoop/mapred/system"));
    Path qualifiedSysDir = fs.makeQualified(sysDir);
    FileStatus[] subDirectories;
    try {
      subDirectories = fs.listStatus(sysDir, new PathFilter() {
        @Override public boolean accept(Path p) {
          return p.getName().matches(SYSTEM_DIR_SEQUENCE_PREFIX + "\\d+");
        }
      });
    } catch (FileNotFoundException e) {
      subDirectories = null;
    }
    // Find the next system directory by looking for the previous one and
    // incrementing its sequence number
    Path prevSysDir = null;
    if (subDirectories != null && subDirectories.length > 0) {
      Arrays.sort(subDirectories);
      prevSysDir = subDirectories[subDirectories.length - 1].getPath();
    }
    Path nextSysDir;
    if (prevSysDir == null) {
      LOG.info("No previous system directory found");
      nextSysDir = new Path(qualifiedSysDir, createSysDirName(0));
    } else {
      long previous = Long.parseLong(
          prevSysDir.getName().substring(SYSTEM_DIR_SEQUENCE_PREFIX.length()));
      nextSysDir = new Path(qualifiedSysDir, createSysDirName(previous + 1));
      LOG.info("Renaming previous system directory " + prevSysDir +
          " to " + nextSysDir);
      if (!fs.rename(prevSysDir, nextSysDir)) {
        throw new IOException("Could not rename " + prevSysDir +
            " to " + nextSysDir);
      }
    }
    // set nextSysDir on the configuration passed to the JT
    jtConf.set("mapred.system.dir", nextSysDir.toString());
    return nextSysDir;
  }

  /**
   * @return zero padded counter with sys dir prefix
   */
  private String createSysDirName(long counter) {
    String paddedCounter = Strings.padStart("" + counter, 12, '0');
    return SYSTEM_DIR_SEQUENCE_PREFIX + paddedCounter;
  }

  @Override
  public void transitionToStandby(StateChangeRequestInfo reqInfo)
      throws ServiceFailedException, AccessControlException, IOException {
    if (haState == HAServiceState.STANDBY) {
      LOG.info("Already in standby state.");
      return;
    }
    LOG.info("Transitioning to standby");
    try {
      if (sysDirMonitorExecutor != null) {
        sysDirMonitorExecutor.shutdownNow();
      }
      jtRunner.stopJobTracker();
      httpRedirector.start();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
    sysDirMonitorExecutor = null;
    currentSysDir = null;
    haState = HAServiceState.STANDBY;
    LOG.info("Transitioned to standby");
  }
  
  public void stop() {
    LOG.info("Stopping");
    try {
      if (sysDirMonitorExecutor != null) {
        sysDirMonitorExecutor.shutdownNow();
      }
      jtRunner.stopJobTracker();
      httpRedirector.stop();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
    sysDirMonitorExecutor = null;
    currentSysDir = null;
    haState = HAServiceState.STANDBY;
    LOG.info("Stopped");
  }
  
  /**
   * Shutdown the JT immediately in an ungraceful way. Used when it would be
   * unsafe for the JT to continue operating, e.g. during a failed HA state
   * transition.
   * 
   * @param t exception which warrants the shutdown. Printed to the JT log
   *          before exit.
   * @throws ExitException thrown only for testing.
   */
  private synchronized void doImmediateShutdown(Throwable t)
      throws ExitException {
    String message = "Error encountered requiring JT shutdown. " +
        "Shutting down immediately.";
    try {
      LOG.fatal(message, t);
    } catch (Throwable ignored) {
      // This is unlikely to happen, but there's nothing we can do if it does.
    }
    terminate(1, t);
  }

}

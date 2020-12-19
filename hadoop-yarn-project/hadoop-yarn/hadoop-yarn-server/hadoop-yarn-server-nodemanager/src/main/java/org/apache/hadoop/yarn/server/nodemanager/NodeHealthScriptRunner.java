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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * 
 * The class which provides functionality of checking the health of the node
 * using the configured node health script and reporting back to the service
 * for which the health checker has been asked to report.
 */
public class NodeHealthScriptRunner extends AbstractService {

  private static Log LOG = LogFactory.getLog(NodeHealthScriptRunner.class);

  /** Absolute path to the health script. */
  private String nodeHealthScript;
  /** Delay after which node health script to be executed */
  private long intervalTime;
  /** Time after which the script should be timedout */
  private long scriptTimeout;
  /** Timer used to schedule node health monitoring script execution */
  private Timer nodeHealthScriptScheduler;

  /** ShellCommandExecutor used to execute monitoring script */
  ShellCommandExecutor shexec = null;

  /** Configuration used by the checker */
  private Configuration conf;

  /** Pattern used for searching in the output of the node health script */
  static private final String ERROR_PATTERN = "ERROR";

  /** Time out error message */
  static final String NODE_HEALTH_SCRIPT_TIMED_OUT_MSG = "Node health script timed out";

  private boolean isHealthy;

  private String healthReport;

  private long lastReportedTime;

  private TimerTask timer;
  
  private enum HealthCheckerExitStatus {
    SUCCESS,
    TIMED_OUT,
    FAILED_WITH_EXIT_CODE,
    FAILED_WITH_EXCEPTION,
    FAILED
  }


  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * node health script.
   * 
   */
  private class NodeHealthMonitorExecutor extends TimerTask {

    String exceptionStackTrace = "";

    public NodeHealthMonitorExecutor(String[] args) {
      ArrayList<String> execScript = new ArrayList<String>();
      execScript.add(nodeHealthScript);
      if (args != null) {
        execScript.addAll(Arrays.asList(args));
      }
      shexec = new ShellCommandExecutor(execScript
          .toArray(new String[execScript.size()]), null, null, scriptTimeout);
    }

    @Override
    public void run() {
      HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
      try {
        shexec.execute();
      } catch (ExitCodeException e) {
        // ignore the exit code of the script
        status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
      } catch (Exception e) {
        LOG.warn("Caught exception : " + e.getMessage());
        if (!shexec.isTimedOut()) {
          status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
        } else {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
        exceptionStackTrace = StringUtils.stringifyException(e);
      } finally {
        if (status == HealthCheckerExitStatus.SUCCESS) {
          if (hasErrors(shexec.getOutput())) {
            status = HealthCheckerExitStatus.FAILED;
          }
        }
        reportHealthStatus(status);
      }
    }

    /**
     * Method which is used to parse output from the node health monitor and
     * send to the report address.
     * 
     * The timed out script or script which causes IOException output is
     * ignored.
     * 
     * The node is marked unhealthy if
     * <ol>
     * <li>The node health script times out</li>
     * <li>The node health scripts output has a line which begins with ERROR</li>
     * <li>An exception is thrown while executing the script</li>
     * </ol>
     * If the script throws {@link IOException} or {@link ExitCodeException} the
     * output is ignored and node is left remaining healthy, as script might
     * have syntax error.
     * 
     * @param status
     */
    void reportHealthStatus(HealthCheckerExitStatus status) {
      long now = System.currentTimeMillis();
      switch (status) {
      case SUCCESS:
        setHealthStatus(true, "", now);
        break;
      case TIMED_OUT:
        setHealthStatus(false, NODE_HEALTH_SCRIPT_TIMED_OUT_MSG);
        break;
      case FAILED_WITH_EXCEPTION:
        setHealthStatus(false, exceptionStackTrace);
        break;
      case FAILED_WITH_EXIT_CODE:
        setHealthStatus(true, "", now);
        break;
      case FAILED:
        setHealthStatus(false, shexec.getOutput());
        break;
      }
    }

    /**
     * Method to check if the output string has line which begins with ERROR.
     * 
     * @param output
     *          string
     * @return true if output string has error pattern in it.
     */
    private boolean hasErrors(String output) {
      String[] splits = output.split("\n");
      for (String split : splits) {
        if (split.startsWith(ERROR_PATTERN)) {
          return true;
        }
      }
      return false;
    }
  }

  public NodeHealthScriptRunner() {
    super(NodeHealthScriptRunner.class.getName());
    this.lastReportedTime = System.currentTimeMillis();
    this.isHealthy = true;
    this.healthReport = "";    
  }

  /*
   * Method which initializes the values for the script path and interval time.
   */
  @Override
  public void init(Configuration conf) {
    this.conf = conf;
    this.nodeHealthScript = 
        conf.get(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH);
    this.intervalTime = conf.getLong(YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS);
    this.scriptTimeout = conf.getLong(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS);
    String[] args = conf.getStrings(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_OPTS,
        new String[] {});
    timer = new NodeHealthMonitorExecutor(args);
  }

  /**
   * Method used to start the Node health monitoring.
   * 
   */
  @Override
  public void start() {
    // if health script path is not configured don't start the thread.
    if (!shouldRun(conf)) {
      LOG.info("Not starting node health monitor");
      return;
    }
    nodeHealthScriptScheduler = new Timer("NodeHealthMonitor-Timer", true);
    // Start the timer task immediately and
    // then periodically at interval time.
    nodeHealthScriptScheduler.scheduleAtFixedRate(timer, 0, intervalTime);
  }

  /**
   * Method used to terminate the node health monitoring service.
   * 
   */
  @Override
  public void stop() {
    if (!shouldRun(conf)) {
      return;
    }
    nodeHealthScriptScheduler.cancel();
    if (shexec != null) {
      Process p = shexec.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
  }

  /**
   * Gets the if the node is healthy or not
   * 
   * @return true if node is healthy
   */
  public boolean isHealthy() {
    return isHealthy;
  }

  /**
   * Sets if the node is healhty or not considering disks' health also.
   * 
   * @param isHealthy
   *          if or not node is healthy
   */
  private synchronized void setHealthy(boolean isHealthy) {
    this.isHealthy = isHealthy;
  }

  /**
   * Returns output from health script. if node is healthy then an empty string
   * is returned.
   * 
   * @return output from health script
   */
  public String getHealthReport() {
    return healthReport;
  }

  /**
   * Sets the health report from the node health script. Also set the disks'
   * health info obtained from DiskHealthCheckerService.
   *
   * @param healthReport
   */
  private synchronized void setHealthReport(String healthReport) {
    this.healthReport = healthReport;
  }
  
  /**
   * Returns time stamp when node health script was last run.
   * 
   * @return timestamp when node health script was last run
   */
  public long getLastReportedTime() {
    return lastReportedTime;
  }

  /**
   * Sets the last run time of the node health script.
   * 
   * @param lastReportedTime
   */
  private synchronized void setLastReportedTime(long lastReportedTime) {
    this.lastReportedTime = lastReportedTime;
  }

  /**
   * Method used to determine if or not node health monitoring service should be
   * started or not. Returns true if following conditions are met:
   * 
   * <ol>
   * <li>Path to Node health check script is not empty</li>
   * <li>Node health check script file exists</li>
   * </ol>
   * 
   * @param conf
   * @return true if node health monitoring service can be started.
   */
  public static boolean shouldRun(Configuration conf) {
    String nodeHealthScript = 
      conf.get(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH);
    if (nodeHealthScript == null || nodeHealthScript.trim().isEmpty()) {
      return false;
    }
    File f = new File(nodeHealthScript);
    return f.exists() && f.canExecute();
  }

  private synchronized void setHealthStatus(boolean isHealthy, String output) {
    this.setHealthy(isHealthy);
    this.setHealthReport(output);
  }
  
  private synchronized void setHealthStatus(boolean isHealthy, String output,
      long time) {
    this.setHealthStatus(isHealthy, output);
    this.setLastReportedTime(time);
  }

  /**
   * Used only by tests to access the timer task directly
   * @return the timer task
   */
  TimerTask getTimerTask() {
    return timer;
  }
}

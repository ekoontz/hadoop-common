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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * This service tries to add entries in the JHS for applications that failed
 * or were killed
 */
@Private
public class KilledHistoryService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(KilledHistoryService.class);
  private static final Pattern FLAG_FILE_PATTERN =
      Pattern.compile("(\\S+)_appattempt_(\\d+)_(\\d+)_(\\d+)");
  private static final Pattern SUBMIT_TIME_PATTERN =
      Pattern.compile("submitTime=(\\d+)");
  private static final Pattern FINISH_TIME_PATTERN =
      Pattern.compile("finishTime=(\\d+)");
  private static final Pattern JOB_NAME_PATTERN =
      Pattern.compile("jobName=([^,]+)");
  private static final Pattern NUM_MAPS_PATTERN =
      Pattern.compile("numMaps=(\\d+)");
  private static final Pattern NUM_REDUCES_PATTERN =
      Pattern.compile("numReduces=(\\d+)");
  private static final Pattern STATUS_PATTERN =
      Pattern.compile("status=([^,]+)");
  private static final Pattern QUEUE_PATTERN =
      Pattern.compile("queue=([^,]+)");

  private Timer timer = null;
  private long checkIntervalMsecs;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    checkIntervalMsecs = conf.getLong(
        JHAdminConfig.MR_HISTORY_MOVE_INTERVAL_MS,
        JHAdminConfig.DEFAULT_MR_HISTORY_MOVE_INTERVAL_MS);
    super.serviceInit(conf);
  }

  static class FlagFileHandler extends TimerTask {
    private final Configuration conf;
    private Path failDir = null;
    private String intermediateDirPrefix = null;

    public FlagFileHandler(Configuration conf) throws IOException {
      this.conf = conf;
      this.failDir = new Path(conf.get(
          YarnConfiguration.YARN_AM_FAILURE_FLAG_DIR,
          YarnConfiguration.DEFAULT_YARN_AM_FAILURE_FLAG_DIR));
      this.intermediateDirPrefix =
            JobHistoryUtils.getConfiguredHistoryIntermediateDoneDirPrefix(conf);
    }

    @Override
    public void run() {
      try {
        FileSystem failDirFS = failDir.getFileSystem(conf);
        if (failDirFS.exists(failDir)) {
          for (FileStatus flagFileStatus : failDirFS.listStatus(failDir)) {
            String flagFileName = flagFileStatus.getPath().getName();
            Matcher m = FLAG_FILE_PATTERN.matcher(flagFileName);
            if (m.matches()) {
              final String user = m.group(1);
              long timestamp = Long.parseLong(m.group(2));
              int appId = Integer.parseInt(m.group(3));
              final int attempt = Integer.parseInt(m.group(4));
              ApplicationId applicationId =
                  ApplicationId.newInstance(timestamp, appId);
              final JobId jobId =TypeConverter.toYarn(
                  TypeConverter.fromYarn(applicationId));
              final Path intermediateDir = new Path(intermediateDirPrefix, user);
              final Path stagingDirForJob = new Path(
                  MRApps.getStagingAreaDir(conf, user), jobId.toString());
              try {
                final Path inSummaryFile = new Path(stagingDirForJob,
                    JobHistoryUtils.getIntermediateSummaryFileName(jobId));
                UserGroupInformation ugi =
                    UserGroupInformation.createProxyUser(user,
                        UserGroupInformation.getCurrentUser());
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws IOException {
                    JobIndexInfo jobIndexInfo =
                        buildJobIndexInfo(inSummaryFile, jobId, user);
                    String historyFilename =
                        FileNameIndexUtils.getDoneFileName(jobIndexInfo);
                    copy(JobHistoryUtils
                        .getStagingConfFile(stagingDirForJob, jobId, attempt),
                             new Path(intermediateDir, JobHistoryUtils
                                 .getIntermediateConfFileName(jobId)));
                    copy(inSummaryFile, new Path(intermediateDir,
                        JobHistoryUtils.getIntermediateSummaryFileName(jobId)));
                    copy(JobHistoryUtils
                        .getStagingJobHistoryFile(stagingDirForJob, jobId,
                            attempt), new Path(intermediateDir, historyFilename));
                    return null;
                  }
                });
                failDirFS.delete(flagFileStatus.getPath(), false);
              } catch (IOException ioe) {
                removeFlagFileWithMessage(failDirFS, flagFileStatus.getPath(),
                    "Could not process job files", ioe);
              } catch (InterruptedException ie) {
                removeFlagFileWithMessage(failDirFS, flagFileStatus.getPath(),
                    "Could not process job files", ie);
              }
            } else {
              removeFlagFileWithMessage(failDirFS, flagFileStatus.getPath(),
                  "Could not process fail flag file", null);
            }
          }
        }
      } catch (IOException ioe) {
        LOG.info("Could not access fail flag dir", ioe);
      }
    }

    private void removeFlagFileWithMessage(FileSystem failDirFS, Path flagFile,
        String message, Exception ex) {
      if (ex == null) {
        LOG.warn(message);
      } else {
        LOG.warn(message, ex);
      }
      // Try to delete the flag file so we don't keep trying to process it
      try {
        failDirFS.delete(flagFile, false);
      } catch (IOException ioe) {
        // ignore
      }
    }

    private void copy(Path fromPath, Path toPath) throws IOException {
      FileSystem fromFs = fromPath.getFileSystem(conf);
      FileSystem toFs = toPath.getFileSystem(conf);
      LOG.info("Copying " + fromPath.toString() + " to " + toPath.toString());
      boolean copied = FileUtil.copy(toFs, fromPath, fromFs, toPath,
          false, conf);
      if (copied) {
        LOG.info("Copied to done location: " + toPath);
      } else {
        LOG.info("copy failed");
      }
      toFs.setPermission(toPath, new FsPermission(
          JobHistoryUtils.HISTORY_INTERMEDIATE_FILE_PERMISSIONS));
    }

    private JobIndexInfo buildJobIndexInfo(Path summaryFile, JobId jobId,
        String user) throws IOException {
      FileSystem fs = summaryFile.getFileSystem(conf);
      FSDataInputStream in = fs.open(summaryFile);
      String summaryString = in.readUTF();
      in.close();
      long submitTime =
          extractLong(SUBMIT_TIME_PATTERN, summaryString, "submitTime");
      long finishTime =
          extractLong(FINISH_TIME_PATTERN, summaryString, "finishTime");
      if (finishTime == 0) {
        finishTime = submitTime;  // prevent JHS from thinking it's too old
      }
      String jobName =
          extractString(JOB_NAME_PATTERN, summaryString, "jobName");
      int numMaps = extractInt(NUM_MAPS_PATTERN, summaryString, "numMaps");
      if (numMaps == 0) {
        numMaps = -1;
      }
      int numReduces =
          extractInt(NUM_REDUCES_PATTERN, summaryString, "numReduces");
      if (numReduces == 0) {
        numReduces = -1;
      }
      String jobStatus = extractString(STATUS_PATTERN, summaryString, "status");
      if (jobStatus.equals("null")) {
        jobStatus = "FAILED"; // assume FAILED
      }
      String queue = extractString(QUEUE_PATTERN, summaryString, "queue");
      JobIndexInfo info = new JobIndexInfo(submitTime, finishTime, user,
          jobName, jobId, numMaps, numReduces, jobStatus);
      info.setQueueName(queue);
      return info;
    }

    private String extractString(Pattern pattern, String str, String type)
        throws IOException {
      String result = null;
      Matcher m = pattern.matcher(str);
      if (m.find()) {
        result = m.group(1);
      } else {
        throw new IOException("Could not extract " + type
            + " field from summary file");
      }
      return result;
    }

    private long extractLong(Pattern pattern, String str, String type)
        throws IOException {
      String result = extractString(pattern, str, type);
      return (result == null) ? -1L : Long.parseLong(result);
    }

    private int extractInt(Pattern pattern, String str, String type)
        throws IOException {
      String result = extractString(pattern, str, type);
      return (result == null) ? -1 : Integer.parseInt(result);
    }
  }

  public KilledHistoryService() {
    super(KilledHistoryService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    scheduleFlagHandlerTask();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopTimer();
    super.serviceStop();
  }

  private void scheduleFlagHandlerTask() throws IOException {
    Configuration conf = getConfig();
    TimerTask task = new FlagFileHandler(conf);
    timer = new Timer();
    timer.scheduleAtFixedRate(task, 0, checkIntervalMsecs);
  }

  private void stopTimer() {
    if (timer != null) {
      timer.cancel();
    }
  }
}

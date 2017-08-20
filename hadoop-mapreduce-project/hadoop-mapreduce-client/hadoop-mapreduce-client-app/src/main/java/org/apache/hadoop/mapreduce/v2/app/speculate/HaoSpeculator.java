package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.impl.MapTaskImpl;
import org.apache.hadoop.yarn.util.Clock;

public class HaoSpeculator extends DefaultSpeculator {
	
//  public static final String HAO_PARAM_AVERAGE_SPILL_DURATION_SECONDS = "hao.average.map.spill.duration.seconds";
//  public static final String HAO_PARAM_MINIMUN_SPECULATE_SPILL_COUNT_DELAY = "hao.minimum.speculate.spill.count.delay";
//  public static final String HAO_PARAM_MINIMUN_SPECULATE_SECONDS_DELAY = "hao.minimun.speculate.seconds.delay";

//  private int haoMinimunSpeculateSpillCountDelay;
//  private long haoMinumunSpeculateSecondsDelay;
//  private boolean haoVerbose = true;
//  
  public HaoSpeculator(Configuration conf, AppContext context, Clock clock) {
    super(conf, context, clock);
    
    this.haoMinimunSpeculateSpillCountDelay = conf.getFloat(HAO_PARAM_MINIMUN_SPECULATE_SPILL_COUNT_DELAY, 0.1F);
    this.haoMinumunSpeculateSecondsDelay = conf.getLong(HAO_PARAM_MINIMUN_SPECULATE_SECONDS_DELAY, 15);
    haoVerbose = conf.getBoolean("hao.verbose", true);
  }

  public HaoSpeculator(Configuration conf, AppContext context,
      TaskRuntimeEstimator estimator, Clock clock) {
    super(conf, context, estimator, clock);
  }

  public HaoSpeculator(Configuration conf, AppContext context) {
    super(conf, context);
    LOG.info("HAO: HaoSpeculator(CONF, CONTEXT)");
  }
//
//  protected static final Log LOG = LogFactory.getLog(HaoSpeculator.class);
//
//  @Override
//  protected long speculationValue(TaskId taskID, long now) {
//    Job job = context.getJob(taskID.getJobId());
//    Task task = job.getTask(taskID);
//    Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
//    long acceptableRuntime = Long.MIN_VALUE;
//    long result = Long.MIN_VALUE;
//
//    if (!mayHaveSpeculated.contains(taskID)) {
//      acceptableRuntime = estimator.thresholdRuntime(taskID);
//      if (acceptableRuntime == Long.MAX_VALUE) {
//        return ON_SCHEDULE;
//      }
//    }
//
//    TaskAttemptId runningTaskAttemptID = null;
//
//    int numberRunningAttempts = 0;
//
//    for (TaskAttempt taskAttempt : attempts.values()) {
//      if (taskAttempt.getState() == TaskAttemptState.RUNNING
//          || taskAttempt.getState() == TaskAttemptState.STARTING) {
//        if (++numberRunningAttempts > 1) {
//          return ALREADY_SPECULATING;
//        }
//        runningTaskAttemptID = taskAttempt.getID();
//
//        long estimatedRunTime = estimator.estimatedRuntime(runningTaskAttemptID);
//        if (estimatedRunTime <= 0) {
//        	continue;
//        }
//
//        long taskAttemptStartTime
//            = estimator.attemptEnrolledTime(runningTaskAttemptID);
////        long averageSpillDuration = conf.getLong(HAO_PARAM_AVERAGE_SPILL_DURATION_SECONDS, 0);
//        long averageSpillDuration = DefaultSpeculator.averageSpillDuration;
//        
//        long haodelay = Math.max((long)(averageSpillDuration * this.haoMinimunSpeculateSpillCountDelay), this.haoMinumunSpeculateSecondsDelay);
//        haodelay *= 1000;
//        
//        if (haoVerbose) {
//        LOG.info(String.format("HaoSpeculator: taskId=%s averageSpillDuration=%d, haoDelay=%d, estimatedRunTime=%d, taskAttemptStartTime=%d, now=%d",
//        		taskAttempt.getID().toString(), averageSpillDuration, haodelay, estimatedRunTime, taskAttemptStartTime, now));
//        }
//        
////        if (LOG.isDebugEnabled()) {
////            LOG.debug("HAO-SPECULATOR: averageSpillDuration=" + averageSpillDuration + " haodelay=" + haodelay);
////        }
//        
//        if (taskAttemptStartTime + haodelay > now) {
//          // This background process ran before we could process the task
//          //  attempt status change that chronicles the attempt start
//          return TOO_NEW;
//        }
//
//        long estimatedEndTime = estimatedRunTime + taskAttemptStartTime;
//
//        // HAO
//        
//        long estimatedReplacementEndTime = 0;
//        if (task instanceof MapTaskImpl) {
//          MapTaskImpl mapTask = (MapTaskImpl)task;
//          System.out.println("HAO: speculate mapRatio=" + mapTask.getFinishedRatio());
//          long estimate = (long)(estimator.estimatedNewAttemptRuntime(taskID) * (1-mapTask.getFinishedRatio()));
//          estimatedReplacementEndTime = now + estimate;
//        } else {
//          estimatedReplacementEndTime = now + estimator.estimatedNewAttemptRuntime(taskID);
//        }
//
//        if (haoVerbose) {
//        LOG.info(String.format("HaoSpeculator: taskId=%s now=%d estimatedReplacementEndTime=%d", taskAttempt.getID().toString(), now, estimatedReplacementEndTime));
//        }
//
//        float progress = taskAttempt.getProgress();
//        TaskAttemptHistoryStatistics data =
//            runningTaskAttemptStatistics.get(runningTaskAttemptID);
//        if (data == null) {
//          runningTaskAttemptStatistics.put(runningTaskAttemptID,
//            new TaskAttemptHistoryStatistics(estimatedRunTime, progress, now));
//        } else {
//          if (estimatedRunTime == data.getEstimatedRunTime()
//              && progress == data.getProgress()) {
//            // Previous stats are same as same stats
//            if (data.notHeartbeatedInAWhile(now)) {
//              // Stats have stagnated for a while, simulate heart-beat.
//              TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatus();
//              taskAttemptStatus.id = runningTaskAttemptID;
//              taskAttemptStatus.progress = progress;
//              taskAttemptStatus.taskState = taskAttempt.getState();
//              // Now simulate the heart-beat
//              handleAttempt(taskAttemptStatus);
//            }
//          } else {
//            // Stats have changed - update our data structure
//            data.setEstimatedRunTime(estimatedRunTime);
//            data.setProgress(progress);
//            data.resetHeartBeatTime(now);
//          }
//        }
//
//        if (estimatedEndTime < now + haodelay) {
//          return PROGRESS_IS_GOOD;
//        }
//
//        if (estimatedReplacementEndTime >= estimatedEndTime) {
//          return TOO_LATE_TO_SPECULATE;
//        }
//
//        result = estimatedEndTime - estimatedReplacementEndTime;
//      }
//    }
//
//    // If we are here, there's at most one task attempt.
//    if (numberRunningAttempts == 0) {
//      return NOT_RUNNING;
//    }
//
//
//
//    if (acceptableRuntime == Long.MIN_VALUE) {
//      acceptableRuntime = estimator.thresholdRuntime(taskID);
//      if (acceptableRuntime == Long.MAX_VALUE) {
//        return ON_SCHEDULE;
//      }
//    }
//
//    return result;
//  }
  
}

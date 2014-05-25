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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapInputRange;
import org.apache.hadoop.mapred.MapInputRangeList;
import org.apache.hadoop.mapred.MapSpillInfo;
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

import com.sun.tools.javac.util.Position;

@SuppressWarnings({ "rawtypes" })
public class MapTaskImpl extends TaskImpl {

  private final TaskSplitMetaInfo taskSplitMetaInfo;

  private static final Log LOG = LogFactory.getLog(MapTaskImpl.class);

  public MapTaskImpl(JobId jobId, int partition, EventHandler eventHandler,
      Path remoteJobConfFile, JobConf conf,
      TaskSplitMetaInfo taskSplitMetaInfo,
      TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      int appAttemptId, MRAppMetrics metrics, AppContext appContext) {
    super(jobId, TaskType.MAP, partition, eventHandler, remoteJobConfFile,
        conf, taskAttemptListener, jobToken, credentials, clock,
        appAttemptId, metrics, appContext);
    this.taskSplitMetaInfo = taskSplitMetaInfo;
  }

  @Override
  protected int getMaxAttempts() {
    return conf.getInt(MRJobConfig.MAP_MAX_ATTEMPTS, 4);
  }

  @Override
  protected TaskAttemptImpl createAttempt() {
    return new MapTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, taskSplitMetaInfo, conf, taskAttemptListener,
        jobToken, credentials, clock, appContext);
  }

  @Override
  public TaskType getType() {
    return TaskType.MAP;
  }

  protected TaskSplitMetaInfo getTaskSplitMetaInfo() {
    return this.taskSplitMetaInfo;
  }
  
  ReentrantReadWriteLock finishedRangesLock = new ReentrantReadWriteLock();
  Lock readLock = finishedRangesLock.readLock();
  Lock writeLock = finishedRangesLock.writeLock();
  
  private List<MapSpillInfo> finishedRanges = new ArrayList<MapSpillInfo>();
  
  protected void handleNewSpill(TaskImpl task, TaskEvent event) {
    
    if (event.getSpillMapAttemptID().getId() != currentMapAttemptId) {
      return;
    }
    
    try {
      writeLock.lock();
      

      MapSpillInfo newRange = event.getSpillInfo();
      int mypos = 0;
      while (mypos < finishedRanges.size()) {
        MapSpillInfo range = finishedRanges.get(mypos);
        if (newRange.getStart() >= range.getStart()) {
          ++mypos;
        } else {
          break;
        }
      }
      // check if new range is disjoint with neighbors
      boolean leftDisjoint = true;
      if (mypos > 0) {
        MapSpillInfo left = finishedRanges.get(mypos - 1);
        if (left.getEnd() > newRange.getStart()) {
          leftDisjoint = false;
        }
      }
      boolean rightDisjoint = true;
      if (mypos < finishedRanges.size()) {
        MapSpillInfo right = finishedRanges.get(mypos);
        if (newRange.getEnd() > right.getStart()) {
          rightDisjoint = false;
        }
      }
      if (leftDisjoint == false || rightDisjoint == false) {
        LOG.info("discarding spill " + newRange + " of " + event.getSpillMapAttemptID()
                  + " because leftDisjoint=" + leftDisjoint + " rightDisjoint=" + rightDisjoint
                  + " mypos=" + mypos + " currentRanges=" + finishedRanges);
        return;
      }
      
      finishedRanges.add(mypos, newRange);
      LOG.info(" handleNewSpill map " + event.getSpillMapAttemptID().getTaskID().getId() + " new range: " + mypos + " " + newRange + " after=" + finishedRanges);
      
      JobEvent jobEvent = new JobEvent(task.getID().getJobId(), JobEventType.JOB_NEW_MAP_SPILL);
      jobEvent.setSpillInfo(event.getSpillInfo());
     
      jobEvent.setMapId(event.getSpillMapAttemptID());

      String scheme = (task.encryptedShuffle) ? "https://" : "http://";
      String url = scheme + event.getHost() + ":" + event.getPort();
      jobEvent.setNodeHttp(url);
      
      task.eventHandler.handle(jobEvent);
      
      // TODO HAO
      
    } finally {
      writeLock.unlock();
    }
    
    
  }
  
  public MapInputRangeList getMapAttemptIgnoreRanges(TaskAttemptID taskid) {
    readLock.lock();
    try {
      List<MapInputRange> ranges = new ArrayList<MapInputRange>();
      for (MapSpillInfo spill : finishedRanges) {
        ranges.add(new MapInputRange(spill.getStart(), spill.getEnd()));
      }

      MapInputRangeList list = new MapInputRangeList();
      list.setRanges(ranges);
      return list;
    } finally {
      readLock.unlock();
    }
    
  }

  /**
   * @return a String formatted as a comma-separated list of splits.
   */
  @Override
  protected String getSplitsAsString() {
    String[] splits = getTaskSplitMetaInfo().getLocations();
    if (splits == null || splits.length == 0)
    return "";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < splits.length; i++) {
      if (i != 0) sb.append(",");
      sb.append(splits[i]);
    }
    return sb.toString();
  }
}

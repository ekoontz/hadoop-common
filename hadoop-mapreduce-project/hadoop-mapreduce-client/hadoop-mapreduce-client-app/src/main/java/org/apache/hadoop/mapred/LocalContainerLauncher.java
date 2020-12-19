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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * Runs the container task locally in a thread.
 * Since all (sub)tasks share the same local directory, they must be executed
 * sequentially in order to avoid creating/deleting the same files/dirs.
 */
public class LocalContainerLauncher extends AbstractService implements
    ContainerLauncher {

  private static final File curDir = new File(".");
  private static final Log LOG = LogFactory.getLog(LocalContainerLauncher.class);

  private FileContext curFC = null;
  private final HashSet<File> localizedFiles;
  private final AppContext context;
  private final TaskUmbilicalProtocol umbilical;
  private Thread eventHandlingThread;
  private BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();

  public LocalContainerLauncher(AppContext context,
                                TaskUmbilicalProtocol umbilical) {
    super(LocalContainerLauncher.class.getName());
    this.context = context;
    this.umbilical = umbilical;
        // umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
        // (TODO/FIXME:  pointless to use RPC to talk to self; should create
        // LocalTaskAttemptListener or similar:  implement umbilical protocol
        // but skip RPC stuff)

    try {
      curFC = FileContext.getFileContext(curDir.toURI());
    } catch (UnsupportedFileSystemException ufse) {
      LOG.error("Local filesystem " + curDir.toURI().toString()
                + " is unsupported?? (should never happen)");
    }

    // Save list of files/dirs that are supposed to be present so can delete
    // any extras created by one task before starting subsequent task.  Note
    // that there's no protection against deleted or renamed localization;
    // users who do that get what they deserve (and will have to disable
    // uberization in order to run correctly).
    File[] curLocalFiles = curDir.listFiles();
    localizedFiles = new HashSet<File>(curLocalFiles.length);
    for (int j = 0; j < curLocalFiles.length; ++j) {
      localizedFiles.add(curLocalFiles[j]);
    }

    // Relocalization note/future FIXME (per chrisdo, 20110315):  At moment,
    // full localization info is in AppSubmissionContext passed from client to
    // RM and then to NM for AM-container launch:  no difference between AM-
    // localization and MapTask- or ReduceTask-localization, so can assume all
    // OK.  Longer-term, will need to override uber-AM container-localization
    // request ("needed resources") with union of regular-AM-resources + task-
    // resources (and, if maps and reduces ever differ, then union of all three
    // types), OR will need localizer service/API that uber-AM can request
    // after running (e.g., "localizeForTask()" or "localizeForMapTask()").
  }

  public void start() {
    eventHandlingThread = new Thread(new SubtaskRunner(), "uber-SubtaskRunner");
    eventHandlingThread.start();
    super.start();
  }

  public void stop() {
    eventHandlingThread.interrupt();
    super.stop();
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);  // FIXME? YarnException is "for runtime exceptions only"
    }
  }


  /*
   * Uber-AM lifecycle/ordering ("normal" case):
   *
   * - [somebody] sends TA_ASSIGNED
   *   - handled by ContainerAssignedTransition (TaskAttemptImpl.java)
   *     - creates "remoteTask" for us == real Task
   *     - sends CONTAINER_REMOTE_LAUNCH
   *     - TA: UNASSIGNED -> ASSIGNED
   * - CONTAINER_REMOTE_LAUNCH handled by LocalContainerLauncher (us)
   *   - sucks "remoteTask" out of TaskAttemptImpl via getRemoteTask()
   *   - sends TA_CONTAINER_LAUNCHED
   *     [[ elsewhere...
   *       - TA_CONTAINER_LAUNCHED handled by LaunchedContainerTransition
   *         - registers "remoteTask" with TaskAttemptListener (== umbilical)
   *         - NUKES "remoteTask"
   *         - sends T_ATTEMPT_LAUNCHED (Task: SCHEDULED -> RUNNING)
   *         - TA: ASSIGNED -> RUNNING
   *     ]]
   *   - runs Task (runSubMap() or runSubReduce())
   *     - TA can safely send TA_UPDATE since in RUNNING state
   */
  private class SubtaskRunner implements Runnable {

    private boolean doneWithMaps = false;
    private int finishedSubMaps = 0;

    SubtaskRunner() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      ContainerLauncherEvent event = null;

      // _must_ either run subtasks sequentially or accept expense of new JVMs
      // (i.e., fork()), else will get weird failures when maps try to create/
      // write same dirname or filename:  no chdir() in Java
      while (!Thread.currentThread().isInterrupted()) {
        try {
          event = eventQueue.take();
        } catch (InterruptedException e) {  // mostly via T_KILL? JOB_KILL?
          LOG.error("Returning, interrupted : " + e);
          return;
        }

        LOG.info("Processing the event " + event.toString());

        if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {

          ContainerRemoteLaunchEvent launchEv =
              (ContainerRemoteLaunchEvent)event;
          TaskAttemptId attemptID = launchEv.getTaskAttemptID(); 

          Job job = context.getAllJobs().get(attemptID.getTaskId().getJobId());
          int numMapTasks = job.getTotalMaps();
          int numReduceTasks = job.getTotalReduces();

          // YARN (tracking) Task:
          org.apache.hadoop.mapreduce.v2.app.job.Task ytask =
              job.getTask(attemptID.getTaskId());
          // classic mapred Task:
          org.apache.hadoop.mapred.Task remoteTask = launchEv.getRemoteTask();

          // after "launching," send launched event to task attempt to move
          // state from ASSIGNED to RUNNING (also nukes "remoteTask", so must
          // do getRemoteTask() call first)
          
          //There is no port number because we are not really talking to a task
          // tracker.  The shuffle is just done through local files.  So the
          // port number is set to -1 in this case.
          context.getEventHandler().handle(
              new TaskAttemptContainerLaunchedEvent(attemptID, -1));

          if (numMapTasks == 0) {
            doneWithMaps = true;
          }

          try {
            if (remoteTask.isMapOrReduce()) {
              JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.getTaskId().getJobId());
              jce.addCounterUpdate(JobCounter.TOTAL_LAUNCHED_UBERTASKS, 1);
              if (remoteTask.isMapTask()) {
                jce.addCounterUpdate(JobCounter.NUM_UBER_SUBMAPS, 1);
              } else {
                jce.addCounterUpdate(JobCounter.NUM_UBER_SUBREDUCES, 1);
              }
              context.getEventHandler().handle(jce);
            }
            runSubtask(remoteTask, ytask.getType(), attemptID, numMapTasks,
                       (numReduceTasks > 0));
            
          } catch (RuntimeException re) {
            JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.getTaskId().getJobId());
            jce.addCounterUpdate(JobCounter.NUM_FAILED_UBERTASKS, 1);
            context.getEventHandler().handle(jce);
            // this is our signal that the subtask failed in some way, so
            // simulate a failed JVM/container and send a container-completed
            // event to task attempt (i.e., move state machine from RUNNING
            // to FAIL_CONTAINER_CLEANUP [and ultimately to FAILED])
            context.getEventHandler().handle(new TaskAttemptEvent(attemptID,
                TaskAttemptEventType.TA_CONTAINER_COMPLETED));
          } catch (IOException ioe) {
            // if umbilical itself barfs (in error-handler of runSubMap()),
            // we're pretty much hosed, so do what YarnChild main() does
            // (i.e., exit clumsily--but can never happen, so no worries!)
            LOG.fatal("oopsie...  this can never happen: "
                + StringUtils.stringifyException(ioe));
            System.exit(-1);
          }

        } else if (event.getType() == EventType.CONTAINER_REMOTE_CLEANUP) {

          // no container to kill, so just send "cleaned" event to task attempt
          // to move us from SUCCESS_CONTAINER_CLEANUP to SUCCEEDED state
          // (or {FAIL|KILL}_CONTAINER_CLEANUP to {FAIL|KILL}_TASK_CLEANUP)
          context.getEventHandler().handle(
              new TaskAttemptEvent(event.getTaskAttemptID(),
                  TaskAttemptEventType.TA_CONTAINER_CLEANED));

        } else {
          LOG.warn("Ignoring unexpected event " + event.toString());
        }

      }
    }

    @SuppressWarnings("deprecation")
    private void runSubtask(org.apache.hadoop.mapred.Task task,
                            final TaskType taskType,
                            TaskAttemptId attemptID,
                            final int numMapTasks,
                            boolean renameOutputs)
    throws RuntimeException, IOException {
      org.apache.hadoop.mapred.TaskAttemptID classicAttemptID =
          TypeConverter.fromYarn(attemptID);

      try {
        JobConf conf = new JobConf(getConfig());
        conf.set(JobContext.TASK_ID, task.getTaskID().toString());
        conf.set(JobContext.TASK_ATTEMPT_ID, classicAttemptID.toString());
        conf.setBoolean(JobContext.TASK_ISMAP, (taskType == TaskType.MAP));
        conf.setInt(JobContext.TASK_PARTITION, task.getPartition());
        conf.set(JobContext.ID, task.getJobID().toString());

        // Use the AM's local dir env to generate the intermediate step 
        // output files
        String[] localSysDirs = StringUtils.getTrimmedStrings(
            System.getenv(ApplicationConstants.LOCAL_DIR_ENV));
        conf.setStrings(MRConfig.LOCAL_DIR, localSysDirs);
        LOG.info(MRConfig.LOCAL_DIR + " for uber task: "
            + conf.get(MRConfig.LOCAL_DIR));

        // mark this as an uberized subtask so it can set task counter
        // (longer-term/FIXME:  could redefine as job counter and send
        // "JobCounterEvent" to JobImpl on [successful] completion of subtask;
        // will need new Job state-machine transition and JobImpl jobCounters
        // map to handle)
        conf.setBoolean("mapreduce.task.uberized", true);

        // META-FIXME: do we want the extra sanity-checking (doneWithMaps,
        // etc.), or just assume/hope the state machine(s) and uber-AM work
        // as expected?
        if (taskType == TaskType.MAP) {
          if (doneWithMaps) {
            LOG.error("CONTAINER_REMOTE_LAUNCH contains a map task ("
                      + attemptID + "), but should be finished with maps");
            throw new RuntimeException();
          }

          MapTask map = (MapTask)task;
          map.setConf(conf);

          map.run(conf, umbilical);

          if (renameOutputs) {
            renameMapOutputForReduce(conf, attemptID, map.getMapOutputFile());
          }
          relocalize();

          if (++finishedSubMaps == numMapTasks) {
            doneWithMaps = true;
          }

        } else /* TaskType.REDUCE */ {

          if (!doneWithMaps) {
            // check if event-queue empty?  whole idea of counting maps vs. 
            // checking event queue is a tad wacky...but could enforce ordering
            // (assuming no "lost events") at LocalMRAppMaster [CURRENT BUG(?): 
            // doesn't send reduce event until maps all done]
            LOG.error("CONTAINER_REMOTE_LAUNCH contains a reduce task ("
                      + attemptID + "), but not yet finished with maps");
            throw new RuntimeException();
          }

          // a.k.a. "mapreduce.jobtracker.address" in LocalJobRunner:
          // set framework name to local to make task local
          conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
          conf.set(MRConfig.MASTER_ADDRESS, "local");  // bypass shuffle

          ReduceTask reduce = (ReduceTask)task;
          reduce.setConf(conf);          

          reduce.run(conf, umbilical);
          //relocalize();  // needed only if more than one reducer supported (is MAPREDUCE-434 fixed yet?)
        }

      } catch (FSError e) {
        LOG.fatal("FSError from child", e);
        // umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
        umbilical.fsError(classicAttemptID, e.getMessage());
        throw new RuntimeException();

      } catch (Exception exception) {
        LOG.warn("Exception running local (uberized) 'child' : "
            + StringUtils.stringifyException(exception));
        try {
          if (task != null) {
            // do cleanup for the task
            task.taskCleanup(umbilical);
          }
        } catch (Exception e) {
          LOG.info("Exception cleaning up: "
              + StringUtils.stringifyException(e));
        }
        // Report back any failures, for diagnostic purposes
        umbilical.reportDiagnosticInfo(classicAttemptID, 
            StringUtils.stringifyException(exception));
        throw new RuntimeException();

      } catch (Throwable throwable) {
        LOG.fatal("Error running local (uberized) 'child' : "
            + StringUtils.stringifyException(throwable));
        Throwable tCause = throwable.getCause();
        String cause = (tCause == null)
            ? throwable.getMessage()
                : StringUtils.stringifyException(tCause);
            umbilical.fatalError(classicAttemptID, cause);
        throw new RuntimeException();
      }
    }

    /**
     * Within the _local_ filesystem (not HDFS), all activity takes place within
     * a single subdir (${local.dir}/usercache/$user/appcache/$appId/$contId/),
     * and all sub-MapTasks create the same filename ("file.out").  Rename that
     * to something unique (e.g., "map_0.out") to avoid collisions.
     *
     * Longer-term, we'll modify [something] to use TaskAttemptID-based
     * filenames instead of "file.out". (All of this is entirely internal,
     * so there are no particular compatibility issues.)
     */
    @SuppressWarnings("deprecation")
    private void renameMapOutputForReduce(JobConf conf, TaskAttemptId mapId,
                                          MapOutputFile subMapOutputFile)
    throws IOException {
      FileSystem localFs = FileSystem.getLocal(conf);
      // move map output to reduce input
      Path mapOut = subMapOutputFile.getOutputFile();
      FileStatus mStatus = localFs.getFileStatus(mapOut);      
      Path reduceIn = subMapOutputFile.getInputFileForWrite(
          TypeConverter.fromYarn(mapId).getTaskID(), mStatus.getLen());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Renaming map output file for task attempt "
            + mapId.toString() + " from original location " + mapOut.toString()
            + " to destination " + reduceIn.toString());
      }
      if (!localFs.mkdirs(reduceIn.getParent())) {
        throw new IOException("Mkdirs failed to create "
            + reduceIn.getParent().toString());
      }
      if (!localFs.rename(mapOut, reduceIn))
        throw new IOException("Couldn't rename " + mapOut);
    }

    /**
     * Also within the local filesystem, we need to restore the initial state
     * of the directory as much as possible.  Compare current contents against
     * the saved original state and nuke everything that doesn't belong, with
     * the exception of the renamed map outputs.
     *
     * Any jobs that go out of their way to rename or delete things from the
     * local directory are considered broken and deserve what they get...
     */
    private void relocalize() {
      File[] curLocalFiles = curDir.listFiles();
      for (int j = 0; j < curLocalFiles.length; ++j) {
        if (!localizedFiles.contains(curLocalFiles[j])) {
          // found one that wasn't there before:  delete it
          boolean deleted = false;
          try {
            if (curFC != null) {
              // this is recursive, unlike File delete():
              deleted = curFC.delete(new Path(curLocalFiles[j].getName()),true);
            }
          } catch (IOException e) {
            deleted = false;
          }
          if (!deleted) {
            LOG.warn("Unable to delete unexpected local file/dir "
                + curLocalFiles[j].getName() + ": insufficient permissions?");
          }
        }
      }
    }

  } // end SubtaskRunner

}

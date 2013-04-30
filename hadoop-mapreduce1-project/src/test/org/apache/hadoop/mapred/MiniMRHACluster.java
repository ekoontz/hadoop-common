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

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.MiniMRHACluster.TaskTrackerRunner;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.util.ExitUtil;

public class MiniMRHACluster {
  
  private static final Log LOG = 
    LogFactory.getLog(MiniMRHACluster.class);

  public static final String LOGICAL_NAME = "logicaljt";

  private Configuration conf; // client and tt configuration
  private List<JobTrackerHADaemon> jtHaDaemonList = new ArrayList<JobTrackerHADaemon>();

  private List<TaskTrackerRunner> taskTrackerList = new ArrayList<TaskTrackerRunner>();
  private List<Thread> taskTrackerThreadList = new ArrayList<Thread>();
  
  public MiniMRHACluster() throws IOException, InterruptedException {
    this(new Configuration());
  }
  public MiniMRHACluster(Configuration conf) throws IOException, InterruptedException {
    this(conf, 2, 1);
  }
  public MiniMRHACluster(Configuration conf, int numJobTrackers, int numTaskTrackers) throws IOException, InterruptedException {
    this.conf = conf;
    ExitUtil.disableSystemExit();
    configureLogicalName(conf);
    
    for (int i = 0; i < numJobTrackers; i++) {
      JobTrackerHADaemon jtHaDaemon = createJobTrackerHADaemon(conf, "jt" + (i + 1));
      jtHaDaemon.start();
      jtHaDaemonList.add(jtHaDaemon);
      Thread.sleep(1000); // wait so jt identifiers are different
    }
  }
  
  public JobTrackerHADaemon getJobTrackerHaDaemon(int index) {
    return jtHaDaemonList.get(index);
  }
  
  public Configuration getClientConf() {
    return conf;
  }
  
  private static JobTrackerHADaemon createJobTrackerHADaemon(Configuration conf, String jtId) throws IOException {
    Configuration c = new Configuration(conf);
    c.setBoolean("mapred.jobtracker.restart.recover", true); // recover jobs
    c.set(HAUtil.MR_HA_JOBTRACKER_ID_KEY, jtId);
    return new JobTrackerHADaemon(c);
  }

  private static int findEphemeralPort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }
  
  public static void configureLogicalName(Configuration conf) throws IOException {
    String jt1Id = "jt1";
    String jt2Id = "jt2";

    // We can't use port number 0 directly since a free port will
    // not be picked until the JTs are started, and the standby JT will
    // not start until failover, which is too late for the client.
    // Instead we find a free port by opening then immediately closing
    // a server socket.
    String jt1Address = "localhost:" + findEphemeralPort();
    String jt2Address = "localhost:" + findEphemeralPort();
    String jt1HaAddress = "localhost:" + findEphemeralPort();
    String jt2HaAddress = "localhost:" + findEphemeralPort();
    String jt1HttpAddress = "0.0.0.0:" + findEphemeralPort();
    String jt2HttpAddress = "0.0.0.0:" + findEphemeralPort();
    String jt1HttpRedirectAddress = jt1HttpAddress.replace("0.0.0.0", "localhost");
    String jt2HttpRedirectAddress = jt2HttpAddress.replace("0.0.0.0", "localhost");
    
    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_JOBTRACKER_RPC_ADDRESS_KEY, LOGICAL_NAME, jt1Id), jt1Address);
    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_JOBTRACKER_RPC_ADDRESS_KEY, LOGICAL_NAME, jt2Id), jt2Address);
    
    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_HA_JOBTRACKER_RPC_ADDRESS_KEY, LOGICAL_NAME, jt1Id), jt1HaAddress);
    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_HA_JOBTRACKER_RPC_ADDRESS_KEY, LOGICAL_NAME, jt2Id), jt2HaAddress);

    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_JOBTRACKER_HTTP_ADDRESS_KEY, LOGICAL_NAME, jt1Id), jt1HttpAddress);
    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_JOBTRACKER_HTTP_ADDRESS_KEY, LOGICAL_NAME, jt2Id), jt2HttpAddress);

    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_HA_JOBTRACKER_HTTP_REDIRECT_ADDRESS_KEY, LOGICAL_NAME, jt1Id), jt1HttpRedirectAddress);
    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_HA_JOBTRACKER_HTTP_REDIRECT_ADDRESS_KEY, LOGICAL_NAME, jt2Id), jt2HttpRedirectAddress);

    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_HA_JOBTRACKERS_KEY_PREFIX, LOGICAL_NAME), jt1Id + "," + jt2Id);
    conf.set(HAUtil.addKeySuffixes(HAUtil.MR_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX, LOGICAL_NAME), ConfiguredFailoverProxyProvider.class.getName());
    
    conf.set(HAUtil.MR_JOBTRACKER_ADDRESS_KEY, LOGICAL_NAME);
  }
  
  // Wait until at least one JT is active
  public void waitActive() throws IOException {
    while (true) {
      for (JobTrackerHADaemon jtHaDaemon : jtHaDaemonList) {
        JobTracker jt = jtHaDaemon.getJobTracker();
        if (jt != null) {
          if (jt.getClusterStatus().getJobTrackerStatus() == JobTrackerStatus.RUNNING) {
            return;
          }
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {}
    }
  }
  
  public void startTaskTracker(int idx, int numDir) throws IOException {
    TaskTrackerRunner taskTracker;
    taskTracker = new TaskTrackerRunner(idx, numDir, null, new JobConf(conf));
    
    addTaskTracker(taskTracker);
  }
  
  void addTaskTracker(TaskTrackerRunner taskTracker) {
    Thread taskTrackerThread = new Thread(taskTracker);
    taskTrackerList.add(taskTracker);
    taskTrackerThreadList.add(taskTrackerThread);
    taskTrackerThread.start();
  }

  private void waitTaskTrackers() {
    for(Iterator<TaskTrackerRunner> itr= taskTrackerList.iterator(); itr.hasNext();) {
      TaskTrackerRunner runner = itr.next();
      while (!runner.isDead && (!runner.isInitialized || !runner.tt.isIdle())) {
        if (!runner.isInitialized) {
          LOG.info("Waiting for task tracker to start.");
        } else {
          LOG.info("Waiting for task tracker " + runner.tt.getName() +
                   " to be idle.");
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
  }
  
  public void shutdownJobTracker(int index) {
    JobTrackerHADaemon jtHaDaemon = jtHaDaemonList.get(index);
    try {
      jtHaDaemon.stop();
    } catch (IOException ex) {
      LOG.error("Problem shutting down jobtracker HA daemon", ex);
    }
    jtHaDaemon.join();
  }
  
  public void shutdown() {
    waitTaskTrackers();
    for (int idx = 0; idx < taskTrackerList.size(); idx++) {
      TaskTrackerRunner taskTracker = taskTrackerList.get(idx);
      Thread taskTrackerThread = taskTrackerThreadList.get(idx);
      taskTracker.shutdown();
      taskTrackerThread.interrupt();
      try {
        taskTrackerThread.join();
      } catch (InterruptedException ex) {
        LOG.error("Problem shutting down task tracker", ex);
      }
    }
    for (JobTrackerHADaemon jtHaDaemon : jtHaDaemonList) {
      try {
        jtHaDaemon.stop();
      } catch (IOException ex) {
        LOG.error("Problem shutting down jobtracker HA daemon", ex);
      }
      jtHaDaemon.join();
    }
  }

  static class TaskTrackerRunner implements Runnable {
    volatile TaskTracker tt;
    int trackerId;
    // the localDirs for this taskTracker
    String[] localDirs;
    volatile boolean isInitialized = false;
    volatile boolean isDead = false;
    volatile boolean exited = false;
    int numDir;

    public TaskTrackerRunner(int trackerId, int numDir, String hostname, 
                                    JobConf cfg) 
    throws IOException {
      this.trackerId = trackerId;
      this.numDir = numDir;
      localDirs = new String[numDir];
      final JobConf conf = cfg;

      if (hostname != null) {
        conf.set("slave.host.name", hostname);
      }
      conf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
      conf.set("mapred.task.tracker.report.address", 
                "127.0.0.1:0");
      File localDirBase = 
        new File(conf.get("mapred.local.dir")).getAbsoluteFile();
      localDirBase.mkdirs();
      StringBuffer localPath = new StringBuffer();
      for(int i=0; i < numDir; ++i) {
        File ttDir = new File(localDirBase, 
                              Integer.toString(trackerId) + "_" + i);
        if (!ttDir.mkdirs()) {
          if (!ttDir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + ttDir);
          }
        }
        localDirs[i] = ttDir.toString();
        if (i != 0) {
          localPath.append(",");
        }
        localPath.append(localDirs[i]);
      }
      conf.set("mapred.local.dir", localPath.toString());
      try {
        tt = createTaskTracker(conf);

        isInitialized = true;
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        e.printStackTrace();
      }
    }
     
    /**
     * Creates a default {@link TaskTracker} using the conf passed. 
     */
    TaskTracker createTaskTracker(JobConf conf)
        throws IOException, InterruptedException {
      return new TaskTracker(conf);
    }
    
    /**
     * Create and run the task tracker.
     */
    public void run() {
      try {
        if (tt != null) {
          tt.run();
        }
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        e.printStackTrace();
      }
      exited = true;
    }
 
    /**
     * Get the local dir for this TaskTracker.
     * This is there so that we do not break
     * previous tests. 
     * @return the absolute pathname
     */
    public String getLocalDir() {
      return localDirs[0];
    }
       
    public String[] getLocalDirs(){
      return localDirs;
    } 
    
    public TaskTracker getTaskTracker() {
      return tt;
    }
    
    /**
     * Shut down the server and wait for it to finish.
     */
    public void shutdown() {
      if (tt != null) {
        try {
          tt.shutdown();
        } catch (Throwable e) {
          e.printStackTrace();

        }
      }
    }
  }

}

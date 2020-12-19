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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;
import org.apache.hadoop.yarn.service.CompositeService;

/******************************************************************
 * {@link JobHistoryServer} is responsible for servicing all job history
 * related requests from client.
 *
 *****************************************************************/
public class JobHistoryServer extends CompositeService {

  /**
   * Priority of the JobHistoryServer shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  public static final long historyServerTimeStamp = System.currentTimeMillis();

  private static final Log LOG = LogFactory.getLog(JobHistoryServer.class);
  private HistoryContext historyContext;
  private HistoryClientService clientService;
  private JobHistory jobHistoryService;
  private JHSDelegationTokenSecretManager jhsDTSecretManager;
  private AggregatedLogDeletionService aggLogDelService;

  public JobHistoryServer() {
    super(JobHistoryServer.class.getName());
  }

  @Override
  public synchronized void init(Configuration conf) {
    Configuration config = new YarnConfiguration(conf);

    config.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    try {
      doSecureLogin(conf);
    } catch(IOException ie) {
      throw new YarnException("History Server Failed to login", ie);
    }
    jobHistoryService = new JobHistory();
    historyContext = (HistoryContext)jobHistoryService;
    this.jhsDTSecretManager = createJHSSecretManager(conf);
    clientService = new HistoryClientService(historyContext, 
        this.jhsDTSecretManager);
    aggLogDelService = new AggregatedLogDeletionService();
    addService(jobHistoryService);
    addService(clientService);
    addService(aggLogDelService);
    super.init(config);
  }

  protected JHSDelegationTokenSecretManager createJHSSecretManager(
      Configuration conf) {
    long secretKeyInterval = 
        conf.getLong(MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_KEY, 
                     MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
      long tokenMaxLifetime =
        conf.getLong(MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                     MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
      long tokenRenewInterval =
        conf.getLong(MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 
                     MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
      
    return new JHSDelegationTokenSecretManager(secretKeyInterval, 
        tokenMaxLifetime, tokenRenewInterval, 3600000);
  }
  
  protected void doSecureLogin(Configuration conf) throws IOException {
    SecurityUtil.login(conf, JHAdminConfig.MR_HISTORY_KEYTAB,
        JHAdminConfig.MR_HISTORY_PRINCIPAL);
  }

  @Override
  public void start() {
    DefaultMetricsSystem.initialize("JobHistoryServer");
    JvmMetrics.initSingleton("JobHistoryServer", null);
    try {
      jhsDTSecretManager.startThreads();
    } catch(IOException io) {
      LOG.error("Error while starting the Secret Manager threads", io);
      throw new RuntimeException(io);
    }
    super.start();
  }
  
  @Override
  public void stop() {
    jhsDTSecretManager.stopThreads();
    DefaultMetricsSystem.shutdown();
    super.stop();
  }

  @Private
  public HistoryClientService getClientService() {
    return this.clientService;
  }

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(JobHistoryServer.class, args, LOG);
    try {
      JobHistoryServer jobHistoryServer = new JobHistoryServer();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(jobHistoryServer),
          SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      jobHistoryServer.init(conf);
      jobHistoryServer.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting JobHistoryServer", t);
      System.exit(-1);
    }
  }
}

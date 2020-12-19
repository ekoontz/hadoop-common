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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNMWebServer {

  private static final File testRootDir = new File("target",
      TestNMWebServer.class.getSimpleName());
  private static File testLogDir = new File("target",
      TestNMWebServer.class.getSimpleName() + "LogDir");

  @Before
  public void setup() {
    testRootDir.mkdirs();
    testLogDir.mkdir(); 
  }

  @After
  public void tearDown() {
    FileUtil.fullyDelete(testRootDir);
    FileUtil.fullyDelete(testLogDir);
  }
  
  private String startNMWebAppServer(String webAddr) {
    Context nmContext = new NodeManager.NMContext(null);
    ResourceView resourceView = new ResourceView() {
      @Override
      public long getVmemAllocatedForContainers() {
        return 0;
      }
      @Override
      public long getPmemAllocatedForContainers() {
        return 0;
      }
    };
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, testRootDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, testLogDir.getAbsolutePath());
    NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
    healthChecker.init(conf);
    LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
    conf.set(YarnConfiguration.NM_WEBAPP_ADDRESS, webAddr);
    WebServer server = new WebServer(nmContext, resourceView,
        new ApplicationACLsManager(conf), dirsHandler);
    server.init(conf);
    server.start();
    String webAppAddr = conf.get(YarnConfiguration.NM_WEBAPP_ADDRESS);
    return StringUtils.split(webAppAddr, ':')[1];
  }
  
  @Test
  public void testNMWebAppWithOutPort() throws IOException {
    String port = startNMWebAppServer("0.0.0.0");
    Assert.assertTrue("Port is not updated", Integer.parseInt(port) > 0);
  }
  
  @Test
  public void testNMWebAppWithEphemeralPort() throws IOException {
    String port = startNMWebAppServer("0.0.0.0:0"); 
    Assert.assertTrue("Port is not updated", Integer.parseInt(port) > 0);
  }

  @Test
  public void testNMWebApp() throws IOException {
    Context nmContext = new NodeManager.NMContext(null);
    ResourceView resourceView = new ResourceView() {
      @Override
      public long getVmemAllocatedForContainers() {
        return 0;
      }
      @Override
      public long getPmemAllocatedForContainers() {
        return 0;
      }
    };
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, testRootDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, testLogDir.getAbsolutePath());
    NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
    healthChecker.init(conf);
    LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();

    WebServer server = new WebServer(nmContext, resourceView,
        new ApplicationACLsManager(conf), dirsHandler);
    server.init(conf);
    server.start();

    // Add an application and the corresponding containers
    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(conf);
    Dispatcher dispatcher = new AsyncDispatcher();
    String user = "nobody";
    long clusterTimeStamp = 1234;
    ApplicationId appId =
        BuilderUtils.newApplicationId(recordFactory, clusterTimeStamp, 1);
    Application app = mock(Application.class);
    when(app.getUser()).thenReturn(user);
    when(app.getAppId()).thenReturn(appId);
    nmContext.getApplications().put(appId, app);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    ContainerId container1 =
        BuilderUtils.newContainerId(recordFactory, appId, appAttemptId, 0);
    ContainerId container2 =
        BuilderUtils.newContainerId(recordFactory, appId, appAttemptId, 1);
    NodeManagerMetrics metrics = mock(NodeManagerMetrics.class);
    for (ContainerId containerId : new ContainerId[] { container1,
        container2}) {
      // TODO: Use builder utils
      ContainerLaunchContext launchContext =
          recordFactory.newRecordInstance(ContainerLaunchContext.class);
      launchContext.setContainerId(containerId);
      launchContext.setUser(user);
      Container container =
          new ContainerImpl(conf, dispatcher, launchContext, null, metrics) {
        @Override
        public ContainerState getContainerState() {
          return ContainerState.RUNNING;
        };
      };
      nmContext.getContainers().put(containerId, container);
      //TODO: Gross hack. Fix in code.
      ApplicationId applicationId = 
          containerId.getApplicationAttemptId().getApplicationId();
      nmContext.getApplications().get(applicationId).getContainers()
          .put(containerId, container);
      writeContainerLogs(nmContext, containerId, dirsHandler);

    }
    // TODO: Pull logs and test contents.
//    Thread.sleep(1000000);
  }

  private void writeContainerLogs(Context nmContext,
      ContainerId containerId, LocalDirsHandlerService dirsHandler)
        throws IOException {
    // ContainerLogDir should be created
    File containerLogDir =
        ContainerLogsPage.ContainersLogsBlock.getContainerLogDirs(containerId,
            dirsHandler).get(0);
    containerLogDir.mkdirs();
    for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
      Writer writer = new FileWriter(new File(containerLogDir, fileType));
      writer.write(ConverterUtils.toString(containerId) + "\n Hello "
          + fileType + "!");
      writer.close();
    }
  }
}

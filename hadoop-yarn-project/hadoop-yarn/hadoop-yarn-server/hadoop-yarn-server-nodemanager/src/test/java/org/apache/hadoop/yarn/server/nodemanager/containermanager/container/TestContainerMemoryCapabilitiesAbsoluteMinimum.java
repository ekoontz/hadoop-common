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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import junit.framework.Assert;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStartMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.InetAddress;

public class TestContainerMemoryCapabilitiesAbsoluteMinimum {

  @SuppressWarnings("unchecked")
  private void testMinimumMemory(int memoryMbSet, long memoryBytesExpected)
      throws Exception {
    ContainerImpl.LaunchTransition lt = new ContainerImpl.LaunchTransition();

    ArgumentCaptor<ContainerStartMonitoringEvent> captor =
        ArgumentCaptor.forClass(ContainerStartMonitoringEvent.class);

    Resource resource = Resource.newInstance(memoryMbSet, 1);

    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptID =
        ApplicationAttemptId.newInstance(appId, 0);
    ContainerId containerId = ContainerId.newInstance(appAttemptID, 1);

    Dispatcher mockDispatcher = Mockito.mock(Dispatcher.class);
    EventHandler mockEventHandler = Mockito.mock(EventHandler.class);
    Mockito.when(mockDispatcher.getEventHandler()).thenReturn(mockEventHandler);
    NodeManagerMetrics mockMetrics = Mockito.mock(NodeManagerMetrics.class);
    ContainerTokenIdentifier containerToken = BuilderUtils
        .newContainerTokenIdentifier(BuilderUtils.newContainerToken(
            containerId, InetAddress.getByName("localhost")
            .getCanonicalHostName(), 1234, "u", resource,
            System.currentTimeMillis() + 10000, 123, "password".getBytes(),
            System.currentTimeMillis()));
    ContainerImpl container = new ContainerImpl(new YarnConfiguration(),
        mockDispatcher, null, null, null, mockMetrics, containerToken);
    lt.transition(container, null);
    Mockito.verify(mockEventHandler, Mockito.times(1)).handle(captor.capture());
    Assert.assertEquals(memoryBytesExpected, captor.getValue().getPmemLimit());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAbsoluteMinimumMemory() throws Exception {
    testMinimumMemory(0, 5L * 1024 * 1024);
  }

  @Test
  public void testRegularMemory() throws Exception {
    testMinimumMemory(1024, 1024L * 1024 * 1024);
  }

}

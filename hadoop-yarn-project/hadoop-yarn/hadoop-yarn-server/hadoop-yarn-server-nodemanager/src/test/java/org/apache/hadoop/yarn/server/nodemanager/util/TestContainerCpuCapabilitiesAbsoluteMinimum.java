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
package org.apache.hadoop.yarn.server.nodemanager.util;

import junit.framework.Assert;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Test;

import java.io.IOException;

public class TestContainerCpuCapabilitiesAbsoluteMinimum {
  private static int valueSet;

  private static class TestCgroupsLCEResourcesHandler extends CgroupsLCEResourcesHandler {

    @Override
    void createCgroup(String controller, String groupName) throws IOException {
    }

    @Override
    void updateCgroup(String controller, String groupName, String param,
        String value) throws IOException {
      valueSet = Integer.parseInt(value);
    }
  }

  private void testCpu(int cpuSet, int sharesExpected) throws Exception {
    Resource resource = Resource.newInstance(1, cpuSet);

    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptID =
        ApplicationAttemptId.newInstance(appId, 0);
    ContainerId containerId = ContainerId.newInstance(appAttemptID, 1);

    CgroupsLCEResourcesHandler handler = new TestCgroupsLCEResourcesHandler();
    handler.setupLimits(containerId, resource);
    Assert.assertEquals(sharesExpected, valueSet);
  }

  @Test
  public void testAbsoluteMinimumCpu() throws Exception {
    testCpu(0, 10);
  }

  @Test
  public void testRegularCpu() throws Exception {
    testCpu(1, 1024);
  }

}

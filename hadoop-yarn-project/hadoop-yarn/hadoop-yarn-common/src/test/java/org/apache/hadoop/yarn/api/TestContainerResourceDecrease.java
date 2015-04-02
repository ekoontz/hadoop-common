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

package org.apache.hadoop.yarn.api;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerResourceDecreasePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerResourceDecreaseProto;
import org.junit.Test;

public class TestContainerResourceDecrease {
  @Test
  public void testResourceDecreaseContext() {
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    Resource resource = Resource.newInstance(1023, 3);
    ContainerResourceDecrease ctx = ContainerResourceDecrease.newInstance(
        containerId, resource);

    // get proto and recover to ctx
    ContainerResourceDecreaseProto proto = 
        ((ContainerResourceDecreasePBImpl) ctx).getProto();
    ctx = new ContainerResourceDecreasePBImpl(proto);

    // check values
    Assert.assertEquals(ctx.getCapability(), resource);
    Assert.assertEquals(ctx.getContainerId(), containerId);
  }
  
  @Test
  public void testResourceDecreaseContextWithNull() {
    ContainerResourceDecrease ctx = ContainerResourceDecrease.newInstance(null,
        null);
    
    // get proto and recover to ctx;
    ContainerResourceDecreaseProto proto = 
        ((ContainerResourceDecreasePBImpl) ctx).getProto();
    ctx = new ContainerResourceDecreasePBImpl(proto);

    // check values
    Assert.assertNull(ctx.getCapability());
    Assert.assertNull(ctx.getContainerId());
  }
}

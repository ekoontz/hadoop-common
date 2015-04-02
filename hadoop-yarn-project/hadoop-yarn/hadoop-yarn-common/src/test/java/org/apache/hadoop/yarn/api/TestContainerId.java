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
import org.junit.Test;

public class TestContainerId {

  @Test
  public void testContainerId() {
    ContainerId c1 = newContainerId(1, 1, 10l, 1);
    ContainerId c2 = newContainerId(1, 1, 10l, 2);
    ContainerId c3 = newContainerId(1, 1, 10l, 1);
    ContainerId c4 = newContainerId(1, 3, 10l, 1);
    ContainerId c5 = newContainerId(1, 3, 8l, 1);

    Assert.assertTrue(c1.equals(c3));
    Assert.assertFalse(c1.equals(c2));
    Assert.assertFalse(c1.equals(c4));
    Assert.assertFalse(c1.equals(c5));

    Assert.assertTrue(c1.compareTo(c3) == 0);
    Assert.assertTrue(c1.compareTo(c2) < 0);
    Assert.assertTrue(c1.compareTo(c4) < 0);
    Assert.assertTrue(c1.compareTo(c5) > 0);

    Assert.assertTrue(c1.hashCode() == c3.hashCode());
    Assert.assertFalse(c1.hashCode() == c2.hashCode());
    Assert.assertFalse(c1.hashCode() == c4.hashCode());
    Assert.assertFalse(c1.hashCode() == c5.hashCode());
    
    long ts = System.currentTimeMillis();
    ContainerId c6 = newContainerId(36473, 4365472, ts, 25645811);
    Assert.assertEquals("container_10_0001_01_000001", c1.toString());
    Assert.assertEquals("container_" + ts + "_36473_4365472_25645811",
        c6.toString());
  }

  public static ContainerId newContainerId(int appId, int appAttemptId,
      long timestamp, int containerId) {
    ApplicationId applicationId = ApplicationId.newInstance(timestamp, appId);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, appAttemptId);
    return ContainerId.newInstance(applicationAttemptId, containerId);
  }
}

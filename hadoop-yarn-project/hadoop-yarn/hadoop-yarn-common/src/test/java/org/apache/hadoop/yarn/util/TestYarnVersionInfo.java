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

package org.apache.hadoop.yarn.util;

import junit.framework.TestCase;
import java.io.IOException;

import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.junit.Test;

/**
 * A JUnit test to test {@link YarnVersionInfo}
 */
public class TestYarnVersionInfo extends TestCase {
  
  /**
   * Test the yarn version info routines.
   * @throws IOException
   */
  @Test
  public void testVersionInfoGenerated() throws IOException {

    // can't easily know what the correct values are going to be so just
    // make sure they aren't Unknown
    assertTrue("getVersion returned Unknown", !YarnVersionInfo.getVersion().equals("Unknown"));
    assertTrue("getUser returned Unknown", !YarnVersionInfo.getUser().equals("Unknown"));
    assertTrue("getUrl returned Unknown", !YarnVersionInfo.getUrl().equals("Unknown"));
    assertTrue("getSrcChecksum returned Unknown", !YarnVersionInfo.getSrcChecksum().equals("Unknown"));

    // these could be Unknown if the VersionInfo generated from code not in svn or git
    // so just check that they return something
    assertNotNull("getRevision returned null", YarnVersionInfo.getRevision());
    assertNotNull("getBranch returned null", YarnVersionInfo.getBranch());

    assertTrue("getBuildVersion check doesn't contain: source checksum",
               YarnVersionInfo.getBuildVersion().contains("source checksum"));

  }
}

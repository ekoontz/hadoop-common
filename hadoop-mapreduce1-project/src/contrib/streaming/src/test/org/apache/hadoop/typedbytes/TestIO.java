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

package org.apache.hadoop.typedbytes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

public class TestIO extends TestCase {

  private File tmpfile;

  protected void setUp() throws Exception {
    File testDir = new File(System.getProperty("test.build.data", "/tmp"));
    testDir.mkdir();
    this.tmpfile = new File(testDir, "typedbytes.bin");
  }

  protected void tearDown() throws Exception {
    tmpfile.delete();
  }

  public void testCustomTypesIO() throws IOException {
    byte[] rawBytes = new byte[] { 100, 0, 0, 0, 3, 1, 2, 3 };
    
    FileOutputStream ostream = new FileOutputStream(tmpfile);
    DataOutputStream dostream = new DataOutputStream(ostream);
    TypedBytesOutput out = new TypedBytesOutput(dostream);
    out.writeRaw(rawBytes);
    dostream.close();
    ostream.close();

    FileInputStream istream = new FileInputStream(tmpfile);
    DataInputStream distream = new DataInputStream(istream);
    TypedBytesInput in = new TypedBytesInput(distream);
    assertTrue(Arrays.equals(rawBytes, in.readRaw()));
    distream.close();
    istream.close();
  }
}

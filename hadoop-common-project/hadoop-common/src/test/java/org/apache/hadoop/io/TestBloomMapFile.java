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

package org.apache.hadoop.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

public class TestBloomMapFile extends TestCase {
  private static Configuration conf = new Configuration();
  
  public void testMembershipTest() throws Exception {
    // write the file
    Path dirName = new Path(System.getProperty("test.build.data",".") +
        getName() + ".bloommapfile"); 
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);
    conf.setInt("io.mapfile.bloom.size", 2048);
    BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, fs,
      qualifiedDirName.toString(), IntWritable.class, Text.class);
    IntWritable key = new IntWritable();
    Text value = new Text();
    for (int i = 0; i < 2000; i += 2) {
      key.set(i);
      value.set("00" + i);
      writer.append(key, value);
    }
    writer.close();
    
    BloomMapFile.Reader reader = new BloomMapFile.Reader(fs,
        qualifiedDirName.toString(), conf);
    // check false positives rate
    int falsePos = 0;
    int falseNeg = 0;
    for (int i = 0; i < 2000; i++) {
      key.set(i);
      boolean exists = reader.probablyHasKey(key);
      if (i % 2 == 0) {
        if (!exists) falseNeg++;
      } else {
        if (exists) falsePos++;
      }
    }
    reader.close();
    fs.delete(qualifiedDirName, true);
    System.out.println("False negatives: " + falseNeg);
    assertEquals(0, falseNeg);
    System.out.println("False positives: " + falsePos);
    assertTrue(falsePos < 2);
  }

  private void checkMembershipVaryingSizedKeys(String name, List<Text> keys) throws Exception {
    Path dirName = new Path(System.getProperty("test.build.data",".") +
        name + ".bloommapfile"); 
    FileSystem fs = FileSystem.getLocal(conf);
    Path qualifiedDirName = fs.makeQualified(dirName);
    BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, fs,
      qualifiedDirName.toString(), Text.class, NullWritable.class);
    for (Text key : keys) {
      writer.append(key, NullWritable.get());
    }
    writer.close();

    // will check for membership in the opposite order of how keys were inserted
    BloomMapFile.Reader reader = new BloomMapFile.Reader(fs,
        qualifiedDirName.toString(), conf);
    Collections.reverse(keys);
    for (Text key : keys) {
      assertTrue("False negative for existing key " + key, reader.probablyHasKey(key));
    }
    reader.close();
    fs.delete(qualifiedDirName, true);
  }

  public void testMembershipVaryingSizedKeysTest1() throws Exception {
    ArrayList<Text> list = new ArrayList<Text>();
    list.add(new Text("A"));
    list.add(new Text("BB"));
    checkMembershipVaryingSizedKeys(getName(), list);
  }

  public void testMembershipVaryingSizedKeysTest2() throws Exception {
    ArrayList<Text> list = new ArrayList<Text>();
    list.add(new Text("AA"));
    list.add(new Text("B"));
    checkMembershipVaryingSizedKeys(getName(), list);
  }

}

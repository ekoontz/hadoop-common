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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TestFileInputFormat extends TestCase {

  public void testAddInputPath() throws IOException {
    final Configuration conf = new Configuration();
    conf.set("fs.default.name", "s3://abc:xyz@hostname/");
    final Job j = new Job(conf);

    //setup default fs
    final FileSystem defaultfs = FileSystem.get(conf);
    System.out.println("defaultfs.getUri() = " + defaultfs.getUri());

    {
      //test addInputPath
      final Path original = new Path("file:/foo");
      System.out.println("original = " + original);
      FileInputFormat.addInputPath(j, original);
      final Path[] results = FileInputFormat.getInputPaths(j);
      System.out.println("results = " + Arrays.asList(results));
      assertEquals(1, results.length);
      assertEquals(original, results[0]);
    }

    {
      //test setInputPaths
      final Path original = new Path("file:/bar");
      System.out.println("original = " + original);
      FileInputFormat.setInputPaths(j, original);
      final Path[] results = FileInputFormat.getInputPaths(j);
      System.out.println("results = " + Arrays.asList(results));
      assertEquals(1, results.length);
      assertEquals(original, results[0]);
    }
  }
  
  /**
   * Test when the input file's length is 0.
   */
  public void testForEmptyFile() throws Exception {
      Configuration conf = new Configuration();
      FileSystem fileSys = FileSystem.get(conf);
      Path file = new Path("test" + "/file");
      FSDataOutputStream out = fileSys.create(file, true,
              conf.getInt("io.file.buffer.size", 4096), (short) 1, (long) 1024);
      out.write(new byte[0]);
      out.close();

      // split it using a File input format
      DummyInputFormat inFormat = new DummyInputFormat();
      Job job = Job.getInstance(conf);
      FileInputFormat.setInputPaths(job, "test");
      List<InputSplit> splits = inFormat.getSplits(job);
      assertEquals(1, splits.size());
      FileSplit fileSplit = (FileSplit) splits.get(0);
      assertEquals(0, fileSplit.getLocations().length);
      assertEquals(file.getName(), fileSplit.getPath().getName());
      assertEquals(0, fileSplit.getStart());
      assertEquals(0, fileSplit.getLength());

      fileSys.delete(file.getParent(), true);
  }

  /** Dummy class to extend FileInputFormat*/
  private class DummyInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text,Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException {
      return null;
    }
  }

}

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
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.MapReduceTestUtil.DataCopyMapper;
import org.apache.hadoop.mapreduce.MapReduceTestUtil.DataCopyReducer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests context api and {@link StatusReporter#getProgress()} via 
 * {@link TaskAttemptContext#getProgress()} API . 
 */
public class TestTaskContext extends HadoopTestCase {
  private static final Path rootTempDir =
    new Path(System.getProperty("test.build.data", "/tmp"));
  private static final Path testRootTempDir = 
    new Path(rootTempDir, "TestTaskContext");
  
  private static FileSystem fs = null;

  @BeforeClass
  public static void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    fs.delete(testRootTempDir, true);
    fs.mkdirs(testRootTempDir);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    fs.delete(testRootTempDir, true);
  }

  public TestTaskContext() throws IOException {
    super(HadoopTestCase.CLUSTER_MR , HadoopTestCase.LOCAL_FS, 1, 1);
  }

  static String myStatus = "my status";
  static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void setup(Context context) throws IOException {
      context.setStatus(myStatus);
      assertEquals(myStatus, context.getStatus());
    }
  }

  /**
   * Tests context.setStatus method.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public void testContextStatus()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path test = new Path(testRootTempDir, "testContextStatus");
    
    // test with 1 map and 0 reducers
    // test with custom task status
    int numMaps = 1;
    Job job = MapReduceTestUtil.createJob(createJobConf(), new Path("in"),
        new Path("out"), numMaps, 0);
    job.setMapperClass(MyMapper.class);
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    
    // test with 1 map and 1 reducer
    // test with default task status
    int numReduces = 1;
    job = MapReduceTestUtil.createJob(createJobConf(), 
            new Path(test, "in"), new Path(test, "out"), numMaps, numReduces);
    job.setMapperClass(DataCopyMapper.class);
    job.setReducerClass(DataCopyReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    // run the job and wait for completion
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
  }
}

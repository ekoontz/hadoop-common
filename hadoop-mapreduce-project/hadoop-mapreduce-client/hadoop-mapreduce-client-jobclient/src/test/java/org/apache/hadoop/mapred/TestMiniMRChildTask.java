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
package org.apache.hadoop.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.*;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;

/**
 * Class to test mapred task's 
 *   - temp directory
 *   - child env
 */
public class TestMiniMRChildTask {
  private static final Log LOG =
    LogFactory.getLog(TestMiniMRChildTask.class.getName());

  private final static String OLD_CONFIGS = "test.old.configs";
  private final static String TASK_OPTS_VAL = "-Xmx200m";
  private final static String MAP_OPTS_VAL = "-Xmx200m";
  private final static String REDUCE_OPTS_VAL = "-Xmx300m";

  private static MiniMRYarnCluster mr;
  private static MiniDFSCluster dfs;
  private static FileSystem fileSys;
  
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static Path TEST_ROOT_DIR = new Path("target",
      TestMiniMRChildTask.class.getName() + "-tmpDir").makeQualified(localFs);
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");

  /**
   * Map class which checks whether temp directory exists
   * and check the value of java.io.tmpdir
   * Creates a tempfile and checks whether that is created in 
   * temp directory specified.
   */
  public static class MapClass extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, IntWritable> {
      Path tmpDir;
      public void map (LongWritable key, Text value, 
          OutputCollector<Text, IntWritable> output, 
          Reporter reporter) throws IOException {
        if (localFs.exists(tmpDir)) {
        } else {
          fail("Temp directory " + tmpDir +" doesnt exist.");
        }
        File tmpFile = File.createTempFile("test", ".tmp");
      }
      public void configure(JobConf job) {
        tmpDir = new Path(System.getProperty("java.io.tmpdir"));
        try {
          localFs = FileSystem.getLocal(job);
        } catch (IOException ioe) {
          ioe.printStackTrace();
          fail("IOException in getting localFS");
        }
      }
  }

  // configure a job
  private void configure(JobConf conf, Path inDir, Path outDir, String input,
                         Class<? extends Mapper> map, 
                         Class<? extends Reducer> reduce) 
  throws IOException {
    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    // configure the mapred Job which creates a tempfile in map.
    conf.setJobName("testmap");
    conf.setMapperClass(map);
    conf.setReducerClass(reduce);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                      "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);
  }

  /**
   * Launch tests 
   * @param conf Configuration of the mapreduce job.
   * @param inDir input path
   * @param outDir output path
   * @param input Input text
   * @throws IOException
   */
  public void launchTest(JobConf conf,
                         Path inDir,
                         Path outDir,
                         String input)
  throws IOException, InterruptedException, ClassNotFoundException {
    configure(conf, inDir, outDir, input, 
              MapClass.class, IdentityReducer.class);

    FileSystem outFs = outDir.getFileSystem(conf);
    
    // Launch job with default option for temp dir. 
    // i.e. temp dir is ./tmp 
    Job job = new Job(conf);
    job.addFileToClassPath(APP_JAR);
    job.setJarByClass(TestMiniMRChildTask.class);
    job.setMaxMapAttempts(1); // speed up failures
    job.waitForCompletion(true);
    boolean succeeded = job.waitForCompletion(true);
    assertTrue(succeeded);
    outFs.delete(outDir, true);
  }

  private static void checkEnv(String envName, String expValue, String mode) {
    String envValue = System.getenv(envName).trim();
    if ("append".equals(mode)) {
      if (envValue == null || !envValue.contains(":")) {
        throw new RuntimeException("Missing env variable");
      } else {
        String parts[] = envValue.split(":");
        // check if the value is appended
        if (!parts[parts.length - 1].equals(expValue)) {
          throw new RuntimeException("Wrong env variable in append mode");
        }
      }
    } else {
      if (envValue == null || !envValue.equals(expValue)) {
        throw new RuntimeException("Wrong env variable in noappend mode");
      }
    }
  }

  // Mappers that simply checks if the desired user env are present or not
  static class EnvCheckMapper extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    
    public void configure(JobConf job) {
      boolean oldConfigs = job.getBoolean(OLD_CONFIGS, false);
      if (oldConfigs) {
        String javaOpts = job.get(JobConf.MAPRED_TASK_JAVA_OPTS);
        assertNotNull(JobConf.MAPRED_TASK_JAVA_OPTS + " is null!", 
                      javaOpts);
        assertEquals(JobConf.MAPRED_TASK_JAVA_OPTS + " has value of: " + 
                     javaOpts, 
                     javaOpts, TASK_OPTS_VAL);
      } else {
        String mapJavaOpts = job.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS);
        assertNotNull(JobConf.MAPRED_MAP_TASK_JAVA_OPTS + " is null!", 
                      mapJavaOpts);
        assertEquals(JobConf.MAPRED_MAP_TASK_JAVA_OPTS + " has value of: " + 
                     mapJavaOpts, 
                     mapJavaOpts, MAP_OPTS_VAL);
      }

      String path = job.get("path");
      
      // check if the pwd is there in LD_LIBRARY_PATH
      String pwd = System.getenv("PWD");
      
      assertTrue("LD doesnt contain pwd", 
                 System.getenv("LD_LIBRARY_PATH").contains(pwd));
      
      // check if X=$X:/abc works for LD_LIBRARY_PATH
      checkEnv("LD_LIBRARY_PATH", "/tmp", "append");
      // check if X=y works for an already existing parameter
      checkEnv("LANG", "en_us_8859_1", "noappend");
      // check if X=/tmp for a new env variable
      checkEnv("MY_PATH", "/tmp", "noappend");
      // check if X=$X:/tmp works for a new env var and results into :/tmp
      checkEnv("NEW_PATH", ":/tmp", "noappend");
      // check if X=$(tt's X var):/tmp for an old env variable inherited from 
      // the tt
      checkEnv("PATH",  path + ":/tmp", "noappend");

      String jobLocalDir = job.get(MRJobConfig.JOB_LOCAL_DIR);
      assertNotNull(MRJobConfig.JOB_LOCAL_DIR + " is null",
                    jobLocalDir);
    }

    public void map(WritableComparable key, Writable value,
                    OutputCollector<WritableComparable, Writable> out, 
                    Reporter reporter)
        throws IOException {
    }
  }

  static class EnvCheckReducer extends MapReduceBase 
  implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {
    
    @Override
    public void configure(JobConf job) {
      boolean oldConfigs = job.getBoolean(OLD_CONFIGS, false);
      if (oldConfigs) {
        String javaOpts = job.get(JobConf.MAPRED_TASK_JAVA_OPTS);
        assertNotNull(JobConf.MAPRED_TASK_JAVA_OPTS + " is null!", 
                      javaOpts);
        assertEquals(JobConf.MAPRED_TASK_JAVA_OPTS + " has value of: " + 
                     javaOpts, 
                     javaOpts, TASK_OPTS_VAL);
      } else {
        String reduceJavaOpts = job.get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS);
        assertNotNull(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS + " is null!", 
                      reduceJavaOpts);
        assertEquals(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS + " has value of: " + 
                     reduceJavaOpts, 
                     reduceJavaOpts, REDUCE_OPTS_VAL);
      }

      String path = job.get("path");
      
      // check if the pwd is there in LD_LIBRARY_PATH
      String pwd = System.getenv("PWD");
      
      assertTrue("LD doesnt contain pwd", 
                 System.getenv("LD_LIBRARY_PATH").contains(pwd));
      
      // check if X=$X:/abc works for LD_LIBRARY_PATH
      checkEnv("LD_LIBRARY_PATH", "/tmp", "append");
      // check if X=y works for an already existing parameter
      checkEnv("LANG", "en_us_8859_1", "noappend");
      // check if X=/tmp for a new env variable
      checkEnv("MY_PATH", "/tmp", "noappend");
      // check if X=$X:/tmp works for a new env var and results into :/tmp
      checkEnv("NEW_PATH", ":/tmp", "noappend");
      // check if X=$(tt's X var):/tmp for an old env variable inherited from 
      // the tt
      checkEnv("PATH",  path + ":/tmp", "noappend");

    }

    @Override
    public void reduce(WritableComparable key, Iterator<Writable> values,
                       OutputCollector<WritableComparable, Writable> output, 
                       Reporter reporter)
        throws IOException {
    }
    
  }
  
  @BeforeClass
  public static void setup() throws IOException {
    // create configuration, dfs, file system and mapred cluster 
    dfs = new MiniDFSCluster(conf, 1, true, null);
    fileSys = dfs.getFileSystem();

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mr == null) {
      mr = new MiniMRYarnCluster(TestMiniMRChildTask.class.getName());
      Configuration conf = new Configuration();
      mr.init(conf);
      mr.start();
    }

    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() {
    // close file system and shut down dfs and mapred cluster
    try {
      if (fileSys != null) {
        fileSys.close();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
      if (mr != null) {
        mr.stop();
        mr = null;
      }
    } catch (IOException ioe) {
      LOG.info("IO exception in closing file system)" );
      ioe.printStackTrace();           
    }
  }
  
  /**
   * Tests task's temp directory.
   * 
   * In this test, we give different values to mapreduce.task.tmp.dir
   * both relative and absolute. And check whether the temp directory 
   * is created. We also check whether java.io.tmpdir value is same as 
   * the directory specified. We create a temp file and check if is is 
   * created in the directory specified.
   */
  @Test
  public void testTaskTempDir(){
    try {
      JobConf conf = new JobConf(mr.getConfig());
      
      // intialize input, output directories
      Path inDir = new Path("testing/wc/input");
      Path outDir = new Path("testing/wc/output");
      String input = "The input";
      
      launchTest(conf, inDir, outDir, input);
      
    } catch(Exception e) {
      e.printStackTrace();
      fail("Exception in testing temp dir");
      tearDown();
    }
  }

  /**
   * Test to test if the user set env variables reflect in the child
   * processes. Mainly
   *   - x=y (x can be a already existing env variable or a new variable)
   *   - x=$x:y (replace $x with the current value of x)
   */
  @Test
  public void testTaskEnv(){
    try {
      JobConf conf = new JobConf(mr.getConfig());
      // initialize input, output directories
      Path inDir = new Path("testing/wc/input1");
      Path outDir = new Path("testing/wc/output1");
      FileSystem outFs = outDir.getFileSystem(conf);
      runTestTaskEnv(conf, inDir, outDir, false);
      outFs.delete(outDir, true);
    } catch(Exception e) {
      e.printStackTrace();
      fail("Exception in testing child env");
      tearDown();
    }
  }
  
  /**
   * Test to test if the user set *old* env variables reflect in the child
   * processes. Mainly
   *   - x=y (x can be a already existing env variable or a new variable)
   *   - x=$x:y (replace $x with the current value of x)
   */
  @Test
  public void testTaskOldEnv(){
    try {
      JobConf conf = new JobConf(mr.getConfig());
      // initialize input, output directories
      Path inDir = new Path("testing/wc/input1");
      Path outDir = new Path("testing/wc/output1");
      FileSystem outFs = outDir.getFileSystem(conf);
      runTestTaskEnv(conf, inDir, outDir, true);
      outFs.delete(outDir, true);
    } catch(Exception e) {
      e.printStackTrace();
      fail("Exception in testing child env");
      tearDown();
    }
  }
  
  void runTestTaskEnv(JobConf conf, Path inDir, Path outDir, boolean oldConfigs) 
  throws IOException, InterruptedException, ClassNotFoundException {
    String input = "The input";
    configure(conf, inDir, outDir, input, 
              EnvCheckMapper.class, EnvCheckReducer.class);
    // test 
    //  - new SET of new var (MY_PATH)
    //  - set of old var (LANG)
    //  - append to an old var from modified env (LD_LIBRARY_PATH)
    //  - append to an old var from tt's env (PATH)
    //  - append to a new var (NEW_PATH)
    String mapTaskEnvKey = JobConf.MAPRED_MAP_TASK_ENV;
    String reduceTaskEnvKey = JobConf.MAPRED_MAP_TASK_ENV;
    String mapTaskJavaOptsKey = JobConf.MAPRED_MAP_TASK_JAVA_OPTS;
    String reduceTaskJavaOptsKey = JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS;
    String mapTaskJavaOpts = MAP_OPTS_VAL;
    String reduceTaskJavaOpts = REDUCE_OPTS_VAL;
    conf.setBoolean(OLD_CONFIGS, oldConfigs);
    if (oldConfigs) {
      mapTaskEnvKey = reduceTaskEnvKey = JobConf.MAPRED_TASK_ENV;
      mapTaskJavaOptsKey = reduceTaskJavaOptsKey = JobConf.MAPRED_TASK_JAVA_OPTS;
      mapTaskJavaOpts = reduceTaskJavaOpts = TASK_OPTS_VAL;
    }
    conf.set(mapTaskEnvKey, 
             "MY_PATH=/tmp,LANG=en_us_8859_1,LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp," +
             "PATH=$PATH:/tmp,NEW_PATH=$NEW_PATH:/tmp");
    conf.set(reduceTaskEnvKey, 
             "MY_PATH=/tmp,LANG=en_us_8859_1,LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp," +
             "PATH=$PATH:/tmp,NEW_PATH=$NEW_PATH:/tmp");
    conf.set("path", System.getenv("PATH"));
    conf.set(mapTaskJavaOptsKey, mapTaskJavaOpts);
    conf.set(reduceTaskJavaOptsKey, reduceTaskJavaOpts);

    Job job = new Job(conf);
    job.addFileToClassPath(APP_JAR);
    job.setJarByClass(TestMiniMRChildTask.class);
    job.setMaxMapAttempts(1); // speed up failures
    job.waitForCompletion(true);
    boolean succeeded = job.waitForCompletion(true);
    assertTrue("The environment checker job failed.", succeeded);
  }
  
}

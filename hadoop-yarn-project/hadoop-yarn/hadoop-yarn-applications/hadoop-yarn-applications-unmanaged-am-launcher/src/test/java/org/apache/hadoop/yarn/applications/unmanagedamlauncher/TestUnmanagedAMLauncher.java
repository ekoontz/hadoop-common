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

package org.apache.hadoop.yarn.applications.unmanagedamlauncher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUnmanagedAMLauncher {

  private static final Log LOG = LogFactory
      .getLog(TestUnmanagedAMLauncher.class);

  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    LOG.info("Starting up YARN cluster");
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(
          TestUnmanagedAMLauncher.class.getName(), 1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      URL url = Thread.currentThread().getContextClassLoader()
          .getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException(
            "Could not find 'yarn-site.xml' dummy file in classpath");
      }
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      yarnCluster.getConfig().writeXml(os);
      os.close();
    }
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
  }

  private static String getTestRuntimeClasspath() {
    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    String envClassPath = "";
    String cp = System.getProperty("java.class.path");
    if (cp != null) {
      envClassPath += cp.trim() + File.pathSeparator;
    }
    // yarn-site.xml at this location contains proper config for mini cluster
    ClassLoader thisClassLoader = Thread.currentThread()
      .getContextClassLoader();
    URL url = thisClassLoader.getResource("yarn-site.xml");
    envClassPath += new File(url.getFile()).getParent();
    return envClassPath;
  }

  @Test
  public void testDSShell() throws Exception {
    String classpath = getTestRuntimeClasspath();
    String javaHome = System.getenv("JAVA_HOME");
    if (javaHome == null) {
      LOG.fatal("JAVA_HOME not defined. Test not running.");
      return;
    }
    // start dist-shell with 0 containers because container launch will fail if 
    // there are no dist cache resources.
    String[] args = {
        "--classpath",
        classpath,
        "--queue",
        "default",
        "--cmd",
        javaHome
            + "/bin/java -Xmx512m "
            + "org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster "
            + "--container_memory 128 --num_containers 0 --priority 0 --shell_command ls" };

    LOG.info("Initializing Launcher");
    UnmanagedAMLauncher launcher = new UnmanagedAMLauncher(new Configuration(
        yarnCluster.getConfig()));
    boolean initSuccess = launcher.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running Launcher");
    boolean result = launcher.run();

    LOG.info("Launcher run completed. Result=" + result);
    Assert.assertTrue(result);

  }

}

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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.mapred.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.junit.*;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Tests web UI redirect from standby to active jobtracker.
 */
public class TestHAWebUI {
  
  private static final Log LOG = 
    LogFactory.getLog(TestHAWebUI.class);

  private MiniMRHACluster cluster;
  private JobTrackerHADaemon jt1;
  private JobTrackerHADaemon jt2;
  private JobTrackerHAServiceTarget target1;
  private JobTrackerHAServiceTarget target2;
  private Configuration conf;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(HAUtil.MR_HA_FENCING_METHODS_KEY,
        AlwaysSucceedFencer.class.getName());
    cluster = new MiniMRHACluster(conf);
    cluster.getJobTrackerHaDaemon(0).makeActive();
    cluster.startTaskTracker(0, 1);
    cluster.waitActive();
    
    jt1 = cluster.getJobTrackerHaDaemon(0);
    jt2 = cluster.getJobTrackerHaDaemon(1);
    target1 = new JobTrackerHAServiceTarget(jt1.getConf());
    target2 = new JobTrackerHAServiceTarget(jt2.getConf());
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  @Test(timeout=60000)
  public void testRedirect() throws Exception {

    // both jobtracker addresses should serve up the jobtracker page
    // regardless of state
    checkJobTrackerPage("jt1");
    checkJobTrackerPage("jt2");

    // failover to jt2
    FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    fc.failover(target1, target2, false, false);
    
    cluster.waitActive();
    
    checkJobTrackerPage("jt1");
    checkJobTrackerPage("jt2");
  }
  
  private void checkJobTrackerPage(String jtId) throws IOException {
    String redirectAddress = conf.get(HAUtil.addKeySuffixes(
        HAUtil.MR_HA_JOBTRACKER_HTTP_REDIRECT_ADDRESS_KEY, "logicaljt", jtId));
    URL url = new URL("http://" + redirectAddress + "/jobtracker.jsp");
    String page = DFSTestUtil.urlGet(url);
    assertTrue(page.contains("Hadoop Map/Reduce Administration"));
  }

  public static class OKServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  private Server createJettyServer() throws Exception {
    InetAddress localhost = InetAddress.getByName("localhost");
    String host = "localhost";
    ServerSocket ss = new ServerSocket(0, 50, localhost);
    int port = ss.getLocalPort();
    ss.close();
    Server server = new Server(0);
    server.getConnectors()[0].setHost(host);
    server.getConnectors()[0].setPort(port);
    return server;
  }

  private URL getJettyURL(Server server) throws Exception {
    if (server == null) {
      throw new IllegalStateException("This test does not use @TestJetty");
    }
    return new URL("http://" + server.getConnectors()[0].getHost() + ":" + 
      server.getConnectors()[0].getPort());
  }

  private void testRedirect(Class firstServletClass, Class secondServlet,
                            int expectedResponse) throws Exception {
    Server server1 = null;
    Server server2 = null;
    try {
      server1 = createJettyServer();
      server2 = createJettyServer();

      Context context = new Context();
      context.setContextPath("/");
      context.addServlet(firstServletClass, "/*");
      context.setAttribute(JobTrackerHAHttpRedirector.ACTIVE_JOBTRACKER_BASEURL,
        getJettyURL(server2).toExternalForm());
      server1.addHandler(context);

      context = new Context();
      context.setContextPath("/");
      context.addServlet(secondServlet, "/*");
      context.setAttribute(JobTrackerHAHttpRedirector.ACTIVE_JOBTRACKER_BASEURL,
        getJettyURL(server1).toExternalForm());
      server2.addHandler(context);

      server1.start();
      server2.start();

      URL url = new URL(getJettyURL(server1), "/bar");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      assertEquals(expectedResponse, conn.getResponseCode());

    } finally {
      if (server1 != null) {
        server1.stop();
      }
      if (server2 != null) {
        server1.stop();
      }
    }
  }

  @Test
  public void redirectOK() throws Exception {
    testRedirect(JobTrackerHAHttpRedirector.RedirectorServlet.class,
      OKServlet.class, HttpServletResponse.SC_OK);
  }

  @Test
  public void redirectLoop() throws Exception {
    testRedirect(JobTrackerHAHttpRedirector.RedirectorServlet.class,
      JobTrackerHAHttpRedirector.RedirectorServlet.class,
      HttpServletResponse.SC_SERVICE_UNAVAILABLE);
  }

}

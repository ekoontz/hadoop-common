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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;

public class JobTrackerHAHttpRedirector {
  
  private static final Log LOG =
    LogFactory.getLog(JobTrackerHAHttpRedirector.class);

  static final String ACTIVE_JOBTRACKER_BASEURL =
    "mapred.ha.active.jobtracker.baseurl";

  private Configuration conf;
  private String activeJobTrackerUrl;
  private HttpServer server;

  public JobTrackerHAHttpRedirector(Configuration conf) {
    this.conf = conf;
    this.activeJobTrackerUrl = getActiveJobTrackerUrl(conf);
    if (activeJobTrackerUrl == null) {
      LOG.warn("No redirect address configured. Set " +
          HAUtil.MR_HA_JOBTRACKER_HTTP_REDIRECT_ADDRESS_KEY);
    } else {
      LOG.info("Redirect address is " + activeJobTrackerUrl);
    }
  }

  private String getActiveJobTrackerUrl(Configuration conf) {
    StringBuilder sb = new StringBuilder(HttpConfig.getSchemePrefix());
    String otherJtId = HAUtil.getJobTrackerIdOfOtherNode(conf);
    sb.append(HAUtil.getJtHaHttpRedirectAddress(conf, otherJtId));
    return sb.toString();
  }

  public synchronized void start() throws Exception {
    if (activeJobTrackerUrl == null) {
      return;
    }
    if (server == null) {
      @SuppressWarnings("deprecation")
      String infoAddr =
        NetUtils2.getServerAddress(conf,
          HAUtil.MR_JOBTRACKER_OLD_HTTP_ADDRESS_KEY,
          HAUtil.MR_JOBTRACKER_OLD_HTTP_PORT_KEY,
          HAUtil.MR_JOBTRACKER_HTTP_ADDRESS_KEY);
      LOG.info("Starting " + getClass().getSimpleName() + " on " + infoAddr);
      InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
      String infoBindAddress = infoSocAddr.getHostName();
      int infoBindPort = infoSocAddr.getPort();
      server = new HttpServer("static", infoBindAddress,
        infoBindPort, false);
      server.addInternalServlet("redirector", "/*", RedirectorServlet.class,
        false);
      server.setAttribute(ACTIVE_JOBTRACKER_BASEURL, activeJobTrackerUrl);
      server.start();
      LOG.info("Started");
    } else {
      throw new Exception("JobTrackerHAHttpRedirector already running");
    }
  }

  public synchronized void stop() throws Exception {
    if (server != null) {
      LOG.info("Stopping " + getClass().getSimpleName() + " on port " +
          server.getPort());
      server.stop();
      server = null;
      LOG.info("Stopped");
    }
  }

  public static class RedirectorServlet extends HttpServlet {
    private String baseURL;

    private static final String REDIR_COUNT = "redirCount";
    
    @Override
    public void init() {
      baseURL = (String)
        getServletContext().getAttribute(ACTIVE_JOBTRACKER_BASEURL);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
      String redirCountStr = req.getParameter(REDIR_COUNT);
      int redirCount = ((redirCountStr == null) ? 0 : Integer.parseInt(redirCountStr)) + 1;
      if (redirCount == 1) {
        StringBuilder sb = new StringBuilder(baseURL);
        sb.append(req.getRequestURI());
        String queryString = req.getQueryString();
        if (queryString != null) {
          sb.append("?").append(queryString);
        }
        sb.append((queryString == null) ? "?" : "&").append(REDIR_COUNT).append("=").append(redirCount);
        resp.sendRedirect(sb.toString());
      } else {
        resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "None of the JobTrackers is active");
      }
    }
  }

}

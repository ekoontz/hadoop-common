/** * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.util.Map.Entry;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@Path("/ws/v1/node")
public class NMWebServices {
  private Context nmContext;
  private ResourceView rview;
  private WebApp webapp;
  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private @javax.ws.rs.core.Context 
    HttpServletResponse response;

  @javax.ws.rs.core.Context
    UriInfo uriInfo;

  @Inject
  public NMWebServices(final Context nm, final ResourceView view,
      final WebApp webapp) {
    this.nmContext = nm;
    this.rview = view;
    this.webapp = webapp;
  }

  private void init() {
    //clear content type
    response.setContentType(null);
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public NodeInfo get() {
    return getNodeInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public NodeInfo getNodeInfo() {
    init();
    return new NodeInfo(this.nmContext, this.rview);
  }

  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppsInfo getNodeApps(@QueryParam("state") String stateQuery,
      @QueryParam("user") String userQuery) {
    init();
    AppsInfo allApps = new AppsInfo();
    for (Entry<ApplicationId, Application> entry : this.nmContext
        .getApplications().entrySet()) {

      AppInfo appInfo = new AppInfo(entry.getValue());
      if (stateQuery != null && !stateQuery.isEmpty()) {
        ApplicationState.valueOf(stateQuery);
        if (!appInfo.getState().equalsIgnoreCase(stateQuery)) {
          continue;
        }
      }
      if (userQuery != null) {
        if (userQuery.isEmpty()) {
          String msg = "Error: You must specify a non-empty string for the user";
          throw new BadRequestException(msg);
        }
        if (!appInfo.getUser().toString().equals(userQuery)) {
          continue;
        }
      }
      allApps.add(appInfo);
    }
    return allApps;
  }

  @GET
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo getNodeApp(@PathParam("appid") String appId) {
    init();
    ApplicationId id = ConverterUtils.toApplicationId(recordFactory, appId);
    if (id == null) {
      throw new NotFoundException("app with id " + appId + " not found");
    }
    Application app = this.nmContext.getApplications().get(id);
    if (app == null) {
      throw new NotFoundException("app with id " + appId + " not found");
    }
    return new AppInfo(app);

  }

  @GET
  @Path("/containers")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ContainersInfo getNodeContainers() {
    init();
    ContainersInfo allContainers = new ContainersInfo();
    for (Entry<ContainerId, Container> entry : this.nmContext.getContainers()
        .entrySet()) {
      if (entry.getValue() == null) {
        // just skip it
        continue;
      }
      ContainerInfo info = new ContainerInfo(this.nmContext, entry.getValue(),
          uriInfo.getBaseUri().toString(), webapp.name());
      allContainers.add(info);
    }
    return allContainers;
  }

  @GET
  @Path("/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ContainerInfo getNodeContainer(@PathParam("containerid") String id) {
    ContainerId containerId = null;
    init();
    try {
      containerId = ConverterUtils.toContainerId(id);
    } catch (Exception e) {
      throw new BadRequestException("invalid container id, " + id);
    }

    Container container = nmContext.getContainers().get(containerId);
    if (container == null) {
      throw new NotFoundException("container with id, " + id + ", not found");
    }
    return new ContainerInfo(this.nmContext, container, uriInfo.getBaseUri()
        .toString(), webapp.name());

  }

}

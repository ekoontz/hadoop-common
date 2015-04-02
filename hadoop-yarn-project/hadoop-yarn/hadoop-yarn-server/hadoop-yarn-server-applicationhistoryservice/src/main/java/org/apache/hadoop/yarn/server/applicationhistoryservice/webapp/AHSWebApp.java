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
package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.yarn.server.api.ApplicationContext;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManager;
import org.apache.hadoop.yarn.server.applicationhistoryservice.timeline.TimelineStore;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

public class AHSWebApp extends WebApp implements YarnWebParams {

  private final ApplicationHistoryManager applicationHistoryManager;
  private final TimelineStore timelineStore;

  public AHSWebApp(ApplicationHistoryManager applicationHistoryManager,
      TimelineStore timelineStore) {
    this.applicationHistoryManager = applicationHistoryManager;
    this.timelineStore = timelineStore;
  }

  @Override
  public void setup() {
    bind(YarnJacksonJaxbJsonProvider.class);
    bind(AHSWebServices.class);
    bind(TimelineWebServices.class);
    bind(GenericExceptionHandler.class);
    bind(ApplicationContext.class).toInstance(applicationHistoryManager);
    bind(TimelineStore.class).toInstance(timelineStore);
    route("/", AHSController.class);
    route(pajoin("/apps", APP_STATE), AHSController.class);
    route(pajoin("/app", APPLICATION_ID), AHSController.class, "app");
    route(pajoin("/appattempt", APPLICATION_ATTEMPT_ID), AHSController.class,
      "appattempt");
    route(pajoin("/container", CONTAINER_ID), AHSController.class, "container");
    route(
      pajoin("/logs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), AHSController.class, "logs");
  }
}
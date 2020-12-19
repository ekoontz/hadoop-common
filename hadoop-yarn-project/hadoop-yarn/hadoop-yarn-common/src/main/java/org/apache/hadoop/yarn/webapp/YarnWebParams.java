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

package org.apache.hadoop.yarn.webapp;

public interface YarnWebParams {
  String NM_NODENAME = "nm.id";
  String APPLICATION_ID = "app.id";
  String CONTAINER_ID = "container.id";
  String CONTAINER_LOG_TYPE= "log.type";
  String ENTITY_STRING = "entity.string";
  String APP_OWNER = "app.owner";
  String APP_STATE = "app.state";
  String QUEUE_NAME = "queue.name";
  String NODE_STATE = "node.state";
}

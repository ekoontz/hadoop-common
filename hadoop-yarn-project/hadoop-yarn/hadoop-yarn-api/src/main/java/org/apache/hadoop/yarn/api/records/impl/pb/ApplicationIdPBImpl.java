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

package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProtoOrBuilder;


    
public class ApplicationIdPBImpl extends ApplicationId {
  ApplicationIdProto proto = ApplicationIdProto.getDefaultInstance();
  ApplicationIdProto.Builder builder = null;
  boolean viaProto = false;
  
  public ApplicationIdPBImpl() {
    builder = ApplicationIdProto.newBuilder();
  }

  public ApplicationIdPBImpl(ApplicationIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized ApplicationIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized int getId() {
    ApplicationIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }
  @Override
  public synchronized long getClusterTimestamp() {
    ApplicationIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getClusterTimestamp());
  }

  @Override
  public synchronized void setClusterTimestamp(long clusterTimestamp) {
    maybeInitBuilder();
    builder.setClusterTimestamp((clusterTimestamp));
  }
}
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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProtoOrBuilder;


    
public class GetApplicationReportRequestPBImpl extends ProtoBase<GetApplicationReportRequestProto> implements GetApplicationReportRequest {
  GetApplicationReportRequestProto proto = GetApplicationReportRequestProto.getDefaultInstance();
  GetApplicationReportRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  
  
  public GetApplicationReportRequestPBImpl() {
    builder = GetApplicationReportRequestProto.newBuilder();
  }

  public GetApplicationReportRequestPBImpl(GetApplicationReportRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetApplicationReportRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationReportRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationId getApplicationId() {
    GetApplicationReportRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) 
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }



}  

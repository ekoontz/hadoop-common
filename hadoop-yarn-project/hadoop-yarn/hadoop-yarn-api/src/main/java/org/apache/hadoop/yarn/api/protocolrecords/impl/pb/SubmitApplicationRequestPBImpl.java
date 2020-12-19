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


import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProtoOrBuilder;


    
public class SubmitApplicationRequestPBImpl extends ProtoBase<SubmitApplicationRequestProto> implements SubmitApplicationRequest {
  SubmitApplicationRequestProto proto = SubmitApplicationRequestProto.getDefaultInstance();
  SubmitApplicationRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationSubmissionContext applicationSubmissionContext = null;
  
  
  public SubmitApplicationRequestPBImpl() {
    builder = SubmitApplicationRequestProto.newBuilder();
  }

  public SubmitApplicationRequestPBImpl(SubmitApplicationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public SubmitApplicationRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationSubmissionContext != null) {
      builder.setApplicationSubmissionContext(convertToProtoFormat(this.applicationSubmissionContext));
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
      builder = SubmitApplicationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    SubmitApplicationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationSubmissionContext != null) {
      return this.applicationSubmissionContext;
    }
    if (!p.hasApplicationSubmissionContext()) {
      return null;
    }
    this.applicationSubmissionContext = convertFromProtoFormat(p.getApplicationSubmissionContext());
    return this.applicationSubmissionContext;
  }

  @Override
  public void setApplicationSubmissionContext(ApplicationSubmissionContext applicationSubmissionContext) {
    maybeInitBuilder();
    if (applicationSubmissionContext == null) 
      builder.clearApplicationSubmissionContext();
    this.applicationSubmissionContext = applicationSubmissionContext;
  }

  private ApplicationSubmissionContextPBImpl convertFromProtoFormat(ApplicationSubmissionContextProto p) {
    return new ApplicationSubmissionContextPBImpl(p);
  }

  private ApplicationSubmissionContextProto convertToProtoFormat(ApplicationSubmissionContext t) {
    return ((ApplicationSubmissionContextPBImpl)t).getProto();
  }



}  

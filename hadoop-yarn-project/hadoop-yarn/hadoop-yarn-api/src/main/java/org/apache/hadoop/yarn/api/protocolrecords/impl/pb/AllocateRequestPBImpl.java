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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProtoOrBuilder;


    
public class AllocateRequestPBImpl extends ProtoBase<AllocateRequestProto> implements AllocateRequest {
  AllocateRequestProto proto = AllocateRequestProto.getDefaultInstance();
  AllocateRequestProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationAttemptId applicationAttemptID = null;
  private List<ResourceRequest> ask = null;
  private List<ContainerId> release = null;
  
  
  public AllocateRequestPBImpl() {
    builder = AllocateRequestProto.newBuilder();
  }

  public AllocateRequestPBImpl(AllocateRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public AllocateRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationAttemptID != null) {
      builder.setApplicationAttemptId(convertToProtoFormat(this.applicationAttemptID));
    }
    if (this.ask != null) {
      addAsksToProto();
    }
    if (this.release != null) {
      addReleasesToProto();
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
      builder = AllocateRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationAttemptID != null) {
      return this.applicationAttemptID;
    }
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.applicationAttemptID = convertFromProtoFormat(p.getApplicationAttemptId());
    return this.applicationAttemptID;
  }

  @Override
  public void setApplicationAttemptId(ApplicationAttemptId appAttemptId) {
    maybeInitBuilder();
    if (appAttemptId == null) 
      builder.clearApplicationAttemptId();
    this.applicationAttemptID = appAttemptId;
  }

  @Override
  public int getResponseId() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getResponseId();
  }

  @Override
  public void setResponseId(int id) {
    maybeInitBuilder();
    builder.setResponseId(id);
  }

  @Override
  public float getProgress() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getProgress();
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress(progress);
  }

  @Override
  public List<ResourceRequest> getAskList() {
    initAsks();
    return this.ask;
  }
  @Override
  public ResourceRequest getAsk(int index) {
    initAsks();
    return this.ask.get(index);
  }
  @Override
  public int getAskCount() {
    initAsks();
    return this.ask.size();
  }
  
  private void initAsks() {
    if (this.ask != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceRequestProto> list = p.getAskList();
    this.ask = new ArrayList<ResourceRequest>();

    for (ResourceRequestProto c : list) {
      this.ask.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllAsks(final List<ResourceRequest> ask) {
    if (ask == null)
      return;
    initAsks();
    this.ask.addAll(ask);
  }
  
  private void addAsksToProto() {
    maybeInitBuilder();
    builder.clearAsk();
    if (ask == null)
      return;
    Iterable<ResourceRequestProto> iterable = new Iterable<ResourceRequestProto>() {
      @Override
      public Iterator<ResourceRequestProto> iterator() {
        return new Iterator<ResourceRequestProto>() {

          Iterator<ResourceRequest> iter = ask.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceRequestProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllAsk(iterable);
  }
  @Override
  public void addAsk(ResourceRequest ask) {
    initAsks();
    this.ask.add(ask);
  }
  @Override
  public void removeAsk(int index) {
    initAsks();
    this.ask.remove(index);
  }
  @Override
  public void clearAsks() {
    initAsks();
    this.ask.clear();
  }
  @Override
  public List<ContainerId> getReleaseList() {
    initReleases();
    return this.release;
  }
  @Override
  public ContainerId getRelease(int index) {
    initReleases();
    return this.release.get(index);
  }
  @Override
  public int getReleaseCount() {
    initReleases();
    return this.release.size();
  }
  
  private void initReleases() {
    if (this.release != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getReleaseList();
    this.release = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.release.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllReleases(final List<ContainerId> release) {
    if (release == null)
      return;
    initReleases();
    this.release.addAll(release);
  }
  
  private void addReleasesToProto() {
    maybeInitBuilder();
    builder.clearRelease();
    if (release == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = release.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllRelease(iterable);
  }
  @Override
  public void addRelease(ContainerId release) {
    initReleases();
    this.release.add(release);
  }
  @Override
  public void removeRelease(int index) {
    initReleases();
    this.release.remove(index);
  }
  @Override
  public void clearReleases() {
    initReleases();
    this.release.clear();
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

  private ResourceRequestPBImpl convertFromProtoFormat(ResourceRequestProto p) {
    return new ResourceRequestPBImpl(p);
  }

  private ResourceRequestProto convertToProtoFormat(ResourceRequest t) {
    return ((ResourceRequestPBImpl)t).getProto();
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
}  

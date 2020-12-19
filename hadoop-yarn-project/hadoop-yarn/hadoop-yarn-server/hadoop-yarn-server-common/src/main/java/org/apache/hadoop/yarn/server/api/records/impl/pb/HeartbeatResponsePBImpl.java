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

package org.apache.hadoop.yarn.server.api.records.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.HeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.HeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeActionProto;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

public class HeartbeatResponsePBImpl extends
    ProtoBase<HeartbeatResponseProto> implements HeartbeatResponse {
  HeartbeatResponseProto proto = HeartbeatResponseProto.getDefaultInstance();
  HeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private List<ContainerId> containersToCleanup = null;
  private List<ApplicationId> applicationsToCleanup = null;
  private MasterKey masterKey = null;
  
  public HeartbeatResponsePBImpl() {
    builder = HeartbeatResponseProto.newBuilder();
  }

  public HeartbeatResponsePBImpl(HeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public HeartbeatResponseProto getProto() {

      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containersToCleanup != null) {
      addContainersToCleanupToProto();
    }
    if (this.applicationsToCleanup != null) {
      addApplicationsToCleanupToProto();
    }
    if (this.masterKey != null) {
      builder.setMasterKey(convertToProtoFormat(this.masterKey));
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
      builder = HeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getResponseId() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }

  @Override
  public MasterKey getMasterKey() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.masterKey != null) {
      return this.masterKey;
    }
    if (!p.hasMasterKey()) {
      return null;
    }
    this.masterKey = convertFromProtoFormat(p.getMasterKey());
    return this.masterKey;
  }

  @Override
  public void setMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) 
      builder.clearMasterKey();
    this.masterKey = masterKey;
  }

  @Override
  public NodeAction getNodeAction() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasNodeAction()) {
      return null;
    }
    return (convertFromProtoFormat(p.getNodeAction()));
  }

  @Override
  public void setNodeAction(NodeAction nodeAction) {
    maybeInitBuilder();
    if (nodeAction == null) {
      builder.clearNodeAction();
      return;
    }
    builder.setNodeAction(convertToProtoFormat(nodeAction));
  }

  @Override
  public List<ContainerId> getContainersToCleanupList() {
    initContainersToCleanup();
    return this.containersToCleanup;
  }
  @Override
  public ContainerId getContainerToCleanup(int index) {
    initContainersToCleanup();
    return this.containersToCleanup.get(index);
  }
  @Override
  public int getContainersToCleanupCount() {
    initContainersToCleanup();
    return this.containersToCleanup.size();
  }
  
  private void initContainersToCleanup() {
    if (this.containersToCleanup != null) {
      return;
    }
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getContainersToCleanupList();
    this.containersToCleanup = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.containersToCleanup.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllContainersToCleanup(final List<ContainerId> containersToCleanup) {
    if (containersToCleanup == null)
      return;
    initContainersToCleanup();
    this.containersToCleanup.addAll(containersToCleanup);
  }
  
  private void addContainersToCleanupToProto() {
    maybeInitBuilder();
    builder.clearContainersToCleanup();
    if (containersToCleanup == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = containersToCleanup.iterator();

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
    builder.addAllContainersToCleanup(iterable);
  }
  @Override
  public void addContainerToCleanup(ContainerId containersToCleanup) {
    initContainersToCleanup();
    this.containersToCleanup.add(containersToCleanup);
  }
  @Override
  public void removeContainerToCleanup(int index) {
    initContainersToCleanup();
    this.containersToCleanup.remove(index);
  }
  @Override
  public void clearContainersToCleanup() {
    initContainersToCleanup();
    this.containersToCleanup.clear();
  }
  @Override
  public List<ApplicationId> getApplicationsToCleanupList() {
    initApplicationsToCleanup();
    return this.applicationsToCleanup;
  }
  @Override
  public ApplicationId getApplicationsToCleanup(int index) {
    initApplicationsToCleanup();
    return this.applicationsToCleanup.get(index);
  }
  @Override
  public int getApplicationsToCleanupCount() {
    initApplicationsToCleanup();
    return this.applicationsToCleanup.size();
  }
  
  private void initApplicationsToCleanup() {
    if (this.applicationsToCleanup != null) {
      return;
    }
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationIdProto> list = p.getApplicationsToCleanupList();
    this.applicationsToCleanup = new ArrayList<ApplicationId>();

    for (ApplicationIdProto c : list) {
      this.applicationsToCleanup.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllApplicationsToCleanup(final List<ApplicationId> applicationsToCleanup) {
    if (applicationsToCleanup == null)
      return;
    initApplicationsToCleanup();
    this.applicationsToCleanup.addAll(applicationsToCleanup);
  }
  
  private void addApplicationsToCleanupToProto() {
    maybeInitBuilder();
    builder.clearApplicationsToCleanup();
    if (applicationsToCleanup == null)
      return;
    Iterable<ApplicationIdProto> iterable = new Iterable<ApplicationIdProto>() {
      @Override
      public Iterator<ApplicationIdProto> iterator() {
        return new Iterator<ApplicationIdProto>() {

          Iterator<ApplicationId> iter = applicationsToCleanup.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ApplicationIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllApplicationsToCleanup(iterable);
  }
  @Override
  public void addApplicationToCleanup(ApplicationId applicationsToCleanup) {
    initApplicationsToCleanup();
    this.applicationsToCleanup.add(applicationsToCleanup);
  }
  @Override
  public void removeApplicationToCleanup(int index) {
    initApplicationsToCleanup();
    this.applicationsToCleanup.remove(index);
  }
  @Override
  public void clearApplicationsToCleanup() {
    initApplicationsToCleanup();
    this.applicationsToCleanup.clear();
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
  
  private NodeAction convertFromProtoFormat(NodeActionProto p) {
    return  NodeAction.valueOf(p.name());
  }
  
  private NodeActionProto convertToProtoFormat(NodeAction t) {
    return NodeActionProto.valueOf(t.name());
  }

  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }

  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl)t).getProto();
  }
}  

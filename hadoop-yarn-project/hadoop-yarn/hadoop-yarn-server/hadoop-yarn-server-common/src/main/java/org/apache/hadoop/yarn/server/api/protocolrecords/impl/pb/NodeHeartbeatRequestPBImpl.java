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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;

public class NodeHeartbeatRequestPBImpl extends
    ProtoBase<NodeHeartbeatRequestProto> implements NodeHeartbeatRequest {
  NodeHeartbeatRequestProto proto = NodeHeartbeatRequestProto.getDefaultInstance();
  NodeHeartbeatRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private NodeStatus nodeStatus = null;
  private MasterKey lastKnownMasterKey = null;
  
  public NodeHeartbeatRequestPBImpl() {
    builder = NodeHeartbeatRequestProto.newBuilder();
  }

  public NodeHeartbeatRequestPBImpl(NodeHeartbeatRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeHeartbeatRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeStatus != null) {
      builder.setNodeStatus(convertToProtoFormat(this.nodeStatus));
    }
    if (this.lastKnownMasterKey != null) {
      builder
        .setLastKnownMasterKey(convertToProtoFormat(this.lastKnownMasterKey));
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
      builder = NodeHeartbeatRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public NodeStatus getNodeStatus() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeStatus != null) {
      return this.nodeStatus;
    }
    if (!p.hasNodeStatus()) {
      return null;
    }
    this.nodeStatus = convertFromProtoFormat(p.getNodeStatus());
    return this.nodeStatus;
  }

  @Override
  public void setNodeStatus(NodeStatus nodeStatus) {
    maybeInitBuilder();
    if (nodeStatus == null) 
      builder.clearNodeStatus();
    this.nodeStatus = nodeStatus;
  }

  @Override
  public MasterKey getLastKnownMasterKey() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.lastKnownMasterKey != null) {
      return this.lastKnownMasterKey;
    }
    if (!p.hasLastKnownMasterKey()) {
      return null;
    }
    this.lastKnownMasterKey = convertFromProtoFormat(p.getLastKnownMasterKey());
    return this.lastKnownMasterKey;
  }

  @Override
  public void setLastKnownMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) 
      builder.clearLastKnownMasterKey();
    this.lastKnownMasterKey = masterKey;
  }

  private NodeStatusPBImpl convertFromProtoFormat(NodeStatusProto p) {
    return new NodeStatusPBImpl(p);
  }

  private NodeStatusProto convertToProtoFormat(NodeStatus t) {
    return ((NodeStatusPBImpl)t).getProto();
  }

  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }

  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl)t).getProto();
  }
}  

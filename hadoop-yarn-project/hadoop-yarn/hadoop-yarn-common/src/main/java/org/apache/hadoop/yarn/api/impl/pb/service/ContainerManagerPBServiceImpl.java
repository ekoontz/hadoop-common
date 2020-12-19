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

package org.apache.hadoop.yarn.api.impl.pb.service;

import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.ContainerManagerPB;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class ContainerManagerPBServiceImpl implements ContainerManagerPB  {

  private ContainerManager real;
  
  public ContainerManagerPBServiceImpl(ContainerManager impl) {
    this.real = impl;
  }

  @Override
  public GetContainerStatusResponseProto getContainerStatus(RpcController arg0,
      GetContainerStatusRequestProto proto) throws ServiceException {
    GetContainerStatusRequestPBImpl request = new GetContainerStatusRequestPBImpl(proto);
    try {
      GetContainerStatusResponse response = real.getContainerStatus(request);
      return ((GetContainerStatusResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StartContainerResponseProto startContainer(RpcController arg0,
      StartContainerRequestProto proto) throws ServiceException {
    StartContainerRequestPBImpl request = new StartContainerRequestPBImpl(proto);
    try {
      StartContainerResponse response = real.startContainer(request);
      return ((StartContainerResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StopContainerResponseProto stopContainer(RpcController arg0,
      StopContainerRequestProto proto) throws ServiceException {
    StopContainerRequestPBImpl request = new StopContainerRequestPBImpl(proto);
    try {
      StopContainerResponse response = real.stopContainer(request);
      return ((StopContainerResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

}

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

package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.MoveApplicationAcrossQueuesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.MoveApplicationAcrossQueuesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersRequestProto;


import com.google.protobuf.ServiceException;

@Private
public class ApplicationClientProtocolPBClientImpl implements ApplicationClientProtocol,
    Closeable {

  private ApplicationClientProtocolPB proxy;

  public ApplicationClientProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ApplicationClientProtocolPB.class,
      ProtobufRpcEngine.class);
    proxy = RPC.getProxy(ApplicationClientProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    KillApplicationRequestProto requestProto =
        ((KillApplicationRequestPBImpl) request).getProto();
    try {
      return new KillApplicationResponsePBImpl(proxy.forceKillApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException,
      IOException {
    GetApplicationReportRequestProto requestProto =
        ((GetApplicationReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationReportResponsePBImpl(proxy.getApplicationReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException,
      IOException {
    GetClusterMetricsRequestProto requestProto =
        ((GetClusterMetricsRequestPBImpl) request).getProto();
    try {
      return new GetClusterMetricsResponsePBImpl(proxy.getClusterMetrics(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException,
      IOException {
    GetNewApplicationRequestProto requestProto =
        ((GetNewApplicationRequestPBImpl) request).getProto();
    try {
      return new GetNewApplicationResponsePBImpl(proxy.getNewApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException,
      IOException {
    SubmitApplicationRequestProto requestProto =
        ((SubmitApplicationRequestPBImpl) request).getProto();
    try {
      return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request) throws YarnException,
      IOException {
    GetApplicationsRequestProto requestProto =
        ((GetApplicationsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationsResponsePBImpl(proxy.getApplications(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterNodesResponse
      getClusterNodes(GetClusterNodesRequest request)
          throws YarnException, IOException {
    GetClusterNodesRequestProto requestProto =
        ((GetClusterNodesRequestPBImpl) request).getProto();
    try {
      return new GetClusterNodesResponsePBImpl(proxy.getClusterNodes(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    GetQueueInfoRequestProto requestProto =
        ((GetQueueInfoRequestPBImpl) request).getProto();
    try {
      return new GetQueueInfoResponsePBImpl(proxy.getQueueInfo(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException,
      IOException {
    GetQueueUserAclsInfoRequestProto requestProto =
        ((GetQueueUserAclsInfoRequestPBImpl) request).getProto();
    try {
      return new GetQueueUserAclsInfoResponsePBImpl(proxy.getQueueUserAcls(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException,
      IOException {
    GetDelegationTokenRequestProto requestProto =
        ((GetDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new GetDelegationTokenResponsePBImpl(proxy.getDelegationToken(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException,
      IOException {
    RenewDelegationTokenRequestProto requestProto = 
        ((RenewDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new RenewDelegationTokenResponsePBImpl(proxy.renewDelegationToken(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException,
      IOException {
    CancelDelegationTokenRequestProto requestProto =
        ((CancelDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new CancelDelegationTokenResponsePBImpl(
          proxy.cancelDelegationToken(null, requestProto));

    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
  
  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException,
      IOException {
    MoveApplicationAcrossQueuesRequestProto requestProto =
        ((MoveApplicationAcrossQueuesRequestPBImpl) request).getProto();
    try {
      return new MoveApplicationAcrossQueuesResponsePBImpl(
          proxy.moveApplicationAcrossQueues(null, requestProto));

    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
  
  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException {
    GetApplicationAttemptReportRequestProto requestProto =
        ((GetApplicationAttemptReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptReportResponsePBImpl(
        proxy.getApplicationAttemptReport(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    GetApplicationAttemptsRequestProto requestProto =
        ((GetApplicationAttemptsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptsResponsePBImpl(
        proxy.getApplicationAttempts(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    GetContainerReportRequestProto requestProto =
        ((GetContainerReportRequestPBImpl) request).getProto();
    try {
      return new GetContainerReportResponsePBImpl(proxy.getContainerReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    GetContainersRequestProto requestProto =
        ((GetContainersRequestPBImpl) request).getProto();
    try {
      return new GetContainersResponsePBImpl(proxy.getContainers(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

}

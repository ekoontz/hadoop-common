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
package org.apache.hadoop.hdfs.qjournal.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalIdProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.RequestInfoProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link JournalProtocol} interfaces to the RPC server implementing
 * {@link JournalProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class QJournalProtocolTranslatorPB implements ProtocolMetaInterface,
    QJournalProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final QJournalProtocolPB rpcProxy;
  
  public QJournalProtocolTranslatorPB(QJournalProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }


  @Override
  public boolean isFormatted(String journalId) throws IOException {
    try {
      IsFormattedRequestProto req = IsFormattedRequestProto.newBuilder()
          .setJid(convertJournalId(journalId))
          .build();
      IsFormattedResponseProto resp = rpcProxy.isFormatted(
          NULL_CONTROLLER, req);
      return resp.getIsFormatted();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public GetJournalStateResponseProto getJournalState(String jid)
      throws IOException {
    try {
      GetJournalStateRequestProto req = GetJournalStateRequestProto.newBuilder()
          .setJid(convertJournalId(jid))
          .build();
      return rpcProxy.getJournalState(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private JournalIdProto convertJournalId(String jid) {
    return JournalIdProto.newBuilder()
        .setIdentifier(jid)
        .build();
  }
  
  @Override
  public void format(String jid, NamespaceInfo nsInfo) throws IOException {
    try {
      FormatRequestProto req = FormatRequestProto.newBuilder()
          .setJid(convertJournalId(jid))
          .setNsInfo(PBHelper.convert(nsInfo))
          .build();
      rpcProxy.format(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public NewEpochResponseProto newEpoch(String jid, NamespaceInfo nsInfo,
      long epoch) throws IOException {
    try {
      NewEpochRequestProto req = NewEpochRequestProto.newBuilder()
        .setJid(convertJournalId(jid))
        .setNsInfo(PBHelper.convert(nsInfo))
        .setEpoch(epoch)
        .build();
      return rpcProxy.newEpoch(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void journal(RequestInfo reqInfo,
      long segmentTxId, long firstTxnId, int numTxns,
      byte[] records) throws IOException {
    JournalRequestProto req = JournalRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setSegmentTxnId(segmentTxId)
        .setFirstTxnId(firstTxnId)
        .setNumTxns(numTxns)
        .setRecords(PBHelper.getByteString(records))
        .build();
    try {
      rpcProxy.journal(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public void heartbeat(RequestInfo reqInfo) throws IOException {
    try {
      rpcProxy.heartbeat(NULL_CONTROLLER, HeartbeatRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private QJournalProtocolProtos.RequestInfoProto convert(
      RequestInfo reqInfo) {
    RequestInfoProto.Builder builder = RequestInfoProto.newBuilder()
        .setJournalId(convertJournalId(reqInfo.getJournalId()))
        .setEpoch(reqInfo.getEpoch())
        .setIpcSerialNumber(reqInfo.getIpcSerialNumber());
    if (reqInfo.hasCommittedTxId()) {
      builder.setCommittedTxId(reqInfo.getCommittedTxId());
    }
    return builder.build();
  }

  @Override
  public void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    StartLogSegmentRequestProto req = StartLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setTxid(txid)
        .build();
    try {
      rpcProxy.startLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    FinalizeLogSegmentRequestProto req =
        FinalizeLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setStartTxId(startTxId)
        .setEndTxId(endTxId)
        .build();
    try {
      rpcProxy.finalizeLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public void purgeLogsOlderThan(RequestInfo reqInfo, long minTxIdToKeep)
      throws IOException {
    PurgeLogsRequestProto req = PurgeLogsRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setMinTxIdToKeep(minTxIdToKeep)
        .build();
    try {
      rpcProxy.purgeLogs(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(String jid,
      long sinceTxId, boolean forReading) throws IOException {
    try {
      return rpcProxy.getEditLogManifest(NULL_CONTROLLER,
          GetEditLogManifestRequestProto.newBuilder()
            .setJid(convertJournalId(jid))
            .setSinceTxId(sinceTxId)
            .setForReading(forReading)
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public PrepareRecoveryResponseProto prepareRecovery(RequestInfo reqInfo,
      long segmentTxId) throws IOException {
    try {
      return rpcProxy.prepareRecovery(NULL_CONTROLLER,
          PrepareRecoveryRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .setSegmentTxId(segmentTxId)
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void acceptRecovery(RequestInfo reqInfo,
      SegmentStateProto stateToAccept, URL fromUrl) throws IOException {
    try {
      rpcProxy.acceptRecovery(NULL_CONTROLLER,
          AcceptRecoveryRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .setStateToAccept(stateToAccept)
            .setFromURL(fromUrl.toExternalForm())
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        QJournalProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(QJournalProtocolPB.class), methodName);
  }

}

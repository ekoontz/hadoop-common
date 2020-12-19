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
package org.apache.hadoop.hdfs.protocolPB;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockRecoveryCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockTokenIdentifierProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlocksWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CheckpointSignatureProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExportedBlockKeysProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RecoveringBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogManifestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests for {@link PBHelper}
 */
public class TestPBHelper {

  /**
   * Used for asserting equality on doubles.
   */
  private static final double DELTA = 0.000001;

  @Test
  public void testConvertNamenodeRole() {
    assertEquals(NamenodeRoleProto.BACKUP,
        PBHelper.convert(NamenodeRole.BACKUP));
    assertEquals(NamenodeRoleProto.CHECKPOINT,
        PBHelper.convert(NamenodeRole.CHECKPOINT));
    assertEquals(NamenodeRoleProto.NAMENODE,
        PBHelper.convert(NamenodeRole.NAMENODE));
    assertEquals(NamenodeRole.BACKUP,
        PBHelper.convert(NamenodeRoleProto.BACKUP));
    assertEquals(NamenodeRole.CHECKPOINT,
        PBHelper.convert(NamenodeRoleProto.CHECKPOINT));
    assertEquals(NamenodeRole.NAMENODE,
        PBHelper.convert(NamenodeRoleProto.NAMENODE));
  }

  private static StorageInfo getStorageInfo() {
    return new StorageInfo(1, 2, "cid", 3);
  }

  @Test
  public void testConvertStoragInfo() {
    StorageInfo info = getStorageInfo();
    StorageInfoProto infoProto = PBHelper.convert(info);
    StorageInfo info2 = PBHelper.convert(infoProto);
    assertEquals(info.getClusterID(), info2.getClusterID());
    assertEquals(info.getCTime(), info2.getCTime());
    assertEquals(info.getLayoutVersion(), info2.getLayoutVersion());
    assertEquals(info.getNamespaceID(), info2.getNamespaceID());
  }

  @Test
  public void testConvertNamenodeRegistration() {
    StorageInfo info = getStorageInfo();
    NamenodeRegistration reg = new NamenodeRegistration("address:999",
        "http:1000", info, NamenodeRole.NAMENODE);
    NamenodeRegistrationProto regProto = PBHelper.convert(reg);
    NamenodeRegistration reg2 = PBHelper.convert(regProto);
    assertEquals(reg.getAddress(), reg2.getAddress());
    assertEquals(reg.getClusterID(), reg2.getClusterID());
    assertEquals(reg.getCTime(), reg2.getCTime());
    assertEquals(reg.getHttpAddress(), reg2.getHttpAddress());
    assertEquals(reg.getLayoutVersion(), reg2.getLayoutVersion());
    assertEquals(reg.getNamespaceID(), reg2.getNamespaceID());
    assertEquals(reg.getRegistrationID(), reg2.getRegistrationID());
    assertEquals(reg.getRole(), reg2.getRole());
    assertEquals(reg.getVersion(), reg2.getVersion());

  }

  @Test
  public void testConvertDatanodeID() {
    DatanodeID dn = DFSTestUtil.getLocalDatanodeID();
    DatanodeIDProto dnProto = PBHelper.convert(dn);
    DatanodeID dn2 = PBHelper.convert(dnProto);
    compare(dn, dn2);
  }
  
  void compare(DatanodeID dn, DatanodeID dn2) {
    assertEquals(dn.getIpAddr(), dn2.getIpAddr());
    assertEquals(dn.getHostName(), dn2.getHostName());
    assertEquals(dn.getStorageID(), dn2.getStorageID());
    assertEquals(dn.getXferPort(), dn2.getXferPort());
    assertEquals(dn.getInfoPort(), dn2.getInfoPort());
    assertEquals(dn.getIpcPort(), dn2.getIpcPort());
  }

  @Test
  public void testConvertBlock() {
    Block b = new Block(1, 100, 3);
    BlockProto bProto = PBHelper.convert(b);
    Block b2 = PBHelper.convert(bProto);
    assertEquals(b, b2);
  }

  private static BlockWithLocations getBlockWithLocations(int bid) {
    return new BlockWithLocations(new Block(bid, 0, 1), new String[] { "dn1",
        "dn2", "dn3" });
  }

  private void compare(BlockWithLocations locs1, BlockWithLocations locs2) {
    assertEquals(locs1.getBlock(), locs2.getBlock());
    assertTrue(Arrays.equals(locs1.getStorageIDs(), locs2.getStorageIDs()));
  }

  @Test
  public void testConvertBlockWithLocations() {
    BlockWithLocations locs = getBlockWithLocations(1);
    BlockWithLocationsProto locsProto = PBHelper.convert(locs);
    BlockWithLocations locs2 = PBHelper.convert(locsProto);
    compare(locs, locs2);
  }

  @Test
  public void testConvertBlocksWithLocations() {
    BlockWithLocations[] list = new BlockWithLocations[] {
        getBlockWithLocations(1), getBlockWithLocations(2) };
    BlocksWithLocations locs = new BlocksWithLocations(list);
    BlocksWithLocationsProto locsProto = PBHelper.convert(locs);
    BlocksWithLocations locs2 = PBHelper.convert(locsProto);
    BlockWithLocations[] blocks = locs.getBlocks();
    BlockWithLocations[] blocks2 = locs2.getBlocks();
    assertEquals(blocks.length, blocks2.length);
    for (int i = 0; i < blocks.length; i++) {
      compare(blocks[i], blocks2[i]);
    }
  }

  private static BlockKey getBlockKey(int keyId) {
    return new BlockKey(keyId, 10, "encodedKey".getBytes());
  }

  private void compare(BlockKey k1, BlockKey k2) {
    assertEquals(k1.getExpiryDate(), k2.getExpiryDate());
    assertEquals(k1.getKeyId(), k2.getKeyId());
    assertTrue(Arrays.equals(k1.getEncodedKey(), k2.getEncodedKey()));
  }

  @Test
  public void testConvertBlockKey() {
    BlockKey key = getBlockKey(1);
    BlockKeyProto keyProto = PBHelper.convert(key);
    BlockKey key1 = PBHelper.convert(keyProto);
    compare(key, key1);
  }

  @Test
  public void testConvertExportedBlockKeys() {
    BlockKey[] keys = new BlockKey[] { getBlockKey(2), getBlockKey(3) };
    ExportedBlockKeys expKeys = new ExportedBlockKeys(true, 9, 10,
        getBlockKey(1), keys);
    ExportedBlockKeysProto expKeysProto = PBHelper.convert(expKeys);
    ExportedBlockKeys expKeys1 = PBHelper.convert(expKeysProto);
    compare(expKeys, expKeys1);
  }
  
  void compare(ExportedBlockKeys expKeys, ExportedBlockKeys expKeys1) {
    BlockKey[] allKeys = expKeys.getAllKeys();
    BlockKey[] allKeys1 = expKeys1.getAllKeys();
    assertEquals(allKeys.length, allKeys1.length);
    for (int i = 0; i < allKeys.length; i++) {
      compare(allKeys[i], allKeys1[i]);
    }
    compare(expKeys.getCurrentKey(), expKeys1.getCurrentKey());
    assertEquals(expKeys.getKeyUpdateInterval(),
        expKeys1.getKeyUpdateInterval());
    assertEquals(expKeys.getTokenLifetime(), expKeys1.getTokenLifetime());
  }

  @Test
  public void testConvertCheckpointSignature() {
    CheckpointSignature s = new CheckpointSignature(getStorageInfo(), "bpid",
        100, 1);
    CheckpointSignatureProto sProto = PBHelper.convert(s);
    CheckpointSignature s1 = PBHelper.convert(sProto);
    assertEquals(s.getBlockpoolID(), s1.getBlockpoolID());
    assertEquals(s.getClusterID(), s1.getClusterID());
    assertEquals(s.getCTime(), s1.getCTime());
    assertEquals(s.getCurSegmentTxId(), s1.getCurSegmentTxId());
    assertEquals(s.getLayoutVersion(), s1.getLayoutVersion());
    assertEquals(s.getMostRecentCheckpointTxId(),
        s1.getMostRecentCheckpointTxId());
    assertEquals(s.getNamespaceID(), s1.getNamespaceID());
  }
  
  private static void compare(RemoteEditLog l1, RemoteEditLog l2) {
    assertEquals(l1.getEndTxId(), l2.getEndTxId());
    assertEquals(l1.getStartTxId(), l2.getStartTxId());
  }
  
  @Test
  public void testConvertRemoteEditLog() {
    RemoteEditLog l = new RemoteEditLog(1, 100);
    RemoteEditLogProto lProto = PBHelper.convert(l);
    RemoteEditLog l1 = PBHelper.convert(lProto);
    compare(l, l1);
  }
  
  @Test
  public void testConvertRemoteEditLogManifest() {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>();
    logs.add(new RemoteEditLog(1, 10));
    logs.add(new RemoteEditLog(11, 20));
    RemoteEditLogManifest m = new RemoteEditLogManifest(logs);
    RemoteEditLogManifestProto mProto = PBHelper.convert(m);
    RemoteEditLogManifest m1 = PBHelper.convert(mProto);
    
    List<RemoteEditLog> logs1 = m1.getLogs();
    assertEquals(logs.size(), logs1.size());
    for (int i = 0; i < logs.size(); i++) {
      compare(logs.get(i), logs1.get(i));
    }
  }
  public ExtendedBlock getExtendedBlock() {
    return getExtendedBlock(1);
  }
  
  public ExtendedBlock getExtendedBlock(long blkid) {
    return new ExtendedBlock("bpid", blkid, 100, 2);
  }
  
  private void compare(DatanodeInfo dn1, DatanodeInfo dn2) {
      assertEquals(dn1.getAdminState(), dn2.getAdminState());
      assertEquals(dn1.getBlockPoolUsed(), dn2.getBlockPoolUsed());
      assertEquals(dn1.getBlockPoolUsedPercent(), 
          dn2.getBlockPoolUsedPercent(), DELTA);
      assertEquals(dn1.getCapacity(), dn2.getCapacity());
      assertEquals(dn1.getDatanodeReport(), dn2.getDatanodeReport());
      assertEquals(dn1.getDfsUsed(), dn1.getDfsUsed());
      assertEquals(dn1.getDfsUsedPercent(), dn1.getDfsUsedPercent(), DELTA);
      assertEquals(dn1.getIpAddr(), dn2.getIpAddr());
      assertEquals(dn1.getHostName(), dn2.getHostName());
      assertEquals(dn1.getInfoPort(), dn2.getInfoPort());
      assertEquals(dn1.getIpcPort(), dn2.getIpcPort());
      assertEquals(dn1.getLastUpdate(), dn2.getLastUpdate());
      assertEquals(dn1.getLevel(), dn2.getLevel());
      assertEquals(dn1.getNetworkLocation(), dn2.getNetworkLocation());
  }
  
  @Test
  public void testConvertExtendedBlock() {
    ExtendedBlock b = getExtendedBlock();
    ExtendedBlockProto bProto = PBHelper.convert(b);
    ExtendedBlock b1 = PBHelper.convert(bProto);
    assertEquals(b, b1);
    
    b.setBlockId(-1);
    bProto = PBHelper.convert(b);
    b1 = PBHelper.convert(bProto);
    assertEquals(b, b1);
  }
  
  @Test
  public void testConvertRecoveringBlock() {
    DatanodeInfo di1 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo di2 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] dnInfo = new DatanodeInfo[] { di1, di2 };
    RecoveringBlock b = new RecoveringBlock(getExtendedBlock(), dnInfo, 3);
    RecoveringBlockProto bProto = PBHelper.convert(b);
    RecoveringBlock b1 = PBHelper.convert(bProto);
    assertEquals(b.getBlock(), b1.getBlock());
    DatanodeInfo[] dnInfo1 = b1.getLocations();
    assertEquals(dnInfo.length, dnInfo1.length);
    for (int i=0; i < dnInfo.length; i++) {
      compare(dnInfo[0], dnInfo1[0]);
    }
  }
  
  @Test
  public void testConvertBlockRecoveryCommand() {
    DatanodeInfo di1 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo di2 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] dnInfo = new DatanodeInfo[] { di1, di2 };

    List<RecoveringBlock> blks = ImmutableList.of(
      new RecoveringBlock(getExtendedBlock(1), dnInfo, 3),
      new RecoveringBlock(getExtendedBlock(2), dnInfo, 3)
    );
    
    BlockRecoveryCommand cmd = new BlockRecoveryCommand(blks);
    BlockRecoveryCommandProto proto = PBHelper.convert(cmd);
    assertEquals(1, proto.getBlocks(0).getBlock().getB().getBlockId());
    assertEquals(2, proto.getBlocks(1).getBlock().getB().getBlockId());
    
    BlockRecoveryCommand cmd2 = PBHelper.convert(proto);
    
    List<RecoveringBlock> cmd2Blks = Lists.newArrayList(
        cmd2.getRecoveringBlocks());
    assertEquals(blks.get(0).getBlock(), cmd2Blks.get(0).getBlock());
    assertEquals(blks.get(1).getBlock(), cmd2Blks.get(1).getBlock());
    assertEquals(Joiner.on(",").join(blks), Joiner.on(",").join(cmd2Blks));
    assertEquals(cmd.toString(), cmd2.toString());
  }
  
  
  @Test
  public void testConvertText() {
    Text t = new Text("abc".getBytes());
    String s = t.toString();
    Text t1 = new Text(s);
    assertEquals(t, t1);
  }
  
  @Test
  public void testConvertBlockToken() {
    Token<BlockTokenIdentifier> token = new Token<BlockTokenIdentifier>(
        "identifier".getBytes(), "password".getBytes(), new Text("kind"),
        new Text("service"));
    BlockTokenIdentifierProto tokenProto = PBHelper.convert(token);
    Token<BlockTokenIdentifier> token2 = PBHelper.convert(tokenProto);
    compare(token, token2);
  }
  
  @Test
  public void testConvertNamespaceInfo() {
    NamespaceInfo info = new NamespaceInfo(37, "clusterID", "bpID", 2300);
    NamespaceInfoProto proto = PBHelper.convert(info);
    NamespaceInfo info2 = PBHelper.convert(proto);
    compare(info, info2); //Compare the StorageInfo
    assertEquals(info.getBlockPoolID(), info2.getBlockPoolID());
    assertEquals(info.getBuildVersion(), info2.getBuildVersion());
  }

  private void compare(StorageInfo expected, StorageInfo actual) {
    assertEquals(expected.clusterID, actual.clusterID);
    assertEquals(expected.namespaceID, actual.namespaceID);
    assertEquals(expected.cTime, actual.cTime);
    assertEquals(expected.layoutVersion, actual.layoutVersion);
  }

  private void compare(Token<BlockTokenIdentifier> expected,
      Token<BlockTokenIdentifier> actual) {
    assertTrue(Arrays.equals(expected.getIdentifier(), actual.getIdentifier()));
    assertTrue(Arrays.equals(expected.getPassword(), actual.getPassword()));
    assertEquals(expected.getKind(), actual.getKind());
    assertEquals(expected.getService(), actual.getService());
  }
  
  @Test
  public void testConvertLocatedBlock() {
    DatanodeInfo [] dnInfos = {
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h1", AdminStates.DECOMMISSION_INPROGRESS),
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h2", AdminStates.DECOMMISSIONED),
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h3", AdminStates.NORMAL)
    };
    LocatedBlock lb = new LocatedBlock(
        new ExtendedBlock("bp12", 12345, 10, 53), dnInfos, 5, false);
    LocatedBlockProto lbProto = PBHelper.convert(lb);
    LocatedBlock lb2 = PBHelper.convert(lbProto);
    assertEquals(lb.getBlock(), lb2.getBlock());
    compare(lb.getBlockToken(), lb2.getBlockToken());
    assertEquals(lb.getStartOffset(), lb2.getStartOffset());
    assertEquals(lb.isCorrupt(), lb2.isCorrupt());
    DatanodeInfo [] dnInfos2 = lb2.getLocations();
    assertEquals(dnInfos.length, dnInfos2.length);
    for (int i = 0; i < dnInfos.length ; i++) {
      compare(dnInfos[i], dnInfos2[i]);
    }
  }
  
  @Test
  public void testConvertDatanodeRegistration() {
    DatanodeID dnId = DFSTestUtil.getLocalDatanodeID();
    BlockKey[] keys = new BlockKey[] { getBlockKey(2), getBlockKey(3) };
    ExportedBlockKeys expKeys = new ExportedBlockKeys(true, 9, 10,
        getBlockKey(1), keys);
    DatanodeRegistration reg = new DatanodeRegistration(dnId,
        new StorageInfo(), expKeys, "3.0.0");
    DatanodeRegistrationProto proto = PBHelper.convert(reg);
    DatanodeRegistration reg2 = PBHelper.convert(proto);
    compare(reg.getStorageInfo(), reg2.getStorageInfo());
    compare(reg.getExportedKeys(), reg2.getExportedKeys());
    compare(reg, reg2);
    assertEquals(reg.getSoftwareVersion(), reg2.getSoftwareVersion());
  }
  
  @Test
  public void testConvertBlockCommand() {
    Block[] blocks = new Block[] { new Block(21), new Block(22) };
    DatanodeInfo[][] dnInfos = new DatanodeInfo[][] { new DatanodeInfo[1],
        new DatanodeInfo[2] };
    dnInfos[0][0] = DFSTestUtil.getLocalDatanodeInfo();
    dnInfos[1][0] = DFSTestUtil.getLocalDatanodeInfo();
    dnInfos[1][1] = DFSTestUtil.getLocalDatanodeInfo();
    BlockCommand bc = new BlockCommand(DatanodeProtocol.DNA_TRANSFER, "bp1",
        blocks, dnInfos);
    BlockCommandProto bcProto = PBHelper.convert(bc);
    BlockCommand bc2 = PBHelper.convert(bcProto);
    assertEquals(bc.getAction(), bc2.getAction());
    assertEquals(bc.getBlocks().length, bc2.getBlocks().length);
    Block[] blocks2 = bc2.getBlocks();
    for (int i = 0; i < blocks.length; i++) {
      assertEquals(blocks[i], blocks2[i]);
    }
    DatanodeInfo[][] dnInfos2 = bc2.getTargets();
    assertEquals(dnInfos.length, dnInfos2.length);
    for (int i = 0; i < dnInfos.length; i++) {
      DatanodeInfo[] d1 = dnInfos[i];
      DatanodeInfo[] d2 = dnInfos2[i];
      assertEquals(d1.length, d2.length);
      for (int j = 0; j < d1.length; j++) {
        compare(d1[j], d2[j]);
      }
    }
  }
}

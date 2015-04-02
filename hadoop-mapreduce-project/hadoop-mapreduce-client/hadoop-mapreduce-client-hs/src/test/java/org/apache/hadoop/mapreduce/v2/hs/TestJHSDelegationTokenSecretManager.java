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
package org.apache.hadoop.mapreduce.v2.hs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.junit.Test;

public class TestJHSDelegationTokenSecretManager {

  @Test
  public void testRecovery() throws IOException {
    Configuration conf = new Configuration();
    HistoryServerStateStoreService store =
        new HistoryServerMemStateStoreService();
    store.init(conf);
    store.start();
    JHSDelegationTokenSecretManagerForTest mgr =
        new JHSDelegationTokenSecretManagerForTest(store);
    mgr.startThreads();

    MRDelegationTokenIdentifier tokenId1 = new MRDelegationTokenIdentifier(
        new Text("tokenOwner"), new Text("tokenRenewer"),
        new Text("tokenUser"));
    Token<MRDelegationTokenIdentifier> token1 =
        new Token<MRDelegationTokenIdentifier>(tokenId1, mgr);

    MRDelegationTokenIdentifier tokenId2 = new MRDelegationTokenIdentifier(
        new Text("tokenOwner"), new Text("tokenRenewer"),
        new Text("tokenUser"));
    Token<MRDelegationTokenIdentifier> token2 =
        new Token<MRDelegationTokenIdentifier>(tokenId2, mgr);
    DelegationKey[] keys = mgr.getAllKeys();
    long tokenRenewDate1 = mgr.getAllTokens().get(tokenId1).getRenewDate();
    long tokenRenewDate2 = mgr.getAllTokens().get(tokenId2).getRenewDate();
    mgr.stopThreads();

    mgr = new JHSDelegationTokenSecretManagerForTest(store);
    mgr.recover(store.loadState());
    List<DelegationKey> recoveredKeys = Arrays.asList(mgr.getAllKeys());
    for (DelegationKey key : keys) {
      assertTrue("key missing after recovery", recoveredKeys.contains(key));
    }
    assertTrue("token1 missing", mgr.getAllTokens().containsKey(tokenId1));
    assertEquals("token1 renew date", tokenRenewDate1,
        mgr.getAllTokens().get(tokenId1).getRenewDate());
    assertTrue("token2 missing", mgr.getAllTokens().containsKey(tokenId2));
    assertEquals("token2 renew date", tokenRenewDate2,
        mgr.getAllTokens().get(tokenId2).getRenewDate());

    mgr.startThreads();
    mgr.verifyToken(tokenId1, token1.getPassword());
    mgr.verifyToken(tokenId2, token2.getPassword());
    MRDelegationTokenIdentifier tokenId3 = new MRDelegationTokenIdentifier(
        new Text("tokenOwner"), new Text("tokenRenewer"),
        new Text("tokenUser"));
    Token<MRDelegationTokenIdentifier> token3 =
        new Token<MRDelegationTokenIdentifier>(tokenId3, mgr);
    assertEquals("sequence number restore", tokenId2.getSequenceNumber() + 1,
        tokenId3.getSequenceNumber());
    mgr.cancelToken(token1, "tokenOwner");
    long tokenRenewDate3 = mgr.getAllTokens().get(tokenId3).getRenewDate();
    mgr.stopThreads();

    mgr = new JHSDelegationTokenSecretManagerForTest(store);
    mgr.recover(store.loadState());
    assertFalse("token1 should be missing",
        mgr.getAllTokens().containsKey(tokenId1));
    assertTrue("token2 missing", mgr.getAllTokens().containsKey(tokenId2));
    assertEquals("token2 renew date", tokenRenewDate2,
        mgr.getAllTokens().get(tokenId2).getRenewDate());
    assertTrue("token3 missing", mgr.getAllTokens().containsKey(tokenId3));
    assertEquals("token3 renew date", tokenRenewDate3,
        mgr.getAllTokens().get(tokenId3).getRenewDate());

    mgr.startThreads();
    mgr.verifyToken(tokenId2, token2.getPassword());
    mgr.verifyToken(tokenId3, token3.getPassword());
    mgr.stopThreads();
 }

  private static class JHSDelegationTokenSecretManagerForTest
      extends JHSDelegationTokenSecretManager {

    public JHSDelegationTokenSecretManagerForTest(
        HistoryServerStateStoreService store) {
      super(10000, 10000, 10000, 10000, store);
    }

    public Map<MRDelegationTokenIdentifier, DelegationTokenInformation> getAllTokens() {
      return new HashMap<MRDelegationTokenIdentifier, DelegationTokenInformation>(currentTokens);
    }
  }
}

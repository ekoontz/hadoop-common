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

package org.apache.hadoop.hdfs.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Assert;
import org.junit.Test;

public class TestWebHdfsUrl {

  @Test
  public void testDelegationTokenInUrl() throws IOException {
    Configuration conf = new Configuration();
    final String uri = WebHdfsFileSystem.SCHEME + "://" + "127.0.0.1:9071";
    // Turn on security
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
        ugi.getUserName()), null, null);
    FSNamesystem namesystem = mock(FSNamesystem.class);
    DelegationTokenSecretManager dtSecretManager = new DelegationTokenSecretManager(
        86400000, 86400000, 86400000, 86400000, namesystem);
    dtSecretManager.startThreads();
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
        dtId, dtSecretManager);
    token.setService(new Text("127.0.0.1:9071"));
    token.setKind(WebHdfsFileSystem.TOKEN_KIND);
    ugi.addToken(token);
    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(
        URI.create(uri), conf);
    String tokenString = token.encodeToUrlString();
    Path fsPath = new Path("/");
    URL renewTokenUrl = webhdfs.toUrl(PutOpParam.Op.RENEWDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    Assert.assertEquals(
        generateUrlQueryPrefix(PutOpParam.Op.RENEWDELEGATIONTOKEN,
            ugi.getUserName())
            + "&token=" + tokenString, renewTokenUrl.getQuery());
    Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>(
        token);
    delegationToken.setKind(WebHdfsFileSystem.TOKEN_KIND);
    Assert.assertEquals(
        generateUrlQueryPrefix(PutOpParam.Op.CANCELDELEGATIONTOKEN,
            ugi.getUserName())
            + "&token="
            + tokenString
            + "&"
            + DelegationParam.NAME
            + "="
            + delegationToken.encodeToUrlString(), cancelTokenUrl.getQuery());
  }

  private String generateUrlQueryPrefix(HttpOpParam.Op op, String username) {
    return "op=" + op.toString() + "&user.name=" + username;
  }
  
  @Test
  public void testSelectHdfsDelegationToken() throws Exception {
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    Configuration conf = new Configuration();
    conf.setClass("fs.webhdfs.impl", MyWebHdfsFileSystem.class, FileSystem.class);
    
    // test with implicit default port 
    URI fsUri = URI.create("webhdfs://localhost");
    MyWebHdfsFileSystem fs = (MyWebHdfsFileSystem) FileSystem.get(fsUri, conf);
    checkTokenSelection(fs, conf);

    // test with explicit default port
    fsUri = URI.create("webhdfs://localhost:"+fs.getDefaultPort());
    fs = (MyWebHdfsFileSystem) FileSystem.get(fsUri, conf);
    checkTokenSelection(fs, conf);
    
    // test with non-default port
    fsUri = URI.create("webhdfs://localhost:"+(fs.getDefaultPort()-1));
    fs = (MyWebHdfsFileSystem) FileSystem.get(fsUri, conf);
    checkTokenSelection(fs, conf);

  }
  
  private void checkTokenSelection(MyWebHdfsFileSystem fs,
                                   Configuration conf) throws IOException {
    int port = fs.getCanonicalUri().getPort();
    // can't clear tokens from ugi, so create a new user everytime
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting(fs.getUri().getAuthority(), new String[]{});

    // use ip-based tokens
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    // test fallback to hdfs token
    Token<?> hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("127.0.0.1:8020"));
    ugi.addToken(hdfsToken);

    // test fallback to hdfs token
    Token<?> token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hdfsToken, token);

    // test webhdfs is favored over hdfs
    Token<?> webHdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        WebHdfsFileSystem.TOKEN_KIND, new Text("127.0.0.1:"+port));
    ugi.addToken(webHdfsToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(webHdfsToken, token);
    
    // switch to using host-based tokens, no token should match
    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    token = fs.selectDelegationToken(ugi);
    assertNull(token);
    
    // test fallback to hdfs token
    hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("localhost:8020"));
    ugi.addToken(hdfsToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hdfsToken, token);

    // test webhdfs is favored over hdfs
    webHdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        WebHdfsFileSystem.TOKEN_KIND, new Text("localhost:"+port));
    ugi.addToken(webHdfsToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(webHdfsToken, token);
  }
  
  static class MyWebHdfsFileSystem extends WebHdfsFileSystem {
    @Override
    public URI getCanonicalUri() {
      return super.getCanonicalUri();
    }
    @Override
    public int getDefaultPort() {
      return super.getDefaultPort();
    }
    // don't automatically get a token
    @Override
    protected void initDelegationToken() throws IOException {}
  }
}
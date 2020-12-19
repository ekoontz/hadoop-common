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

package org.apache.hadoop.security;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.SaslStatus;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A utility class that encapsulates SASL logic for RPC client
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcClient {
  public static final Log LOG = LogFactory.getLog(SaslRpcClient.class);

  private final SaslClient saslClient;

  /**
   * Create a SaslRpcClient for an authentication method
   * 
   * @param method
   *          the requested authentication method
   * @param token
   *          token to use if needed by the authentication method
   */
  public SaslRpcClient(AuthMethod method,
      Token<? extends TokenIdentifier> token, String serverPrincipal)
      throws IOException {
    String saslUser = null;
    String saslProtocol = null;
    String saslServerName = null;
    Map<String, String> saslProperties = SaslRpcServer.SASL_PROPS;
    CallbackHandler saslCallback = null;
    
    switch (method) {
      case DIGEST: {
        saslServerName = SaslRpcServer.SASL_DEFAULT_REALM;
        saslCallback = new SaslClientCallbackHandler(token);
        break;
      }
      case KERBEROS: {
        if (serverPrincipal == null || serverPrincipal.isEmpty()) {
          throw new IOException(
              "Failed to specify server's Kerberos principal name");
        }
        KerberosName name = new KerberosName(serverPrincipal);
        saslProtocol = name.getServiceName();
        saslServerName = name.getHostName();
        if (saslServerName == null) {
          throw new IOException(
              "Kerberos principal name does NOT have the expected hostname part: "
                  + serverPrincipal);
        }
        break;
      }
      default:
        throw new IOException("Unknown authentication method " + method);
    }
    
    String mechanism = method.getMechanismName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating SASL " + mechanism
          + " client to authenticate to service at " + saslServerName);
    }
    saslClient = Sasl.createSaslClient(
        new String[] { mechanism }, saslUser, saslProtocol, saslServerName,
        saslProperties, saslCallback);
    if (saslClient == null) {
      throw new IOException("Unable to find SASL client implementation");
    }
  }

  private static void readStatus(DataInputStream inStream) throws IOException {
    int status = inStream.readInt(); // read status
    if (status != SaslStatus.SUCCESS.state) {
      throw new RemoteException(WritableUtils.readString(inStream),
          WritableUtils.readString(inStream));
    }
  }
  
  /**
   * Do client side SASL authentication with server via the given InputStream
   * and OutputStream
   * 
   * @param inS
   *          InputStream to use
   * @param outS
   *          OutputStream to use
   * @return true if connection is set up, or false if needs to switch 
   *             to simple Auth.
   * @throws IOException
   */
  public boolean saslConnect(InputStream inS, OutputStream outS)
      throws IOException {
    DataInputStream inStream = new DataInputStream(new BufferedInputStream(inS));
    DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(
        outS));

    try {
      byte[] saslToken = new byte[0];
      if (saslClient.hasInitialResponse())
        saslToken = saslClient.evaluateChallenge(saslToken);
      while (saslToken != null) {
        outStream.writeInt(saslToken.length);
        outStream.write(saslToken, 0, saslToken.length);
        outStream.flush();
        if (LOG.isDebugEnabled())
          LOG.debug("Have sent token of size " + saslToken.length
              + " from initSASLContext.");
        readStatus(inStream);
        int len = inStream.readInt();
        if (len == SaslRpcServer.SWITCH_TO_SIMPLE_AUTH) {
          if (LOG.isDebugEnabled())
            LOG.debug("Server asks us to fall back to simple auth.");
          saslClient.dispose();
          return false;
        } else if ((len == 0) && saslClient.isComplete()) {
          break;
        }
        saslToken = new byte[len];
        if (LOG.isDebugEnabled())
          LOG.debug("Will read input token of size " + saslToken.length
              + " for processing by initSASLContext");
        inStream.readFully(saslToken);
        saslToken = saslClient.evaluateChallenge(saslToken);
      }
      if (!saslClient.isComplete()) { // shouldn't happen
        throw new SaslException("Internal negotiation error");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client context established. Negotiated QoP: "
            + saslClient.getNegotiatedProperty(Sasl.QOP));
      }
      return true;
    } catch (IOException e) {
      try {
        saslClient.dispose();
      } catch (SaslException ignored) {
        // ignore further exceptions during cleanup
      }
      throw e;
    }
  }

  /**
   * Get a SASL wrapped InputStream. Can be called only after saslConnect() has
   * been called.
   * 
   * @param in
   *          the InputStream to wrap
   * @return a SASL wrapped InputStream
   * @throws IOException
   */
  public InputStream getInputStream(InputStream in) throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    return new SaslInputStream(in, saslClient);
  }

  /**
   * Get a SASL wrapped OutputStream. Can be called only after saslConnect() has
   * been called.
   * 
   * @param out
   *          the OutputStream to wrap
   * @return a SASL wrapped OutputStream
   * @throws IOException
   */
  public OutputStream getOutputStream(OutputStream out) throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    return new SaslOutputStream(out, saslClient);
  }

  /** Release resources used by wrapped saslClient */
  public void dispose() throws SaslException {
    saslClient.dispose();
  }

  private static class SaslClientCallbackHandler implements CallbackHandler {
    private final String userName;
    private final char[] userPassword;

    public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = SaslRpcServer.encodeIdentifier(token.getIdentifier());
      this.userPassword = SaslRpcServer.encodePassword(token.getPassword());
    }

    @Override
    public void handle(Callback[] callbacks)
        throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting username: " + userName);
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting userPassword");
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled())
          LOG.debug("SASL client callback: setting realm: "
              + rc.getDefaultText());
        rc.setText(rc.getDefaultText());
      }
    }
  }
}

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

package org.apache.hadoop.yarn.security.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

@Public
@Evolving
public class ClientToAMTokenIdentifier extends TokenIdentifier {

  public static final Text KIND_NAME = new Text("YARN_CLIENT_TOKEN");

  private ApplicationAttemptId applicationAttemptId;
  private Text clientName = new Text();

  // TODO: Add more information in the tokenID such that it is not
  // transferrable, more secure etc.

  public ClientToAMTokenIdentifier() {
  }

  public ClientToAMTokenIdentifier(ApplicationAttemptId id, String client) {
    this();
    this.applicationAttemptId = id;
    this.clientName = new Text(client);
  }

  public ApplicationAttemptId getApplicationAttemptID() {
    return this.applicationAttemptId;
  }

  public String getClientName() {
    return this.clientName.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.applicationAttemptId.getApplicationId()
      .getClusterTimestamp());
    out.writeInt(this.applicationAttemptId.getApplicationId().getId());
    out.writeInt(this.applicationAttemptId.getAttemptId());
    this.clientName.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.applicationAttemptId =
        ApplicationAttemptId.newInstance(
          ApplicationId.newInstance(in.readLong(), in.readInt()), in.readInt());
    this.clientName.readFields(in);
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    if (this.clientName == null) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(this.clientName.toString());
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}

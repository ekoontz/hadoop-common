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

package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.   
 */
//@InterfaceStability.Evolving
public class TokenCache {
  
  private static final Log LOG = LogFactory.getLog(TokenCache.class);

  static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = 
    "mapreduce.job.credentials.binary";

  /**
   * auxiliary method to get user's secret keys..
   * @param alias
   * @return secret key from the storage
   */
  public static byte[] getSecretKey(Credentials credentials, Text alias) {
    if(credentials == null)
      return null;
    return credentials.getSecretKey(alias);
  }

  /**
   * Convenience method to obtain delegation tokens from namenodes 
   * corresponding to the paths passed.
   * @param ps array of paths
   * @param conf configuration
   * @throws IOException
   */
  public static void obtainTokensForNamenodes(Credentials credentials,
                                              Path [] ps, Configuration conf) 
  throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForNamenodesInternal(credentials, ps, conf);
  }

  /**
   * Remove jobtoken referrals which don't make sense in the context
   * of the task execution.
   *
   * @param conf configuration object.
   */
  public static void cleanUpTokenReferral(Configuration conf) {
    conf.unset(MAPREDUCE_JOB_CREDENTIALS_BINARY);
  }

  /**
   * Obtain delegation tokens for the filesystems pointed to by the
   * given paths.  Filesystems may have multiple tokens, so we cannot
   * just check to see whether a token is available already, so we call
   * addDelegationTokens on each one.
   */
  static void obtainTokensForNamenodesInternal(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    Set<FileSystem> fsSet = new HashSet<FileSystem>();
    for(Path p: ps) {
      fsSet.add(p.getFileSystem(conf));
    }
    for (FileSystem fs : fsSet) {
      obtainTokensForNamenodesInternal(fs, credentials, conf);
    }
  }

  /**
   * get delegation tokens for a specific FS
   * @param fs
   * @param credentials
   * @param p
   * @param conf
   * @throws IOException
   */
  private static void obtainTokensForNamenodesInternal(FileSystem fs,
      Credentials credentials, Configuration conf) throws IOException {
    HadoopKerberosName jtKrbName = new HadoopKerberosName(conf.get(JobTracker.JT_USER_NAME, ""));
    String delegTokenRenewer = jtKrbName.getShortName();

    mergeBinaryTokens(credentials, conf);

    final Token<?> tokens[] = fs.addDelegationTokens(delegTokenRenewer,
                                                     credentials);
    if (tokens != null) {
      for (Token<?> token : tokens) {
        LOG.info("Got dt for " + fs.getUri() + "; "+token);
      }
    }
  }

  private static void mergeBinaryTokens(Credentials creds, Configuration conf) {
    String binaryTokenFilename =
        conf.get(MAPREDUCE_JOB_CREDENTIALS_BINARY);
    if (binaryTokenFilename != null) {
      Credentials binary;
      try {
        binary = Credentials.readTokenStorageFile(
            FileSystem.getLocal(conf).makeQualified(
                new Path(binaryTokenFilename)),
            conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // supplement existing tokens with the tokens in the binary file
      creds.mergeAll(binary);
    }
  }

  /**
   * file name used on HDFS for generated job token
   */
  //@InterfaceAudience.Private
  public static final String JOB_TOKEN_HDFS_FILE = "jobToken";

  /**
   * conf setting for job tokens cache file name
   */
  //@InterfaceAudience.Private
  public static final String JOB_TOKENS_FILENAME = "mapreduce.job.jobTokenFile";
  private static final Text JOB_TOKEN = new Text("ShuffleAndJobToken");

  /**
   * load job token from a file
   * @param conf
   * @throws IOException
   */
  //@InterfaceAudience.Private
  public static Credentials loadTokens(String jobTokenFile, Configuration conf) 
  throws IOException {
    Path localJobTokenFile = new Path ("file:///" + jobTokenFile);
    
    Credentials ts = 
      Credentials.readTokenStorageFile(localJobTokenFile, conf);

    if(LOG.isDebugEnabled()) {
      LOG.debug("Task: Loaded jobTokenFile from: "+
          localJobTokenFile.toUri().getPath() 
        +"; num of sec keys  = " + ts.numberOfSecretKeys() +
        " Number of tokens " + 
        ts.numberOfTokens());
    }
    return ts;
  }

  /**
   * store job token
   * @param t
   */
  //@InterfaceAudience.Private
  public static void setJobToken(Token<? extends TokenIdentifier> t, 
      Credentials credentials) {
    credentials.addToken(JOB_TOKEN, t);
  }
  /**
   * 
   * @return job token
   */
  //@InterfaceAudience.Private
  @SuppressWarnings("unchecked")
  public static Token<JobTokenIdentifier> getJobToken(Credentials credentials) {
    return (Token<JobTokenIdentifier>) credentials.getToken(JOB_TOKEN);
  }
}

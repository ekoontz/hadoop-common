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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>ApplicationReport</code> is a report of an application.</p>
 *
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ApplicationId} of the application.</li>
 *     <li>Applications user.</li>
 *     <li>Application queue.</li>
 *     <li>Application name.</li>
 *     <li>Host on which the <code>ApplicationMaster</code> is running.</li>
 *     <li>RPC port of the <code>ApplicationMaster</code>.</li>
 *     <li>Tracking URL.</li>
 *     <li>{@link YarnApplicationState} of the application.</li>
 *     <li>Diagnostic information in case of errors.</li>
 *     <li>Start time of the application.</li>
 *     <li>Client {@link Token} of the application (if security is enabled).</li>
 *   </ul>
 * </p>
 *
 * @see ApplicationClientProtocol#getApplicationReport(org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest)
 */
@Public
@Stable
public abstract class ApplicationReport {

  @Private
  @Unstable
  public static ApplicationReport newInstance(ApplicationId applicationId,
      ApplicationAttemptId applicationAttemptId, String user, String queue,
      String name, String host, int rpcPort, Token clientToAMToken,
      YarnApplicationState state, String diagnostics, String url,
      long startTime, long finishTime, FinalApplicationStatus finalStatus,
      ApplicationResourceUsageReport appResources, String origTrackingUrl,
      float progress, String applicationType, Token amRmToken) {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(applicationId);
    report.setCurrentApplicationAttemptId(applicationAttemptId);
    report.setUser(user);
    report.setQueue(queue);
    report.setName(name);
    report.setHost(host);
    report.setRpcPort(rpcPort);
    report.setClientToAMToken(clientToAMToken);
    report.setYarnApplicationState(state);
    report.setDiagnostics(diagnostics);
    report.setTrackingUrl(url);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    report.setFinalApplicationStatus(finalStatus);
    report.setApplicationResourceUsageReport(appResources);
    report.setOriginalTrackingUrl(origTrackingUrl);
    report.setProgress(progress);
    report.setApplicationType(applicationType);
    report.setAMRMToken(amRmToken);
    return report;
  }

  /**
   * Get the <code>ApplicationId</code> of the application.
   * @return <code>ApplicationId</code> of the application
   */
  @Public
  @Stable
  public abstract ApplicationId getApplicationId();

  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);
  
  /**
   * Get the <code>ApplicationAttemptId</code> of the current
   * attempt of the application
   * @return <code>ApplicationAttemptId</code> of the attempt
   */
  @Public
  @Stable
  public abstract ApplicationAttemptId getCurrentApplicationAttemptId();
  
  @Private
  @Unstable
  public abstract void setCurrentApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  /**
   * Get the <em>user</em> who submitted the application.
   * @return <em>user</em> who submitted the application
   */
  @Public
  @Stable
  public abstract String getUser();

  @Private
  @Unstable
  public abstract void setUser(String user);

  /**
   * Get the <em>queue</em> to which the application was submitted.
   * @return <em>queue</em> to which the application was submitted
   */
  @Public
  @Stable
  public abstract String getQueue();

  @Private
  @Unstable
  public abstract void setQueue(String queue);

  /**
   * Get the user-defined <em>name</em> of the application.
   * @return <em>name</em> of the application
   */
  @Public
  @Stable
  public abstract String getName();

  @Private
  @Unstable
  public abstract void setName(String name);

  /**
   * Get the <em>host</em> on which the <code>ApplicationMaster</code>
   * is running.
   * @return <em>host</em> on which the <code>ApplicationMaster</code>
   *         is running
   */
  @Public
  @Stable
  public abstract String getHost();

  @Private
  @Unstable
  public abstract void setHost(String host);

  /**
   * Get the <em>RPC port</em> of the <code>ApplicationMaster</code>.
   * @return <em>RPC port</em> of the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract int getRpcPort();

  @Private
  @Unstable
  public abstract void setRpcPort(int rpcPort);

  /**
   * Get the <em>client token</em> for communicating with the
   * <code>ApplicationMaster</code>.
   * <p>
   * <em>ClientToAMToken</em> is the security token used by the AMs to verify
   * authenticity of any <code>client</code>.
   * </p>
   *
   * <p>
   * The <code>ResourceManager</code>, provides a secure token (via
   * {@link ApplicationReport#getClientToAMToken()}) which is verified by the
   * ApplicationMaster when the client directly talks to an AM.
   * </p>
   * @return <em>client token</em> for communicating with the
   * <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract Token getClientToAMToken();

  @Private
  @Unstable
  public abstract void setClientToAMToken(Token clientToAMToken);

  /**
   * Get the <code>YarnApplicationState</code> of the application.
   * @return <code>YarnApplicationState</code> of the application
   */
  @Public
  @Stable
  public abstract YarnApplicationState getYarnApplicationState();

  @Private
  @Unstable
  public abstract void setYarnApplicationState(YarnApplicationState state);

  /**
   * Get  the <em>diagnositic information</em> of the application in case of
   * errors.
   * @return <em>diagnositic information</em> of the application in case
   *         of errors
   */
  @Public
  @Stable
  public abstract String getDiagnostics();

  @Private
  @Unstable
  public abstract void setDiagnostics(String diagnostics);

  /**
   * Get the <em>tracking url</em> for the application.
   * @return <em>tracking url</em> for the application
   */
  @Public
  @Stable
  public abstract String getTrackingUrl();

  @Private
  @Unstable
  public abstract void setTrackingUrl(String url);
  
  /**
   * Get the original not-proxied <em>tracking url</em> for the application.
   * This is intended to only be used by the proxy itself.
   * @return the original not-proxied <em>tracking url</em> for the application
   */
  @Private
  @Unstable
  public abstract String getOriginalTrackingUrl();

  @Private
  @Unstable
  public abstract void setOriginalTrackingUrl(String url);

  /**
   * Get the <em>start time</em> of the application.
   * @return <em>start time</em> of the application
   */
  @Public
  @Stable
  public abstract long getStartTime();

  @Private
  @Unstable
  public abstract void setStartTime(long startTime);

  /**
   * Get the <em>finish time</em> of the application.
   * @return <em>finish time</em> of the application
   */
  @Public
  @Stable
  public abstract long getFinishTime();

  @Private
  @Unstable
  public abstract void setFinishTime(long finishTime);


  /**
   * Get the <em>final finish status</em> of the application.
   * @return <em>final finish status</em> of the application
   */
  @Public
  @Stable
  public abstract FinalApplicationStatus getFinalApplicationStatus();

  @Private
  @Unstable
  public abstract void setFinalApplicationStatus(FinalApplicationStatus finishState);

  /**
   * Retrieve the structure containing the job resources for this application
   * @return the job resources structure for this application
   */
  @Public
  @Stable
  public abstract ApplicationResourceUsageReport getApplicationResourceUsageReport();

  /**
   * Store the structure containing the job resources for this application
   * @param appResources structure for this application
   */
  @Private
  @Unstable
  public abstract void setApplicationResourceUsageReport(ApplicationResourceUsageReport appResources);

  /**
   * Get the application's progress ( range 0.0 to 1.0 )
   * @return application's progress
   */
  @Public
  @Stable
  public abstract float getProgress();

  @Private
  @Unstable
  public abstract void setProgress(float progress);
  
  /**
   * Get the application's Type 
   * @return application's Type
   */
  @Public
  @Stable
  public abstract String getApplicationType();

  @Private
  @Unstable
  public abstract void setApplicationType(String applicationType);

  @Private
  @Stable
  public abstract void setAMRMToken(Token amRmToken);

  /**
   * Get the AMRM token of the application.
   * <p/>
   * The AMRM token is required for AM to RM scheduling operations. For 
   * managed Application Masters Yarn takes care of injecting it. For unmanaged
   * Applications Masters, the token must be obtained via this method and set
   * in the {@link org.apache.hadoop.security.UserGroupInformation} of the
   * current user.
   * <p/>
   * The AMRM token will be returned only if all the following conditions are
   * met:
   * <li>
   *   <ul>the requester is the owner of the ApplicationMaster</ul>
   *   <ul>the application master is an unmanaged ApplicationMaster</ul>
   *   <ul>the application master is in ACCEPTED state</ul>
   * </li>
   * Else this method returns NULL.
   * 
   * @return the AM to RM token if available.
   */
  @Public
  @Stable
  public abstract Token getAMRMToken();
  
}

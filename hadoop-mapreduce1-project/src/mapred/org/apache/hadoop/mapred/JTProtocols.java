package org.apache.hadoop.mapred;

import org.apache.hadoop.mr1security.RefreshUserMappingsProtocol;
import org.apache.hadoop.mr1security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.mr1tools.GetUserMappingsProtocol;

public interface JTProtocols extends
  InterTrackerProtocol,
  JobSubmissionProtocol, TaskTrackerManager, RefreshUserMappingsProtocol,
  GetUserMappingsProtocol,
  RefreshAuthorizationPolicyProtocol, AdminOperationsProtocol
{

}

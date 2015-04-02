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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * Render a block of HTML for a give job.
 */
public class HsJobBlock extends HtmlBlock {
  final AppContext appContext;

  @Inject HsJobBlock(AppContext appctx) {
    appContext = appctx;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  @Override protected void render(Block html) {
    String jid = $(JOB_ID);
    if (jid.isEmpty()) {
      html.
        p()._("Sorry, can't do anything without a JobID.")._();
      return;
    }
    JobId jobID = MRApps.toJobID(jid);
    Job j = appContext.getJob(jobID);
    if (j == null) {
      html.
        p()._("Sorry, ", jid, " not found.")._();
      return;
    }
    List<AMInfo> amInfos = j.getAMInfos();
    JobInfo job = new JobInfo(j);
    ResponseInfo infoBlock = info("Job Overview").
        _("Job Name:", job.getName()).
        _("User Name:", job.getUserName()).
        _("Queue:", job.getQueueName()).
        _("State:", job.getState()).
        _("Uberized:", job.isUber()).
        _("Submitted:", new Date(job.getSubmitTime())).
        _("Started:", new Date(job.getStartTime())).
        _("Finished:", new Date(job.getFinishTime())).
        _("Elapsed:", StringUtils.formatTime(
            Times.elapsed(job.getStartTime(), job.getFinishTime(), false)));
    
    String amString =
        amInfos.size() == 1 ? "ApplicationMaster" : "ApplicationMasters"; 
    
    // todo - switch to use JobInfo
    List<String> diagnostics = j.getDiagnostics();
    if(diagnostics != null && !diagnostics.isEmpty()) {
      StringBuffer b = new StringBuffer();
      for(String diag: diagnostics) {
        b.append(diag);
      }
      infoBlock._("Diagnostics:", b.toString());
    }

    if(job.getNumMaps() > 0) {
      infoBlock._("Average Map Time", StringUtils.formatTime(job.getAvgMapTime()));
    }
    if(job.getNumReduces() > 0) {
      infoBlock._("Average Reduce Time", StringUtils.formatTime(job.getAvgReduceTime()));
      infoBlock._("Average Shuffle Time", StringUtils.formatTime(job.getAvgShuffleTime()));
      infoBlock._("Average Merge Time", StringUtils.formatTime(job.getAvgMergeTime()));
    }

    for (ConfEntryInfo entry : job.getAcls()) {
      infoBlock._("ACL "+entry.getName()+":", entry.getValue());
    }
    DIV<Hamlet> div = html.
      _(InfoBlock.class).
      div(_INFO_WRAP);
    
      // MRAppMasters Table
        TABLE<DIV<Hamlet>> table = div.table("#job");
        table.
          tr().
            th(amString).
          _().
          tr().
            th(_TH, "Attempt Number").
            th(_TH, "Start Time").
            th(_TH, "Node").
            th(_TH, "Logs").
            _();
        boolean odd = false;
          for (AMInfo amInfo : amInfos) {
            AMAttemptInfo attempt = new AMAttemptInfo(amInfo,
                job.getId(), job.getUserName(), "", "");
            table.tr((odd = !odd) ? _ODD : _EVEN).
              td(String.valueOf(attempt.getAttemptId())).
              td(new Date(attempt.getStartTime()).toString()).
              td().a(".nodelink", url(MRWebAppUtil.getYARNWebappScheme(),
                  attempt.getNodeHttpAddress()),
                  attempt.getNodeHttpAddress())._().
              td().a(".logslink", url(attempt.getShortLogsLink()), 
                      "logs")._().
            _();
          }
          table._();
          div._();
          
        
        html.div(_INFO_WRAP).        
      
      // Tasks table
        table("#job").
          tr().
            th(_TH, "Task Type").
            th(_TH, "Total").
            th(_TH, "Complete")._().
          tr(_ODD).
            th().
              a(url("tasks", jid, "m"), "Map")._().
            td(String.valueOf(String.valueOf(job.getMapsTotal()))).
            td(String.valueOf(String.valueOf(job.getMapsCompleted())))._().
          tr(_EVEN).
            th().
              a(url("tasks", jid, "r"), "Reduce")._().
            td(String.valueOf(String.valueOf(job.getReducesTotal()))).
            td(String.valueOf(String.valueOf(job.getReducesCompleted())))._()
          ._().

        // Attempts table
        table("#job").
        tr().
          th(_TH, "Attempt Type").
          th(_TH, "Failed").
          th(_TH, "Killed").
          th(_TH, "Successful")._().
        tr(_ODD).
          th("Maps").
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.FAILED.toString()), 
              String.valueOf(job.getFailedMapAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.KILLED.toString()), 
              String.valueOf(job.getKilledMapAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.SUCCESSFUL.toString()), 
              String.valueOf(job.getSuccessfulMapAttempts()))._().
        _().
        tr(_EVEN).
          th("Reduces").
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.FAILED.toString()), 
              String.valueOf(job.getFailedReduceAttempts()))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.KILLED.toString()), 
              String.valueOf(job.getKilledReduceAttempts()))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.SUCCESSFUL.toString()), 
              String.valueOf(job.getSuccessfulReduceAttempts()))._().
         _().
       _().
     _();
  }
}

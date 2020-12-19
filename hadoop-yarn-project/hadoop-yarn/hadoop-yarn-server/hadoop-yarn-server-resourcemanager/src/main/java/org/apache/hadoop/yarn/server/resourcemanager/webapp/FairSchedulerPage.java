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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

public class FairSchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float Q_MAX_WIDTH = 0.8f;
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  static final String Q_END = "left:101%";
  static final String Q_GIVEN = "left:0%;background:none;border:1px dashed rgba(0,0,0,0.25)";
  static final String Q_OVER = "background:rgba(255, 140, 0, 0.8)";
  static final String Q_UNDER = "background:rgba(50, 205, 50, 0.8)";
  
  @RequestScoped
  static class FSQInfo {
    FairSchedulerInfo fsinfo;
    FairSchedulerQueueInfo qinfo;
  }
  
  static class QueueInfoBlock extends HtmlBlock {
    final FairSchedulerQueueInfo qinfo;

    @Inject QueueInfoBlock(ViewContext ctx, FSQInfo info) {
      super(ctx);
      qinfo = (FairSchedulerQueueInfo) info.qinfo;
    }

    @Override
    protected void render(Block html) {
      ResponseInfo ri = info("\'" + qinfo.getQueueName() + "\' Queue Status").
          _("Used Resources:", qinfo.getUsedResources().toString()).
          _("Num Active Applications:", qinfo.getNumActiveApplications()).
          _("Num Pending Applications:", qinfo.getNumPendingApplications()).
          _("Min Resources:", qinfo.getMinResources().toString()).
          _("Max Resources:", qinfo.getMaxResources().toString());
      int maxApps = qinfo.getMaxApplications();
      if (maxApps < Integer.MAX_VALUE) {
          ri._("Max Running Applications:", qinfo.getMaxApplications());
      }
      ri._("Fair Share:", qinfo.getFairShare());

      html._(InfoBlock.class);

      // clear the info contents so this queue's info doesn't accumulate into another queue's info
      ri.clear();
    }
  }
  
  static class QueuesBlock extends HtmlBlock {
    final FairScheduler fs;
    final FSQInfo fsqinfo;
    
    @Inject QueuesBlock(ResourceManager rm, FSQInfo info) {
      fs = (FairScheduler)rm.getResourceScheduler();
      fsqinfo = info;
    }
    
    @Override public void render(Block html) {
      html._(MetricsOverviewTable.class);
      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").
            _("Application Queues")._().
          div("#cs.ui-widget-content.ui-corner-bottom").
            ul();
      if (fs == null) {
        ul.
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(Q_END)._("100% ")._().
              span(".q", "default")._()._();
      } else {
        FairSchedulerInfo sinfo = new FairSchedulerInfo(fs);
        fsqinfo.fsinfo = sinfo;
        fsqinfo.qinfo = null;

        ul.
          li().$style("margin-bottom: 1em").
            span().$style("font-weight: bold")._("Legend:")._().
            span().$class("qlegend ui-corner-all").$style(Q_GIVEN).
              _("Fair Share")._().
            span().$class("qlegend ui-corner-all").$style(Q_UNDER).
              _("Used")._().
            span().$class("qlegend ui-corner-all").$style(Q_OVER).
              _("Used (over fair share)")._().
            span().$class("qlegend ui-corner-all ui-state-default").
              _("Max Capacity")._().
          _();
        
        List<FairSchedulerQueueInfo> subQueues = fsqinfo.fsinfo.getQueueInfos();
        for (FairSchedulerQueueInfo info : subQueues) {
          fsqinfo.qinfo = info;
          float capacity = info.getMaxResourcesFraction();
          float fairShare = info.getFairShareFraction();
          float used = info.getUsedFraction();
          ul.
              li().
                a(_Q).$style(width(capacity * Q_MAX_WIDTH)).
                  $title(join("Fair Share:", percent(fairShare))).
                  span().$style(join(Q_GIVEN, ";font-size:1px;", width(fairShare/capacity))).
                    _('.')._().
                  span().$style(join(width(used/capacity),
                    ";font-size:1px;left:0%;", used > fairShare ? Q_OVER : Q_UNDER)).
                    _('.')._().
                  span(".q", info.getQueueName())._().
                span().$class("qstats").$style(left(Q_STATS_POS)).
                  _(join(percent(used), " used"))._().
                ul("#lq").li()._(QueueInfoBlock.class)._()._().
              _();
        }
      }
      ul._()._().
      script().$type("text/javascript").
          _("$('#cs').hide();")._()._().
      _(FairSchedulerAppsBlock.class);
    }
  }
  
  @Override protected void postHead(Page.HTML<_> html) {
    html.
      style().$type("text/css").
        _("#cs { padding: 0.5em 0 1em 0; margin-bottom: 1em; position: relative }",
          "#cs ul { list-style: none }",
          "#cs a { font-weight: normal; margin: 2px; position: relative }",
          "#cs a span { font-weight: normal; font-size: 80% }",
          "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }",
          "table.info tr th {width: 50%}")._(). // to center info table
      script("/static/jt/jquery.jstree.js").
      script().$type("text/javascript").
        _("$(function() {",
          "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');",
          "  $('#cs').bind('loaded.jstree', function (e, data) {",
          "    data.inst.open_all(); }).",
          "    jstree({",
          "    core: { animation: 188, html_titles: true },",
          "    plugins: ['themeroller', 'html_data', 'ui'],",
          "    themeroller: { item_open: 'ui-icon-minus',",
          "      item_clsd: 'ui-icon-plus', item_leaf: 'ui-icon-gear'",
          "    }",
          "  });",
          "  $('#cs').bind('select_node.jstree', function(e, data) {",
          "    var q = $('.q', data.rslt.obj).first().text();",
            "    if (q == 'root') q = '';",
          "    $('#apps').dataTable().fnFilter(q, 3);",
          "  });",
          "  $('#cs').show();",
          "});")._();
  }
  
  @Override protected Class<? extends SubView> content() {
    return QueuesBlock.class;
  }

  static String percent(float f) {
    return String.format("%.1f%%", f * 100);
  }

  static String width(float f) {
    return String.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return String.format("left:%.1f%%", f * 100);
  }
}

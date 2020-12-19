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

package org.apache.hadoop.yarn.webapp.view;

import com.google.inject.Inject;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * A reusable, pure-css, cross-browser, left nav, 2 column,
 * supposedly liquid layout.
 * Doesn't quite work with resizable themes, kept as an example of the
 * sad state of css (v2/3 anyway) layout.
 * @see TwoColumnLayout
 */
public class TwoColumnCssLayout extends HtmlPage {

  @Override protected void render(Page.HTML<_> html) {
    preHead(html);
    html.
      title($("title")).
      link(root_url("static","yarn.css")).
      style(".main { min-height: 100%; height: auto !important; height: 100%;",
            "  margin: 0 auto -4em; border: 0; }",
            ".footer, .push { height: 4em; clear: both; border: 0 }",
            ".main.ui-widget-content, .footer.ui-widget-content { border: 0; }",
            ".cmask { position: relative; clear: both; float: left;",
            "  width: 100%; overflow: hidden; }",   
            ".leftnav .c1right { float: left; width: 200%; position: relative;",
            "  left: 13em; border: 0; /* background: #fff; */ }",
            ".leftnav .c1wrap { float: right; width: 50%; position: relative;",
            "  right: 13em; padding-bottom: 1em; }",
            ".leftnav .content { margin: 0 1em 0 14em; position: relative;",
            "  right: 100%; overflow: hidden; }",
            ".leftnav .nav { float: left; width: 11em; position: relative;",
            "  right: 12em; overflow: hidden; }").
      _(JQueryUI.class);
    postHead(html);
    JQueryUI.jsnotice(html);
    html.
      div(".main.ui-widget-content").
        _(header()).
        div(".cmask.leftnav").
          div(".c1right").
            div(".c1wrap").
              div(".content").
                _(content())._()._().
            div(".nav").
              _(nav()).
              div(".push")._()._()._()._()._().
      div(".footer.ui-widget-content").
        _(footer())._()._();
  }

  protected void preHead(Page.HTML<_> html) {
  }

  protected void postHead(Page.HTML<_> html) {
  }

  protected Class<? extends SubView> header() {
    return HeaderBlock.class;
  }

  protected Class<? extends SubView> content() {
    return LipsumBlock.class;
  }

  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

  protected Class<? extends SubView> footer() {
    return FooterBlock.class;
  }
}

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
package org.apache.hadoop.yarn.client.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

@Private
@Unstable
public class NodeCLI extends YarnCLI {
  private static final String NODES_PATTERN = "%16s\t%10s\t%17s\t%18s" +
    System.getProperty("line.separator");

  public static void main(String[] args) throws Exception {
    NodeCLI cli = new NodeCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, args);
    cli.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {

    Options opts = new Options();
    opts.addOption(STATUS_CMD, true, "Prints the status report of the node.");
    opts.addOption(LIST_CMD, false, "Lists all the nodes in the RUNNING state.");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    int exitCode = -1;
    if (cliParser.hasOption("status")) {
      if (args.length != 2) {
        printUsage(opts);
        return exitCode;
      }
      printNodeStatus(cliParser.getOptionValue("status"));
    } else if (cliParser.hasOption("list")) {
      listClusterNodes();
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(opts);
    }
    return 0;
  }

  /**
   * It prints the usage of the command
   * 
   * @param opts
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("node", opts);
  }

  /**
   * Lists all the nodes present in the cluster
   * 
   * @throws YarnException
   * @throws IOException
   */
  private void listClusterNodes() throws YarnException, IOException {
    PrintWriter writer = new PrintWriter(sysout);
    List<NodeReport> nodesReport = client.getNodeReports(NodeState.RUNNING);
    writer.println("Total Nodes:" + nodesReport.size());
    writer.printf(NODES_PATTERN, "Node-Id", "Node-State", "Node-Http-Address",
        "Running-Containers");
    for (NodeReport nodeReport : nodesReport) {
      writer.printf(NODES_PATTERN, nodeReport.getNodeId(), nodeReport
          .getNodeState(), nodeReport.getHttpAddress(), nodeReport
          .getNumContainers());
    }
    writer.flush();
  }

  /**
   * Prints the node report for node id.
   * 
   * @param nodeIdStr
   * @throws YarnException
   */
  private void printNodeStatus(String nodeIdStr) throws YarnException,
      IOException {
    NodeId nodeId = ConverterUtils.toNodeId(nodeIdStr);
    List<NodeReport> nodesReport = client.getNodeReports();
    // Use PrintWriter.println, which uses correct platform line ending.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter nodeReportStr = new PrintWriter(baos);
    NodeReport nodeReport = null;
    for (NodeReport report : nodesReport) {
      if (!report.getNodeId().equals(nodeId)) {
        continue;
      }
      nodeReport = report;
      nodeReportStr.println("Node Report : ");
      nodeReportStr.print("\tNode-Id : ");
      nodeReportStr.println(nodeReport.getNodeId());
      nodeReportStr.print("\tRack : ");
      nodeReportStr.println(nodeReport.getRackName());
      nodeReportStr.print("\tNode-State : ");
      nodeReportStr.println(nodeReport.getNodeState());
      nodeReportStr.print("\tNode-Http-Address : ");
      nodeReportStr.println(nodeReport.getHttpAddress());
      nodeReportStr.print("\tLast-Health-Update : ");
      nodeReportStr.println(DateFormatUtils.format(
          new Date(nodeReport.getLastHealthReportTime()),
            "E dd/MMM/yy hh:mm:ss:SSzz"));
      nodeReportStr.print("\tHealth-Report : ");
      nodeReportStr
          .println(nodeReport.getHealthReport());
      nodeReportStr.print("\tContainers : ");
      nodeReportStr.println(nodeReport.getNumContainers());
      nodeReportStr.print("\tMemory-Used : ");
      nodeReportStr.println((nodeReport.getUsed() == null) ? "0MB"
          : (nodeReport.getUsed().getMemory() + "MB"));
      nodeReportStr.print("\tMemory-Capacity : ");
      nodeReportStr.println(nodeReport.getCapability().getMemory() + "MB");
      nodeReportStr.print("\tCPU-Used : ");
      nodeReportStr.println((nodeReport.getUsed() == null) ? "0 vcores"
          : (nodeReport.getUsed().getVirtualCores() + " vcores"));
      nodeReportStr.print("\tCPU-Capacity : ");
      nodeReportStr.println(nodeReport.getCapability().getVirtualCores() + " vcores");
    }

    if (nodeReport == null) {
      nodeReportStr.print("Could not find the node report for node id : "
          + nodeIdStr);
    }
    nodeReportStr.close();
    sysout.println(baos.toString("UTF-8"));
  }
}

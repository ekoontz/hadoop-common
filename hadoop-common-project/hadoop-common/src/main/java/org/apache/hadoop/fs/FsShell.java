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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Provide command line access to a FileSystem. */
@InterfaceAudience.Private
public class FsShell extends Configured implements Tool {
  
  static final Log LOG = LogFactory.getLog(FsShell.class);

  private FileSystem fs;
  private Trash trash;
  protected CommandFactory commandFactory;

  private final String usagePrefix =
    "Usage: hadoop fs [generic options]";

  /**
   * Default ctor with no configuration.  Be sure to invoke
   * {@link #setConf(Configuration)} with a valid configuration prior
   * to running commands.
   */
  public FsShell() {
    this(null);
  }

  /**
   * Construct a FsShell with the given configuration.  Commands can be
   * executed via {@link #run(String[])}
   * @param conf the hadoop configuration
   */
  public FsShell(Configuration conf) {
    super(conf);
  }
  
  protected FileSystem getFS() throws IOException {
    if (fs == null) {
      fs = FileSystem.get(getConf());
    }
    return fs;
  }
  
  protected Trash getTrash() throws IOException {
    if (this.trash == null) {
      this.trash = new Trash(getConf());
    }
    return this.trash;
  }
  
  protected void init() throws IOException {
    getConf().setQuietMode(true);
    if (commandFactory == null) {
      commandFactory = new CommandFactory(getConf());
      commandFactory.addObject(new Help(), "-help");
      commandFactory.addObject(new Usage(), "-usage");
      registerCommands(commandFactory);
    }
  }

  protected void registerCommands(CommandFactory factory) {
    // TODO: DFSAdmin subclasses FsShell so need to protect the command
    // registration.  This class should morph into a base class for
    // commands, and then this method can be abstract
    if (this.getClass().equals(FsShell.class)) {
      factory.registerCommands(FsCommand.class);
    }
  }
  
  /**
   * Returns the Trash object associated with this shell.
   * @return Path to the trash
   * @throws IOException upon error
   */
  public Path getCurrentTrashDir() throws IOException {
    return getTrash().getCurrentTrashDir();
  }

  // NOTE: Usage/Help are inner classes to allow access to outer methods
  // that access commandFactory
  
  /**
   *  Display help for commands with their short usage and long description
   */
   protected class Usage extends FsCommand {
    public static final String NAME = "usage";
    public static final String USAGE = "[cmd ...]";
    public static final String DESCRIPTION =
      "Displays the usage for given command or all commands if none\n" +
      "is specified.";
    
    @Override
    protected void processRawArguments(LinkedList<String> args) {
      if (args.isEmpty()) {
        printUsage(System.out);
      } else {
        for (String arg : args) printUsage(System.out, arg);
      }
    }
  } 

  /**
   * Displays short usage of commands sans the long description
   */
  protected class Help extends FsCommand {
    public static final String NAME = "help";
    public static final String USAGE = "[cmd ...]";
    public static final String DESCRIPTION =
      "Displays help for given command or all commands if none\n" +
      "is specified.";
    
    @Override
    protected void processRawArguments(LinkedList<String> args) {
      if (args.isEmpty()) {
        printHelp(System.out);
      } else {
        for (String arg : args) printHelp(System.out, arg);
      }
    }
  }

  /*
   * The following are helper methods for getInfo().  They are defined
   * outside of the scope of the Help/Usage class because the run() method
   * needs to invoke them too. 
   */

  // print all usages
  private void printUsage(PrintStream out) {
    printInfo(out, null, false);
  }
  
  // print one usage
  private void printUsage(PrintStream out, String cmd) {
    printInfo(out, cmd, false);
  }

  // print all helps
  private void printHelp(PrintStream out) {
    printInfo(out, null, true);
  }

  // print one help
  private void printHelp(PrintStream out, String cmd) {
    printInfo(out, cmd, true);
  }

  private void printInfo(PrintStream out, String cmd, boolean showHelp) {
    if (cmd != null) {
      // display help or usage for one command
      Command instance = commandFactory.getInstance("-" + cmd);
      if (instance == null) {
        throw new UnknownCommandException(cmd);
      }
      if (showHelp) {
        printInstanceHelp(out, instance);
      } else {
        printInstanceUsage(out, instance);
      }
    } else {
      // display help or usage for all commands 
      out.println(usagePrefix);
      
      // display list of short usages
      ArrayList<Command> instances = new ArrayList<Command>();
      for (String name : commandFactory.getNames()) {
        Command instance = commandFactory.getInstance(name);
        if (!instance.isDeprecated()) {
          System.out.println("\t[" + instance.getUsage() + "]");
          instances.add(instance);
        }
      }
      // display long descriptions for each command
      if (showHelp) {
        for (Command instance : instances) {
          out.println();
          printInstanceHelp(out, instance);
        }
      }
      out.println();
      ToolRunner.printGenericCommandUsage(out);
    }
  }

  private void printInstanceUsage(PrintStream out, Command instance) {
    out.println(usagePrefix + " " + instance.getUsage());
  }

  // TODO: will eventually auto-wrap the text, but this matches the expected
  // output for the hdfs tests...
  private void printInstanceHelp(PrintStream out, Command instance) {
    boolean firstLine = true;
    for (String line : instance.getDescription().split("\n")) {
      String prefix;
      if (firstLine) {
        prefix = instance.getUsage() + ":\t";
        firstLine = false;
      } else {
        prefix = "\t\t";
      }
      System.out.println(prefix + line);
    }    
  }

  /**
   * run
   */
  @Override
  public int run(String argv[]) throws Exception {
    // initialize FsShell
    init();

    int exitCode = -1;
    if (argv.length < 1) {
      printUsage(System.err);
    } else {
      String cmd = argv[0];
      Command instance = null;
      try {
        instance = commandFactory.getInstance(cmd);
        if (instance == null) {
          throw new UnknownCommandException();
        }
        exitCode = instance.run(Arrays.copyOfRange(argv, 1, argv.length));
      } catch (IllegalArgumentException e) {
        displayError(cmd, e.getLocalizedMessage());
        if (instance != null) {
          printInstanceUsage(System.err, instance);
        }
      } catch (Exception e) {
        // instance.run catches IOE, so something is REALLY wrong if here
        LOG.debug("Error", e);
        displayError(cmd, "Fatal internal error");
        e.printStackTrace(System.err);
      }
    }
    return exitCode;
  }
  
  private void displayError(String cmd, String message) {
    for (String line : message.split("\n")) {
      System.err.println(cmd + ": " + line);
      if (cmd.charAt(0) != '-') {
        Command instance = null;
        instance = commandFactory.getInstance("-" + cmd);
        if (instance != null) {
          System.err.println("Did you mean -" + cmd + "?  This command " +
              "begins with a dash.");
        }
      }
    }
  }
  
  /**
   *  Performs any necessary cleanup
   * @throws IOException upon error
   */
  public void close() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * main() has some simple utility methods
   * @param argv the command and its arguments
   * @throws Exception upon error
   */
  public static void main(String argv[]) throws Exception {
    FsShell shell = newShellInstance();
    Configuration conf = new Configuration();
    conf.setQuietMode(false);
    shell.setConf(conf);
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }

  // TODO: this should be abstract in a base class
  protected static FsShell newShellInstance() {
    return new FsShell();
  }
  
  /**
   * The default ctor signals that the command being executed does not exist,
   * while other ctor signals that a specific command does not exist.  The
   * latter is used by commands that process other commands, ex. -usage/-help
   */
  @SuppressWarnings("serial")
  static class UnknownCommandException extends IllegalArgumentException {
    private final String cmd;    
    UnknownCommandException() { this(null); }
    UnknownCommandException(String cmd) { this.cmd = cmd; }
    
    @Override
    public String getMessage() {
      return ((cmd != null) ? "`"+cmd+"': " : "") + "Unknown command";
    }
  }
}

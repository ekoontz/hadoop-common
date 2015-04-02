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
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Acl related operations
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AclCommands extends FsCommand {
  private static String GET_FACL = "getfacl";
  private static String SET_FACL = "setfacl";

  public static void registerCommands(CommandFactory factory) {
    factory.addClass(GetfaclCommand.class, "-" + GET_FACL);
    factory.addClass(SetfaclCommand.class, "-" + SET_FACL);
  }

  /**
   * Implementing the '-getfacl' command for the the FsShell.
   */
  public static class GetfaclCommand extends FsCommand {
    public static String NAME = GET_FACL;
    public static String USAGE = "[-R] <path>";
    public static String DESCRIPTION = "Displays the Access Control Lists"
        + " (ACLs) of files and directories. If a directory has a default ACL,"
        + " then getfacl also displays the default ACL.\n"
        + "-R: List the ACLs of all files and directories recursively.\n"
        + "<path>: File or directory to list.\n";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "R");
      cf.parse(args);
      setRecursive(cf.getOpt("R"));
      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      out.println("# file: " + item);
      out.println("# owner: " + item.stat.getOwner());
      out.println("# group: " + item.stat.getGroup());
      FsPermission perm = item.stat.getPermission();
      if (perm.getStickyBit()) {
        out.println("# flags: --" +
          (perm.getOtherAction().implies(FsAction.EXECUTE) ? "t" : "T"));
      }

      if (perm.getAclBit()) {
        AclStatus aclStatus = item.fs.getAclStatus(item.path);
        List<AclEntry> entries = aclStatus.getEntries();
        printExtendedAcl(perm, entries);
      } else {
        printMinimalAcl(perm);
      }

      out.println();
    }

    /**
     * Prints an extended ACL, including all extended ACL entries and also the
     * base entries implied by the permission bits.
     *
     * @param perm FsPermission of file
     * @param entries List<AclEntry> containing ACL entries of file
     */
    private void printExtendedAcl(FsPermission perm, List<AclEntry> entries) {
      // Print owner entry implied by owner permission bits.
      out.println(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER)
        .setPermission(perm.getUserAction())
        .build());

      // Print all extended access ACL entries.
      boolean hasAccessAcl = false;
      Iterator<AclEntry> entryIter = entries.iterator();
      AclEntry curEntry = null;
      while (entryIter.hasNext()) {
        curEntry = entryIter.next();
        if (curEntry.getScope() == AclEntryScope.DEFAULT) {
          break;
        }
        hasAccessAcl = true;
        printExtendedAclEntry(curEntry, perm.getGroupAction());
      }

      // Print mask entry implied by group permission bits, or print group entry
      // if there is no access ACL (only default ACL).
      out.println(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(hasAccessAcl ? AclEntryType.MASK : AclEntryType.GROUP)
        .setPermission(perm.getGroupAction())
        .build());

      // Print other entry implied by other bits.
      out.println(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.OTHER)
        .setPermission(perm.getOtherAction())
        .build());

      // Print default ACL entries.
      if (curEntry != null && curEntry.getScope() == AclEntryScope.DEFAULT) {
        out.println(curEntry);
        // ACL sort order guarantees default mask is the second-to-last entry.
        FsAction maskPerm = entries.get(entries.size() - 2).getPermission();
        while (entryIter.hasNext()) {
          printExtendedAclEntry(entryIter.next(), maskPerm);
        }
      }
    }

    /**
     * Prints a single extended ACL entry.  If the mask restricts the
     * permissions of the entry, then also prints the restricted version as the
     * effective permissions.  The mask applies to all named entries and also
     * the unnamed group entry.
     *
     * @param entry AclEntry extended ACL entry to print
     * @param maskPerm FsAction permissions in the ACL's mask entry
     */
    private void printExtendedAclEntry(AclEntry entry, FsAction maskPerm) {
      if (entry.getName() != null || entry.getType() == AclEntryType.GROUP) {
        FsAction entryPerm = entry.getPermission();
        FsAction effectivePerm = entryPerm.and(maskPerm);
        if (entryPerm != effectivePerm) {
          out.println(String.format("%s\t#effective:%s", entry,
            effectivePerm.SYMBOL));
        } else {
          out.println(entry);
        }
      } else {
        out.println(entry);
      }
    }

    /**
     * Prints a minimal ACL, consisting of exactly 3 ACL entries implied by the
     * permission bits.
     *
     * @param perm FsPermission of file
     */
    private void printMinimalAcl(FsPermission perm) {
      out.println(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER)
        .setPermission(perm.getUserAction())
        .build());
      out.println(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.GROUP)
        .setPermission(perm.getGroupAction())
        .build());
      out.println(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.OTHER)
        .setPermission(perm.getOtherAction())
        .build());
    }
  }

  /**
   * Implementing the '-setfacl' command for the the FsShell.
   */
  public static class SetfaclCommand extends FsCommand {
    public static String NAME = SET_FACL;
    public static String USAGE = "[-R] [{-b|-k} {-m|-x <acl_spec>} <path>]"
        + "|[--set <acl_spec> <path>]";
    public static String DESCRIPTION = "Sets Access Control Lists (ACLs)"
        + " of files and directories.\n" 
        + "Options:\n"
        + "-b :Remove all but the base ACL entries. The entries for user,"
        + " group and others are retained for compatibility with permission "
        + "bits.\n" 
        + "-k :Remove the default ACL.\n"
        + "-R :Apply operations to all files and directories recursively.\n"
        + "-m :Modify ACL. New entries are added to the ACL, and existing"
        + " entries are retained.\n"
        + "-x :Remove specified ACL entries. Other ACL entries are retained.\n"
        + "--set :Fully replace the ACL, discarding all existing entries."
        + " The <acl_spec> must include entries for user, group, and others"
        + " for compatibility with permission bits.\n"
        + "<acl_spec>: Comma separated list of ACL entries.\n"
        + "<path>: File or directory to modify.\n";

    CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "b", "k", "R",
        "m", "x", "-set");
    List<AclEntry> aclEntries = null;
    List<AclEntry> accessAclEntries = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      cf.parse(args);
      setRecursive(cf.getOpt("R"));
      // Mix of remove and modify acl flags are not allowed
      boolean bothRemoveOptions = cf.getOpt("b") && cf.getOpt("k");
      boolean bothModifyOptions = cf.getOpt("m") && cf.getOpt("x");
      boolean oneRemoveOption = cf.getOpt("b") || cf.getOpt("k");
      boolean oneModifyOption = cf.getOpt("m") || cf.getOpt("x");
      boolean setOption = cf.getOpt("-set");
      if ((bothRemoveOptions || bothModifyOptions)
          || (oneRemoveOption && oneModifyOption)
          || (setOption && (oneRemoveOption || oneModifyOption))) {
        throw new HadoopIllegalArgumentException(
            "Specified flags contains both remove and modify flags");
      }

      // Only -m, -x and --set expects <acl_spec>
      if (oneModifyOption || setOption) {
        if (args.size() < 2) {
          throw new HadoopIllegalArgumentException("<acl_spec> is missing");
        }
        aclEntries = AclEntry.parseAclSpec(args.removeFirst(), !cf.getOpt("x"));
      }

      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments");
      }

      // In recursive mode, save a separate list of just the access ACL entries.
      // Only directories may have a default ACL.  When a recursive operation
      // encounters a file under the specified path, it must pass only the
      // access ACL entries.
      if (isRecursive() && (oneModifyOption || setOption)) {
        accessAclEntries = Lists.newArrayList();
        for (AclEntry entry: aclEntries) {
          if (entry.getScope() == AclEntryScope.ACCESS) {
            accessAclEntries.add(entry);
          }
        }
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (cf.getOpt("b")) {
        item.fs.removeAcl(item.path);
      } else if (cf.getOpt("k")) {
        item.fs.removeDefaultAcl(item.path);
      } else if (cf.getOpt("m")) {
        List<AclEntry> entries = getAclEntries(item);
        if (!entries.isEmpty()) {
          item.fs.modifyAclEntries(item.path, entries);
        }
      } else if (cf.getOpt("x")) {
        List<AclEntry> entries = getAclEntries(item);
        if (!entries.isEmpty()) {
          item.fs.removeAclEntries(item.path, entries);
        }
      } else if (cf.getOpt("-set")) {
        List<AclEntry> entries = getAclEntries(item);
        if (!entries.isEmpty()) {
          item.fs.setAcl(item.path, entries);
        }
      }
    }

    /**
     * Returns the ACL entries to use in the API call for the given path.  For a
     * recursive operation, returns all specified ACL entries if the item is a
     * directory or just the access ACL entries if the item is a file.  For a
     * non-recursive operation, returns all specified ACL entries.
     *
     * @param item PathData path to check
     * @return List<AclEntry> ACL entries to use in the API call
     */
    private List<AclEntry> getAclEntries(PathData item) {
      if (isRecursive()) {
        return item.stat.isDirectory() ? aclEntries : accessAclEntries;
      } else {
        return aclEntries;
      }
    }
  }
}

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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test function of OfflineImageViewer by:
 *   * confirming it can correctly process a valid fsimage file and that
 *     the processing generates a correct representation of the namespace
 *   * confirming it correctly fails to process an fsimage file with a layout
 *     version it shouldn't be able to handle
 *   * confirm it correctly bails on malformed image files, in particular, a
 *     file that ends suddenly.
 */
public class TestOfflineImageViewer {
  private static final Log LOG = LogFactory.getLog(OfflineImageViewer.class);
  private static final int NUM_DIRS = 3;
  private static final int FILES_PER_DIR = 4;
  private static final String TEST_RENEWER = "JobTracker";
  private static File originalFsimage = null;

  // Elements of lines of ls-file output to be compared to FileStatus instance
  private static class LsElements {
    public String perms;
    public int replication;
    public String username;
    public String groupname;
    public long filesize;
    public char dir; // d if dir, - otherwise
  }
  
  // namespace as written to dfs, to be compared with viewer's output
  final static HashMap<String, FileStatus> writtenFiles = 
      new HashMap<String, FileStatus>();
  
  private static String ROOT = System.getProperty("test.build.data",
                                                  "build/test/data");
  
  // Create a populated namespace for later testing.  Save its contents to a
  // data structure and store its fsimage location.
  // We only want to generate the fsimage file once and use it for
  // multiple tests.
  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 5000);
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
          "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//" + "DEFAULT");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      cluster.waitActive();
      FileSystem hdfs = cluster.getFileSystem();
      
      int filesize = 256;
      
      // Create a reasonable namespace 
      for(int i = 0; i < NUM_DIRS; i++)  {
        Path dir = new Path("/dir" + i);
        hdfs.mkdirs(dir);
        writtenFiles.put(dir.toString(), pathToFileEntry(hdfs, dir.toString()));
        for(int j = 0; j < FILES_PER_DIR; j++) {
          Path file = new Path(dir, "file" + j);
          FSDataOutputStream o = hdfs.create(file);
          o.write(new byte[ filesize++ ]);
          o.close();
          
          writtenFiles.put(file.toString(), pathToFileEntry(hdfs, file.toString()));
        }
      }

      // Get delegation tokens so we log the delegation token op
      Token<?>[] delegationTokens = 
          hdfs.addDelegationTokens(TEST_RENEWER, null);
      for (Token<?> t : delegationTokens) {
        LOG.debug("got token " + t);
      }

      // Write results to the fsimage file
      cluster.getNameNodeRpc()
          .setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      cluster.getNameNodeRpc().saveNamespace();
      
      // Determine location of fsimage file
      originalFsimage = FSImageTestUtil.findLatestImageFile(
          FSImageTestUtil.getFSImage(
          cluster.getNameNode()).getStorage().getStorageDir(0));
      if (originalFsimage == null) {
        throw new RuntimeException("Didn't generate or can't find fsimage");
      }
      LOG.debug("original FS image file is " + originalFsimage);
    } finally {
      if(cluster != null)
        cluster.shutdown();
    }
  }
  
  @AfterClass
  public static void deleteOriginalFSImage() throws IOException {
    if(originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }
  
  // Convenience method to generate a file status from file system for 
  // later comparison
  private static FileStatus pathToFileEntry(FileSystem hdfs, String file) 
        throws IOException {
    return hdfs.getFileStatus(new Path(file));
  }
  
  // Verify that we can correctly generate an ls-style output for a valid 
  // fsimage
  @Test
  public void outputOfLSVisitor() throws IOException {
    File testFile = new File(ROOT, "/basicCheck");
    File outputFile = new File(ROOT, "/basicCheckOutput");
    
    try {
      copyFile(originalFsimage, testFile);
      
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);

      oiv.go();
      
      HashMap<String, LsElements> fileOutput = readLsfile(outputFile);
      
      compareNamespaces(writtenFiles, fileOutput);
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
    LOG.debug("Correctly generated ls-style output.");
  }
  
  // Confirm that attempting to read an fsimage file with an unsupported
  // layout results in an error
  @Test
  public void unsupportedFSLayoutVersion() throws IOException {
    File testFile = new File(ROOT, "/invalidLayoutVersion");
    File outputFile = new File(ROOT, "invalidLayoutVersionOutput");
    
    try {
      int badVersionNum = -432;
      changeLayoutVersion(originalFsimage, testFile, badVersionNum);
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);
      
      try {
        oiv.go();
        fail("Shouldn't be able to read invalid laytout version");
      } catch(IOException e) {
        if(!e.getMessage().contains(Integer.toString(badVersionNum)))
          throw e; // wasn't error we were expecting
        LOG.debug("Correctly failed at reading bad image version.");
      }
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
  }
  
  // Verify that image viewer will bail on a file that ends unexpectedly
  @Test
  public void truncatedFSImage() throws IOException {
    File testFile = new File(ROOT, "/truncatedFSImage");
    File outputFile = new File(ROOT, "/trucnatedFSImageOutput");
    try {
      copyPartOfFile(originalFsimage, testFile);
      assertTrue("Created truncated fsimage", testFile.exists());
      
      ImageVisitor v = new LsImageVisitor(outputFile.getPath(), true);
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, false);

      try {
        oiv.go();
        fail("Managed to process a truncated fsimage file");
      } catch (EOFException e) {
        LOG.debug("Correctly handled EOF");
      }

    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
  }
  
  // Test that our ls file has all the same compenents of the original namespace
  private void compareNamespaces(HashMap<String, FileStatus> written,
      HashMap<String, LsElements> fileOutput) {
    assertEquals( "Should be the same number of files in both, plus one for root"
            + " in fileoutput", fileOutput.keySet().size(), 
                                written.keySet().size() + 1);
    Set<String> inFile = fileOutput.keySet();

    // For each line in the output file, verify that the namespace had a
    // filestatus counterpart 
    for (String path : inFile) {
      if (path.equals("/")) // root's not included in output from system call
        continue;

      assertTrue("Path in file (" + path + ") was written to fs", written
          .containsKey(path));
      
      compareFiles(written.get(path), fileOutput.get(path));
      
      written.remove(path);
    }

    assertEquals("No more files were written to fs", 0, written.size());
  }
  
  // Compare two files as listed in the original namespace FileStatus and
  // the output of the ls file from the image processor
  private void compareFiles(FileStatus fs, LsElements elements) {
    assertEquals("directory listed as such",  
                 fs.isDirectory() ? 'd' : '-', elements.dir);
    assertEquals("perms string equal", 
                                fs.getPermission().toString(), elements.perms);
    assertEquals("replication equal", fs.getReplication(), elements.replication);
    assertEquals("owner equal", fs.getOwner(), elements.username);
    assertEquals("group equal", fs.getGroup(), elements.groupname);
    assertEquals("lengths equal", fs.getLen(), elements.filesize);
  }

  // Read the contents of the file created by the Ls processor
  private HashMap<String, LsElements> readLsfile(File lsFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(lsFile));
    String line = null;
    HashMap<String, LsElements> fileContents = new HashMap<String, LsElements>();
    
    while((line = br.readLine()) != null) 
      readLsLine(line, fileContents);
    
    return fileContents;
  }
  
  // Parse a line from the ls output.  Store permissions, replication, 
  // username, groupname and filesize in hashmap keyed to the path name
  private void readLsLine(String line, HashMap<String, LsElements> fileContents) {
    String elements [] = line.split("\\s+");
    
    assertEquals("Not enough elements in ls output", 8, elements.length);
    
    LsElements lsLine = new LsElements();
    
    lsLine.dir = elements[0].charAt(0);
    lsLine.perms = elements[0].substring(1);
    lsLine.replication = elements[1].equals("-") 
                                             ? 0 : Integer.valueOf(elements[1]);
    lsLine.username = elements[2];
    lsLine.groupname = elements[3];
    lsLine.filesize = Long.valueOf(elements[4]);
    // skipping date and time 
    
    String path = elements[7];
    
    // Check that each file in the ls output was listed once
    assertFalse("LS file had duplicate file entries", 
        fileContents.containsKey(path));
    
    fileContents.put(path, lsLine);
  }
  
  // Copy one fsimage to another, changing the layout version in the process
  private void changeLayoutVersion(File src, File dest, int newVersion) 
         throws IOException {
    DataInputStream in = null; 
    DataOutputStream out = null; 
    
    try {
      in = new DataInputStream(new FileInputStream(src));
      out = new DataOutputStream(new FileOutputStream(dest));
      
      in.readInt();
      out.writeInt(newVersion);
      
      byte [] b = new byte[1024];
      while( in.read(b)  > 0 ) {
        out.write(b);
      }
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }
  
  // Only copy part of file into the other.  Used for testing truncated fsimage
  private void copyPartOfFile(File src, File dest) throws IOException {
    InputStream in = null;
    OutputStream out = null;
    
    byte [] b = new byte[256];
    int bytesWritten = 0;
    int count;
    int maxBytes = 700;
    
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);
      
      while( (count = in.read(b))  > 0 && bytesWritten < maxBytes ) {
        out.write(b);
        bytesWritten += count;
      } 
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }
  
  // Copy one file's contents into the other
  private void copyFile(File src, File dest) throws IOException {
    InputStream in = null;
    OutputStream out = null;
    
    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest);

      byte [] b = new byte[1024];
      while( in.read(b)  > 0 ) {
        out.write(b);
      }
    } finally {
      if(in != null) in.close();
      if(out != null) out.close();
    }
  }

  @Test
  public void outputOfFileDistributionVisitor() throws IOException {
    File testFile = new File(ROOT, "/basicCheck");
    File outputFile = new File(ROOT, "/fileDistributionCheckOutput");

    int totalFiles = 0;
    try {
      copyFile(originalFsimage, testFile);
      ImageVisitor v = new FileDistributionVisitor(outputFile.getPath(), 0, 0);
      OfflineImageViewer oiv = 
        new OfflineImageViewer(testFile.getPath(), v, false);

      oiv.go();

      BufferedReader reader = new BufferedReader(new FileReader(outputFile));
      String line = reader.readLine();
      assertEquals(line, "Size\tNumFiles");
      while((line = reader.readLine()) != null) {
        String[] row = line.split("\t");
        assertEquals(row.length, 2);
        totalFiles += Integer.parseInt(row[1]);
      }
    } finally {
      if(testFile.exists()) testFile.delete();
      if(outputFile.exists()) outputFile.delete();
    }
    assertEquals(totalFiles, NUM_DIRS * FILES_PER_DIR);
  }
  
  private static class TestImageVisitor extends ImageVisitor {
    private List<String> delegationTokenRenewers = new LinkedList<String>();
    TestImageVisitor() {
    }
    
    List<String> getDelegationTokenRenewers() {
      return delegationTokenRenewers;
    }

    @Override
    void start() throws IOException {
    }

    @Override
    void finish() throws IOException {
    }

    @Override
    void finishAbnormally() throws IOException {
    }

    @Override
    void visit(ImageElement element, String value) throws IOException {
      if (element == ImageElement.DELEGATION_TOKEN_IDENTIFIER_RENEWER) {
        delegationTokenRenewers.add(value);
      }
    }

    @Override
    void visitEnclosingElement(ImageElement element) throws IOException {
    }

    @Override
    void visitEnclosingElement(ImageElement element, ImageElement key,
        String value) throws IOException {
    }

    @Override
    void leaveEnclosingElement() throws IOException {
    }
  }

  @Test
  public void outputOfTestVisitor() throws IOException {
    File testFile = new File(ROOT, "/basicCheck");

    try {
      copyFile(originalFsimage, testFile);
      TestImageVisitor v = new TestImageVisitor();
      OfflineImageViewer oiv = new OfflineImageViewer(testFile.getPath(), v, true);
      oiv.go();

      // Validated stored delegation token identifiers.
      List<String> dtrs = v.getDelegationTokenRenewers();
      assertEquals(1, dtrs.size());
      assertEquals(TEST_RENEWER, dtrs.get(0));
    } finally {
      if(testFile.exists()) testFile.delete();
    }
    LOG.debug("Passed TestVisitor validation.");
  }
}

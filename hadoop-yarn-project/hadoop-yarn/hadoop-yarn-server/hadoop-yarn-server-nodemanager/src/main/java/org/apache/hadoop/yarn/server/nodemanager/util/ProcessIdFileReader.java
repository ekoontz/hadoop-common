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
package org.apache.hadoop.yarn.server.nodemanager.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * Helper functionality to read the pid from a file.
 */
public class ProcessIdFileReader {

  private static final Log LOG = LogFactory.getLog(ProcessIdFileReader.class);
  
  /**
   * Get the process id from specified file path.
   * Parses each line to find a valid number
   * and returns the first one found.
   * @return Process Id if obtained from path specified else null
   * @throws IOException
   */
  public static String getProcessId(Path path) throws IOException {
    if (path == null) {
      throw new IOException("Trying to access process id from a null path");
    }
    
    LOG.debug("Accessing pid from pid file " + path);
    String processId = null;
    FileReader fileReader = null;
    BufferedReader bufReader = null;

    try {
      File file = new File(path.toString());
      if (file.exists()) {
        fileReader = new FileReader(file);
        bufReader = new BufferedReader(fileReader);
        while (true) {
          String line = bufReader.readLine();
          if (line == null) {
            break;
          }
          String temp = line.trim(); 
          if (!temp.isEmpty()) {
            try {
              Long pid = Long.valueOf(temp);
              if (pid > 0) {
                processId = temp;
                break;
              }
            } catch (Exception e) {
              // do nothing
            }
          }
        }
      }
    } finally {
      if (fileReader != null) {
        fileReader.close();
      }
      if (bufReader != null) {
        bufReader.close();
      }
    }
    LOG.debug("Got pid " 
        + (processId != null? processId : "null")  
        + " from path " + path);
    return processId;
  }

}

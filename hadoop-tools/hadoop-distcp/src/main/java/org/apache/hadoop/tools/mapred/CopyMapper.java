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

package org.apache.hadoop.tools.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.util.EnumSet;
import java.util.Arrays;

/**
 * Mapper class that executes the DistCp copy operation.
 * Implements the o.a.h.mapreduce.Mapper<> interface.
 */
public class CopyMapper extends Mapper<Text, FileStatus, Text, Text> {

  /**
   * Hadoop counters for the DistCp CopyMapper.
   * (These have been kept identical to the old DistCp,
   * for backward compatibility.)
   */
  public static enum Counter {
    COPY,         // Number of files received by the mapper for copy.
    SKIP,         // Number of files skipped.
    FAIL,         // Number of files that failed to be copied.
    BYTESCOPIED,  // Number of bytes actually copied by the copy-mapper, total.
    BYTESEXPECTED,// Number of bytes expected to be copied.
    BYTESFAILED,  // Number of bytes that failed to be copied.
    BYTESSKIPPED, // Number of bytes that were skipped from copy.
  }

  private static Log LOG = LogFactory.getLog(CopyMapper.class);

  private Configuration conf;

  private boolean syncFolders = false;
  private boolean ignoreFailures = false;
  private boolean skipCrc = false;
  private boolean overWrite = false;
  private EnumSet<FileAttribute> preserve = EnumSet.noneOf(FileAttribute.class);

  private FileSystem targetFS = null;
  private Path    targetWorkPath = null;

  /**
   * Implementation of the Mapper::setup() method. This extracts the DistCp-
   * options specified in the Job's configuration, to set up the Job.
   * @param context Mapper's context.
   * @throws IOException On IO failure.
   * @throws InterruptedException If the job is interrupted.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();

    syncFolders = conf.getBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false);
    ignoreFailures = conf.getBoolean(DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(), false);
    skipCrc = conf.getBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(), false);
    overWrite = conf.getBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(), false);
    preserve = DistCpUtils.unpackAttributes(conf.get(DistCpOptionSwitch.
        PRESERVE_STATUS.getConfigLabel()));

    targetWorkPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
    Path targetFinalPath = new Path(conf.get(
            DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    targetFS = targetFinalPath.getFileSystem(conf);

    if (targetFS.exists(targetFinalPath) && targetFS.isFile(targetFinalPath)) {
      overWrite = true; // When target is an existing file, overwrite it.
    }

    if (conf.get(DistCpConstants.CONF_LABEL_SSL_CONF) != null) {
      initializeSSLConf(context);
    }
  }

  /**
   * Initialize SSL Config if same is set in conf
   *
   * @throws IOException - If any
   */
  private void initializeSSLConf(Context context) throws IOException {
    LOG.info("Initializing SSL configuration");
    
    String workDir = conf.get(JobContext.JOB_LOCAL_DIR) + "/work";
    Path[] cacheFiles = context.getLocalCacheFiles();

    Configuration sslConfig = new Configuration(false);
    String sslConfFileName = conf.get(DistCpConstants.CONF_LABEL_SSL_CONF);
    Path sslClient = findCacheFile(cacheFiles, sslConfFileName);
    if (sslClient == null) {
      LOG.warn("SSL Client config file not found. Was looking for " + sslConfFileName +
          " in " + Arrays.toString(cacheFiles));
      return;
    }
    sslConfig.addResource(sslClient);

    String trustStoreFile = conf.get("ssl.client.truststore.location");
    Path trustStorePath = findCacheFile(cacheFiles, trustStoreFile);
    sslConfig.set("ssl.client.truststore.location", trustStorePath.toString());

    String keyStoreFile = conf.get("ssl.client.keystore.location");
    Path keyStorePath = findCacheFile(cacheFiles, keyStoreFile);
    sslConfig.set("ssl.client.keystore.location", keyStorePath.toString());

    try {
      OutputStream out = new FileOutputStream(workDir + "/" + sslConfFileName);
      try {
        sslConfig.writeXml(out);
      } finally {
        out.close();
      }
      conf.set(DistCpConstants.CONF_LABEL_SSL_KEYSTORE, sslConfFileName);
    } catch (IOException e) {
      LOG.warn("Unable to write out the ssl configuration. " +
          "Will fall back to default ssl-client.xml in class path, if there is one", e);
    }
  }

  /**
   * Find entry from distributed cache
   *
   * @param cacheFiles - All localized cache files
   * @param fileName - fileName to search
   * @return Path of the filename if found, else null
   */
  private Path findCacheFile(Path[] cacheFiles, String fileName) {
    if (cacheFiles != null && cacheFiles.length > 0) {
      for (Path file : cacheFiles) {
        if (file.getName().equals(fileName)) {
          return file;
        }
      }
    }
    return null;
  }

  /**
   * Implementation of the Mapper<>::map(). Does the copy.
   * @param relPath The target path.
   * @param sourceFileStatus The source path.
   * @throws IOException
   */
  @Override
  public void map(Text relPath, FileStatus sourceFileStatus, Context context)
          throws IOException, InterruptedException {
    Path sourcePath = sourceFileStatus.getPath();

    if (LOG.isDebugEnabled())
      LOG.debug("DistCpMapper::map(): Received " + sourcePath + ", " + relPath);

    Path target = new Path(targetWorkPath.makeQualified(targetFS.getUri(),
                          targetFS.getWorkingDirectory()) + relPath.toString());

    EnumSet<DistCpOptions.FileAttribute> fileAttributes
            = getFileAttributeSettings(context);

    final String description = "Copying " + sourcePath + " to " + target;
    context.setStatus(description);

    LOG.info(description);

    try {
      FileStatus sourceCurrStatus;
      FileSystem sourceFS;
      try {
        sourceFS = sourcePath.getFileSystem(conf);
        sourceCurrStatus = sourceFS.getFileStatus(sourcePath);
      } catch (FileNotFoundException e) {
        throw new IOException(new RetriableFileCopyCommand.CopyReadException(e));
      }

      FileStatus targetStatus = null;

      try {
        targetStatus = targetFS.getFileStatus(target);
      } catch (FileNotFoundException ignore) {
        if (LOG.isDebugEnabled())
          LOG.debug("Path could not be found: " + target, ignore);
      }

      if (targetStatus != null && (targetStatus.isDirectory() != sourceCurrStatus.isDirectory())) {
        throw new IOException("Can't replace " + target + ". Target is " +
            getFileType(targetStatus) + ", Source is " + getFileType(sourceCurrStatus));
      }

      if (sourceCurrStatus.isDirectory()) {
        createTargetDirsWithRetry(description, target, context);
        return;
      }

      if (skipFile(sourceFS, sourceCurrStatus, target)) {
        LOG.info("Skipping copy of " + sourceCurrStatus.getPath()
                 + " to " + target);
        updateSkipCounters(context, sourceCurrStatus);
        context.write(null, new Text("SKIP: " + sourceCurrStatus.getPath()));
      }
      else {
        copyFileWithRetry(description, sourceCurrStatus, target, context,
                          fileAttributes);
      }

      DistCpUtils.preserve(target.getFileSystem(conf), target,
                           sourceCurrStatus, fileAttributes);

    } catch (IOException exception) {
      handleFailures(exception, sourceFileStatus, target, context);
    }
  }

  private String getFileType(FileStatus fileStatus) {
    return fileStatus == null ? "N/A" : (fileStatus.isDirectory() ? "dir" : "file");
  }

  private static EnumSet<DistCpOptions.FileAttribute>
          getFileAttributeSettings(Mapper.Context context) {
    String attributeString = context.getConfiguration().get(
            DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel());
    return DistCpUtils.unpackAttributes(attributeString);
  }

  private void copyFileWithRetry(String description, FileStatus sourceFileStatus,
               Path target, Context context,
               EnumSet<DistCpOptions.FileAttribute> fileAttributes) throws IOException {

    long bytesCopied;
    try {
      bytesCopied = (Long)new RetriableFileCopyCommand(skipCrc, description)
                       .execute(sourceFileStatus, target, context, fileAttributes);
    } catch (Exception e) {
      context.setStatus("Copy Failure: " + sourceFileStatus.getPath());
      throw new IOException("File copy failed: " + sourceFileStatus.getPath() +
          " --> " + target, e);
    }
    incrementCounter(context, Counter.BYTESEXPECTED, sourceFileStatus.getLen());
    incrementCounter(context, Counter.BYTESCOPIED, bytesCopied);
    incrementCounter(context, Counter.COPY, 1);
  }

  private void createTargetDirsWithRetry(String description,
                   Path target, Context context) throws IOException {
    try {
      new RetriableDirectoryCreateCommand(description).execute(target, context);
    } catch (Exception e) {
      throw new IOException("mkdir failed for " + target, e);
    }
    incrementCounter(context, Counter.COPY, 1);
  }

  private static void updateSkipCounters(Context context,
                                         FileStatus sourceFile) {
    incrementCounter(context, Counter.SKIP, 1);
    incrementCounter(context, Counter.BYTESSKIPPED, sourceFile.getLen());

  }

  private void handleFailures(IOException exception,
                                     FileStatus sourceFileStatus, Path target,
                                     Context context) throws IOException, InterruptedException {
    LOG.error("Failure in copying " + sourceFileStatus.getPath() + " to " +
                target, exception);

    if (ignoreFailures && exception.getCause() instanceof
            RetriableFileCopyCommand.CopyReadException) {
      incrementCounter(context, Counter.FAIL, 1);
      incrementCounter(context, Counter.BYTESFAILED, sourceFileStatus.getLen());
      context.write(null, new Text("FAIL: " + sourceFileStatus.getPath() + " - " + 
          StringUtils.stringifyException(exception)));
    }
    else
      throw exception;
  }

  private static void incrementCounter(Context context, Counter counter,
                                       long value) {
    context.getCounter(counter).increment(value);
  }

  private boolean skipFile(FileSystem sourceFS, FileStatus source, Path target)
                                          throws IOException {
    return     targetFS.exists(target)
            && !overWrite
            && !mustUpdate(sourceFS, source, target);
  }

  private boolean mustUpdate(FileSystem sourceFS, FileStatus source, Path target)
                                    throws IOException {
    final FileStatus targetFileStatus = targetFS.getFileStatus(target);

    return     syncFolders
            && (
                   targetFileStatus.getLen() != source.getLen()
                || (!skipCrc &&
                       !DistCpUtils.checksumsAreEqual(sourceFS,
                                          source.getPath(), targetFS, target))
                || (source.getBlockSize() != targetFileStatus.getBlockSize() &&
                      preserve.contains(FileAttribute.BLOCKSIZE))
               );
  }
}

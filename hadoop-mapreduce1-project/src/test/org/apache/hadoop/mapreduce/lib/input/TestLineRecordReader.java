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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import junit.framework.TestCase;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.tools.ant.util.FileUtils;
import org.junit.Test;

public class TestLineRecordReader extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestLineRecordReader.class.getName());

  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestTextInputFormat");
  private static Path inputDir = new Path(workDir, "input");
  private static Path outputDir = new Path(workDir, "output");

  private void testSplitRecords(String testFileName, long firstSplitLength)
      throws IOException {
    Path localCachePath = new Path(System.getProperty("test.cache.data"));
    Path txtPath = new Path(localCachePath, new Path(testFileName));
    File testFile = new File(txtPath.toString());
    long testFileSize = testFile.length();
    Path testFilePath = new Path(testFile.getAbsolutePath());
    Configuration conf = new Configuration();
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    assertTrue("unexpected test data at " + testFile,
        testFileSize > firstSplitLength);

    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    // read the data without splitting to count the records
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader();
    reader.initialize(split, context);
    int numRecordsNoSplits = 0;
    while (reader.nextKeyValue()) {
      ++numRecordsNoSplits;
    }
    reader.close();

    // count the records in the first split
    split = new FileSplit(testFilePath, 0, firstSplitLength, (String[])null);
    reader = new LineRecordReader();
    reader.initialize(split, context);
    int numRecordsFirstSplit = 0;
    while (reader.nextKeyValue()) {
      ++numRecordsFirstSplit;
    }
    reader.close();

    // count the records in the second split
    split = new FileSplit(testFilePath, firstSplitLength,
        testFileSize - firstSplitLength, (String[])null);
    reader = new LineRecordReader();
    reader.initialize(split, context);
    int numRecordsRemainingSplits = 0;
    while (reader.nextKeyValue()) {
      ++numRecordsRemainingSplits;
    }
    reader.close();

    assertEquals("Unexpected number of records in bzip2 compressed split",
        numRecordsNoSplits, numRecordsFirstSplit + numRecordsRemainingSplits);
  }

  @Test
  public void testBzip2SplitEndsAtCR() throws IOException {
    // the test data contains a carriage-return at the end of the first
    // split which ends at compressed offset 136498 and the next
    // character is not a linefeed
    testSplitRecords("blockEndingInCR.txt.bz2", 136498);
  }

  @Test
  public void testBzip2SplitEndsAtCRThenLF() throws IOException {
    // the test data contains a carriage-return at the end of the first
    // split which ends at compressed offset 136498 and the next
    // character is a linefeed
    testSplitRecords("blockEndingInCRThenLF.txt.bz2", 136498);
  }

  //This test ensures record reader doesn't lose records when it starts
  //exactly at the starting byte of a bz2 compressed block
  @Test
  public void testBzip2SplitStartAtBlockMarker() throws IOException {
    //136504 in blockEndingInCR.txt.bz2 is the byte at which the bz2 block ends
    //In the following test cases record readers should iterate over all the records
    //and should not miss any record.

    //Start next split at just the start of the block.
    testSplitRecords("blockEndingInCR.txt.bz2", 136504);

    //Start next split a byte forward in next block.
    testSplitRecords("blockEndingInCR.txt.bz2", 136505);

    //Start next split 3 bytes forward in next block.
    testSplitRecords("blockEndingInCR.txt.bz2", 136508);

    //Start next split 10 bytes from behind the end marker.
    testSplitRecords("blockEndingInCR.txt.bz2", 136494);
  }

  /**
   * Writes the input test file
   *
   * @param conf
   * @throws IOException
   */
  public void createInputFile(Configuration conf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(inputDir, "test.txt");
    Writer writer = new OutputStreamWriter(localFs.create(file));
    writer.write("abc\ndef\t\nghi\njkl");
    writer.close();
  }

  /**
   * Reads the output file into a string
   *
   * @param conf
   * @return
   * @throws IOException
   */
  public String readOutputFile(Configuration conf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(outputDir, "part-r-00000");
    Reader reader = new InputStreamReader(localFs.open(file));
    String r = FileUtils.readFully(reader);
    reader.close();
    return r;
  }

  /**
   * Creates and runs an MR job
   *
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public void createAndRunJob(Configuration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    Job job = new Job(conf, "testLineRecordReader");
    job.setJarByClass(TestLineRecordReader.class);
    job.setMapperClass(Mapper.class);
    job.setReducerClass(Reducer.class);
    FileInputFormat.addInputPath(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.waitForCompletion(true);
  }

  /**
   * Test the case when a custom record delimiter is specified using the
   * textinputformat.record.delimiter configuration property
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testCustomRecordDelimiters() throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    conf.set("textinputformat.record.delimiter", "\t\n");
    FileSystem localFs = FileSystem.getLocal(conf);
    // cleanup
    localFs.delete(workDir, true);
    // creating input test file
    createInputFile(conf);
    createAndRunJob(conf);
    String expected = "0\tabc\ndef\n9\tghi\njkl\n";
    this.assertEquals(expected, readOutputFile(conf));
  }

  /**
   * Test the default behavior when the textinputformat.record.delimiter
   * configuration property is not specified
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testDefaultRecordDelimiters() throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    // cleanup
    localFs.delete(workDir, true);
    // creating input test file
    createInputFile(conf);
    createAndRunJob(conf);
    String expected = "0\tabc\n4\tdef\t\n9\tghi\n13\tjkl\n";
    this.assertEquals(expected, readOutputFile(conf));
  }

  /**
   * Test whether BOM is skipped
   *
   * @throws IOException
   */
  @Test
  public void testStripBOM() throws IOException {
    LOG.info("testStripBOM");
    // the test data contains a BOM at the start of the file
    // confirm the BOM is skipped by LineRecordReader
    String UTF8_BOM = "\uFEFF";
    Path localCachePath = new Path(System.getProperty("test.cache.data"));
    Path txtPath = new Path(localCachePath, new Path("testBOM.txt"));
    LOG.info(txtPath.toString());
    File testFile = new File(txtPath.toString());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
        new TaskAttemptID());

    // read the data and check whether BOM is skipped
    FileSplit split = new FileSplit(txtPath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader();
    reader.initialize(split, context);
    int numRecords = 0;
    boolean firstLine = true;
    boolean skipBOM = true;
    String prevVal = null;
    while (reader.nextKeyValue()) {
      if (firstLine) {
        firstLine = false;
        if (reader.getCurrentValue().toString().startsWith(UTF8_BOM)) {
          skipBOM = false;
        }
      } else {
        assertEquals("not same text", prevVal,
            reader.getCurrentValue().toString());
      }
      prevVal = new String(reader.getCurrentValue().toString());
      ++numRecords;
    }
    reader.close();

    assertTrue("BOM is not skipped", skipBOM);
  }
}

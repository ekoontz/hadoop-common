/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.tools.ant.util.FileUtils;
import org.junit.Test;

public class TestLineRecordReader extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestLineRecordReader.class.getName());

  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "."), "data"), "TestTextInputFormat");
  private static Path inputDir = new Path(workDir, "input");
  private static Path outputDir = new Path(workDir, "output");

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
    Path file = new Path(outputDir, "part-00000");
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
    JobConf job = new JobConf(conf);
    job.setJarByClass(TestLineRecordReader.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(IdentityReducer.class);
    FileInputFormat.addInputPath(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    JobClient.runJob(job);
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
    Path localCachePath = new Path(System.getProperty("test.cache.data"));
    Path txtPath = new Path(localCachePath, new Path("testBOM.txt"));
    String UTF8_BOM = "\uFEFF";
    LOG.info(txtPath.toString());
    File testFile = new File(txtPath.toString());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

    // read the data and check whether BOM is skipped
    FileSplit split = new FileSplit(txtPath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split);
    LongWritable key = new LongWritable();
    Text value = new Text();
    int numRecords = 0;
    boolean firstLine = true;
    boolean skipBOM = true;
    String prevVal = null;
    while (reader.next(key, value)) {
      if (firstLine) {
        firstLine = false;
        if (value.toString().startsWith(UTF8_BOM)) {
          skipBOM = false;
        }
      } else {
        assertEquals("not same text", prevVal, value.toString());
      }
      prevVal = new String(value.toString());
      ++numRecords;
    }
    reader.close();

    assertTrue("BOM is not skipped", skipBOM);
  }

  private void testSplitRecords(String testFileName, long firstSplitLength)
      throws IOException {
    Path localCachePath = new Path(System.getProperty("test.cache.data"));
    Path txtPath = new Path(localCachePath, new Path(testFileName));
    File testFile = new File(txtPath.toString());
    long testFileSize = testFile.length();
    Path testFilePath = new Path(testFile.getAbsolutePath());
    Configuration conf = new Configuration();
    testSplitRecordsForFile(conf, firstSplitLength,
        testFileSize, testFilePath);
  }

  private void testSplitRecordsForFile(Configuration conf,
      long firstSplitLength, long testFileSize, Path testFilePath)
      throws IOException {
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    assertTrue("unexpected test data at " + testFilePath,
        testFileSize > firstSplitLength);

    String delimiter = conf.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }

    // read the data without splitting to count the records
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split,
        recordDelimiterBytes);
    LongWritable key = new LongWritable();
    Text value = new Text();
    int numRecordsNoSplits = 0;
    while (reader.next(key, value)) {
      ++numRecordsNoSplits;
    }
    reader.close();
    // count the records in the first split
    split = new FileSplit(testFilePath, 0, firstSplitLength, (String[])null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    int numRecordsFirstSplit = 0;
    while (reader.next(key,  value)) {
      ++numRecordsFirstSplit;
    }
    reader.close();
    // count the records in the second split
    split = new FileSplit(testFilePath, firstSplitLength,
        testFileSize - firstSplitLength, (String[])null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    int numRecordsRemainingSplits = 0;
    while (reader.next(key,  value)) {
      ++numRecordsRemainingSplits;
    }
    reader.close();
    assertEquals("Unexpected number of records in split",
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

  @Test
  public void testMultipleClose() throws IOException {
    Path localCachePath = new Path(System.getProperty("test.cache.data"));
    Path txtPath = new Path(localCachePath,
        new Path("blockEndingInCR.txt.bz2"));
    File testFile = new File(txtPath.toString());
    Path testFilePath = new Path(testFile.getAbsolutePath());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);

    LineRecordReader reader = new LineRecordReader(conf, split);
    LongWritable key = new LongWritable();
    Text value = new Text();
    //noinspection StatementWithEmptyBody
    while (reader.next(key, value)) ;
    reader.close();
    reader.close();

    BZip2Codec codec = new BZip2Codec();
    codec.setConf(conf);
    Set<Decompressor> decompressors = new HashSet<Decompressor>();
    for (int i = 0; i < 10; ++i) {
      decompressors.add(CodecPool.getDecompressor(codec));
    }
    assertEquals(10, decompressors.size());
  }

  /**
   * Writes the input test file
   *
   * @param conf
   * @return Path of the file created
   * @throws IOException
   */
  private Path createInputFile(Configuration conf, String data)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(inputDir, "test.txt");
    Writer writer = new OutputStreamWriter(localFs.create(file));
    try {
      writer.write(data);
    } finally {
      writer.close();
    }
    return file;
  }

  @Test
  public void testUncompressedInput() throws Exception {
    Configuration conf = new Configuration();
    // single char delimiter, best case
    String inputData = "abc+def+ghi+jkl+mno+pqr+stu+vw +xyz";
    Path inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter, best case
    inputData = "abc|+|def|+|ghi|+|jkl|+|mno|+|pqr|+|stu|+|vw |+|xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "|+|");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // single char delimiter with empty records
    inputData = "abc+def++ghi+jkl++mno+pqr++stu+vw ++xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with empty records
    inputData = "abc|+||+|defghi|+|jkl|+||+|mno|+|pqr|+||+|stu|+|vw |+||+|xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "|+|");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with starting part of the delimiter in the data
    inputData = "abc+def+-ghi+jkl+-mno+pqr+-stu+vw +-xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+-");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with newline as start of the delimiter
    inputData = "abc\n+def\n+ghi\n+jkl\n+mno";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "\n+");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with newline in delimiter and in data
    inputData = "abc\ndef+\nghi+\njkl\nmno";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+\n");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
  }

  @Test
  public void testUncompressedInputContainingCRLF() throws Exception {
    Configuration conf = new Configuration();
    String inputData = "a\r\nb\rc\nd\r\n";
    Path inputFile = createInputFile(conf, inputData);
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(),
            inputFile);
      }
    }
  }

  @Test
  public void testUncompressedInputCustomDelimiterPosValue()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", 10);
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    String inputData = "abcdefghij++kl++mno";
    Path inputFile = createInputFile(conf, inputData);
    String delimiter = "++";
    byte[] recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    // the first split must contain two records to make sure that it also pulls
    // in the record from the 2nd split
    int splitLength = 15;
    FileSplit split = new FileSplit(inputFile, 0, splitLength, (String[]) null);
    LineRecordReader reader = new LineRecordReader(conf, split,
        recordDelimiterBytes);
    LongWritable key = new LongWritable();
    Text value = new Text();
    // Get first record: "abcdefghij"
    assertTrue("Expected record got nothing", reader.next(key, value));
    assertEquals("Wrong length for record value", 10, value.getLength());
    // Position should be 12 right after "abcdefghij++"
    assertEquals("Wrong position after record read", 12, reader.getPos());
    // Get second record: "kl"
    assertTrue("Expected record got nothing", reader.next(key, value));
    assertEquals("Wrong length for record value", 2, value.getLength());
    // Position should be 16 right after "abcdefghij++kl++"
    assertEquals("Wrong position after record read", 16, reader.getPos());
    // Get third record: "mno"
    assertTrue("Expected record got nothing", reader.next(key, value));
    assertEquals("Wrong length for record value", 3, value.getLength());
    // Position should be 19 right after "abcdefghij++kl++mno"
    assertEquals("Wrong position after record read", 19, reader.getPos());
    assertFalse(reader.next(key, value));
    assertEquals("Wrong position after record read", 19, reader.getPos());
    reader.close();
    // No record is in the second split because the second split will drop
    // the first record, which was already reported by the first split.
    split = new FileSplit(inputFile, splitLength,
        inputData.length() - splitLength, (String[]) null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    // The position should be 19 right after "abcdefghij++kl++mno" and should
    // not change
    assertEquals("Wrong position after record read", 19, reader.getPos());
    assertFalse("Unexpected record returned", reader.next(key, value));
    assertEquals("Wrong position after record read", 19, reader.getPos());
    reader.close();

    // multi char delimiter with starting part of the delimiter in the data
    inputData = "abcd+efgh++ijk++mno";
    inputFile = createInputFile(conf, inputData);
    splitLength = 5;
    split = new FileSplit(inputFile, 0, splitLength, (String[]) null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    // Get first record: "abcd+efgh"
    assertTrue("Expected record got nothing", reader.next(key, value));
    assertEquals("Wrong position after record read", 11, reader.getPos());
    assertEquals("Wrong length for record value", 9, value.getLength());
    // should have jumped over the delimiter, no record
    assertFalse("Unexpected record returned", reader.next(key, value));
    assertEquals("Wrong position after record read", 11, reader.getPos());
    reader.close();
    // next split: check for duplicate or dropped records
    split = new FileSplit(inputFile, splitLength,
        inputData.length() - splitLength, (String[]) null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    // Get second record: "ijk" first in this split
    assertTrue("Expected record got nothing", reader.next(key, value));
    assertEquals("Wrong position after record read", 16, reader.getPos());
    assertEquals("Wrong length for record value", 3, value.getLength());
    // Get third record: "mno" second in this split
    assertTrue("Expected record got nothing", reader.next(key, value));
    assertEquals("Wrong position after record read", 19, reader.getPos());
    assertEquals("Wrong length for record value", 3, value.getLength());
    // should be at the end of the input
    assertFalse(reader.next(key, value));
    assertEquals("Wrong position after record read", 19, reader.getPos());
    reader.close();

    inputData = "abcd|efgh|+|ij|kl|+|mno|pqr";
    inputFile = createInputFile(conf, inputData);
    delimiter = "|+|";
    recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    // walking over the buffer and split sizes checks for proper processing
    // of the ambiguous bytes of the delimiter
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        split = new FileSplit(inputFile, 0, bufferSize, (String[]) null);
        reader = new LineRecordReader(conf, split, recordDelimiterBytes);
        // Get first record: "abcd|efgh" always possible
        assertTrue("Expected record got nothing", reader.next(key, value));
        assertTrue("abcd|efgh".equals(value.toString()));
        assertEquals("Wrong position after record read", 9, value.getLength());
        // Position should be 12 right after "|+|"
        int recordPos = 12;
        assertEquals("Wrong position after record read", recordPos,
            reader.getPos());
        // get the next record: "ij|kl" if the split/buffer allows it
        if (reader.next(key, value)) {
          // check the record info: "ij|kl"
          assertTrue("ij|kl".equals(value.toString()));
          // Position should be 20 right after "|+|"
          recordPos = 20;
          assertEquals("Wrong position after record read", recordPos,
              reader.getPos());
        }
        // get the third record: "mno|pqr" if the split/buffer allows it
        if (reader.next(key, value)) {
          // check the record info: "mno|pqr"
          assertTrue("mno|pqr".equals(value.toString()));
          // Position should be 27 at the end of the string now
          recordPos = inputData.length();
          assertEquals("Wrong position after record read", recordPos,
              reader.getPos());
        }
        // no more records can be read we should still be at the last position
        assertFalse("Unexpected record returned", reader.next(key, value));
        assertEquals("Wrong position after record read", recordPos,
            reader.getPos());
        reader.close();
      }
    }
  }

  @Test
  public void testUncompressedInputDefaultDelimiterPosValue()
      throws Exception {
    Configuration conf = new Configuration();
    String inputData = "1234567890\r\n12\r\n345";
    Path inputFile = createInputFile(conf, inputData);
    conf.setInt("io.file.buffer.size", 10);
    conf.setInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    FileSplit split = new FileSplit(inputFile, 0, 15, (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split,
        null);
    LongWritable key = new LongWritable();
    Text value = new Text();
    reader.next(key, value);
    // Get first record:"1234567890"
    assertEquals(10, value.getLength());
    // Position should be 12 right after "1234567890\r\n"
    assertEquals(12, reader.getPos());
    reader.next(key, value);
    // Get second record:"12"
    assertEquals(2, value.getLength());
    // Position should be 16 right after "1234567890\r\n12\r\n"
    assertEquals(16, reader.getPos());
    assertFalse(reader.next(key, value));

    split = new FileSplit(inputFile, 15, 4, (String[])null);
    reader = new LineRecordReader(conf, split, null);
    // The second split dropped the first record "\n"
    // The position should be 16 right after "1234567890\r\n12\r\n"
    assertEquals(16, reader.getPos());
    reader.next(key, value);
    // Get third record:"345"
    assertEquals(3, value.getLength());
    // Position should be 19 right after "1234567890\r\n12\r\n345"
    assertEquals(19, reader.getPos());
    assertFalse(reader.next(key, value));
    assertEquals(19, reader.getPos());

    inputData = "123456789\r\r\n";
    inputFile = createInputFile(conf, inputData);
    split = new FileSplit(inputFile, 0, 12, (String[])null);
    reader = new LineRecordReader(conf, split, null);
    reader.next(key, value);
    // Get first record:"123456789"
    assertEquals(9, value.getLength());
    // Position should be 10 right after "123456789\r"
    assertEquals(10, reader.getPos());
    reader.next(key, value);
    // Get second record:""
    assertEquals(0, value.getLength());
    // Position should be 12 right after "123456789\r\r\n"
    assertEquals(12, reader.getPos());
    assertFalse(reader.next(key, value));
    assertEquals(12, reader.getPos());
  }
}

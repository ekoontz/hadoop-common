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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.FilterInputStream;

import java.lang.Void;

import java.net.HttpURLConnection;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapreduce.TaskID;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test that the Fetcher does what we expect it to.
 */
public class TestFetcher {
  private static final Log LOG = LogFactory.getLog(TestFetcher.class);
  JobConf job = null;
  TaskAttemptID id = null;
  ShuffleSchedulerImpl<Text, Text> ss = null;
  MergeManagerImpl<Text, Text> mm = null;
  Reporter r = null;
  ShuffleClientMetrics metrics = null;
  ExceptionReporter except = null;
  SecretKey key = null;
  HttpURLConnection connection = null;
  Counters.Counter allErrs = null;

  final String encHash = "vFE234EIFCiBgYs2tCXY/SjT8Kg=";
  final MapHost host = new MapHost("localhost", "http://localhost:8080/");
  final TaskAttemptID map1ID = TaskAttemptID.forName("attempt_0_1_m_1_1");
  final TaskAttemptID map2ID = TaskAttemptID.forName("attempt_0_1_m_2_1");

  @Rule public TestName name = new TestName();

  @Before
  @SuppressWarnings("unchecked") // mocked generics
  public void setup() {
    LOG.info(">>>> " + name.getMethodName());
    job = new JobConf();
    id = TaskAttemptID.forName("attempt_0_1_r_1_1");
    ss = mock(ShuffleSchedulerImpl.class);
    mm = mock(MergeManagerImpl.class);
    r = mock(Reporter.class);
    metrics = mock(ShuffleClientMetrics.class);
    except = mock(ExceptionReporter.class);
    key = JobTokenSecretManager.createSecretKey(new byte[]{0,0,0,0});
    connection = mock(HttpURLConnection.class);

    allErrs = mock(Counters.Counter.class);
    when(r.getCounter(anyString(), anyString())).thenReturn(allErrs);

    ArrayList<TaskAttemptID> maps = new ArrayList<TaskAttemptID>(1);
    maps.add(map1ID);
    maps.add(map2ID);
    when(ss.getMapsForHost(host)).thenReturn(maps);
  }

  @After
  public void teardown() {
    LOG.info("<<<< " + name.getMethodName());
  }
  
  @Test
  public void testReduceOutOfDiskSpace() throws Throwable {
    LOG.info("testReduceOutOfDiskSpace");
    
    Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection);

    String replyHash = SecureShuffleUtils.generateHash(encHash.getBytes(), key);
    ShuffleHeader header = new ShuffleHeader(map1ID.toString(), 10, 10, 1);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    header.write(new DataOutputStream(bout));

    ByteArrayInputStream in = new ByteArrayInputStream(bout.toByteArray());
    
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
    .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
    .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    when(connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH))
    .thenReturn(replyHash);
    when(connection.getInputStream()).thenReturn(in);
    
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
    .thenThrow(new DiskErrorException("No disk space available"));
  
    underTest.copyFromHost(host);
    verify(ss).reportLocalError(any(IOException.class));
  }
  
  @Test(timeout=30000)
  public void testCopyFromHostConnectionTimeout() throws Exception {
    when(connection.getInputStream()).thenThrow(
        new SocketTimeoutException("This is a fake timeout :)"));
    
    Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection);

    underTest.copyFromHost(host);
    
    verify(connection).addRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    
    verify(allErrs).increment(1);
    verify(ss).copyFailed(map1ID, host, false, false);
    verify(ss).copyFailed(map2ID, host, false, false);
    
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map1ID));
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map2ID));
  }
  
  @Test
  public void testCopyFromHostBogusHeader() throws Exception {
    Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection);

    String replyHash = SecureShuffleUtils.generateHash(encHash.getBytes(), key);
    
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    when(connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH))
        .thenReturn(replyHash);
    ByteArrayInputStream in = new ByteArrayInputStream(
        "\u00010 BOGUS DATA\nBOGUS DATA\nBOGUS DATA\n".getBytes());
    when(connection.getInputStream()).thenReturn(in);
    
    underTest.copyFromHost(host);
    
    verify(connection).addRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    
    verify(allErrs).increment(1);
    verify(ss).copyFailed(map1ID, host, true, false);
    verify(ss).copyFailed(map2ID, host, true, false);
    
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map1ID));
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map2ID));
  }

  @Test
  public void testCopyFromHostIncompatibleShuffleVersion() throws Exception {
    String replyHash = SecureShuffleUtils.generateHash(encHash.getBytes(), key);
    
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        .thenReturn("mapreduce").thenReturn("other").thenReturn("other");
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
        .thenReturn("1.0.1").thenReturn("1.0.0").thenReturn("1.0.1");
    when(connection.getHeaderField(
        SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH)).thenReturn(replyHash);
    ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
    when(connection.getInputStream()).thenReturn(in);

    for (int i = 0; i < 3; ++i) {
      Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
          r, metrics, except, key, connection);
      underTest.copyFromHost(host);
    }
    
    verify(connection, times(3)).addRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    
    verify(allErrs, times(3)).increment(1);
    verify(ss, times(3)).copyFailed(map1ID, host, false, false);
    verify(ss, times(3)).copyFailed(map2ID, host, false, false);
    
    verify(ss, times(3)).putBackKnownMapOutput(any(MapHost.class), eq(map1ID));
    verify(ss, times(3)).putBackKnownMapOutput(any(MapHost.class), eq(map2ID));
  }

  @Test
  public void testCopyFromHostWait() throws Exception {
    Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection);

    String replyHash = SecureShuffleUtils.generateHash(encHash.getBytes(), key);
    
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH))
        .thenReturn(replyHash);
    ShuffleHeader header = new ShuffleHeader(map1ID.toString(), 10, 10, 1);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    header.write(new DataOutputStream(bout));
    ByteArrayInputStream in = new ByteArrayInputStream(bout.toByteArray());
    when(connection.getInputStream()).thenReturn(in);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    //Defaults to null, which is what we want to test
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(null);
    
    underTest.copyFromHost(host);
    
    verify(connection)
        .addRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH, 
          encHash);
    verify(allErrs, never()).increment(1);
    verify(ss, never()).copyFailed(map1ID, host, true, false);
    verify(ss, never()).copyFailed(map2ID, host, true, false);
    
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map1ID));
    verify(ss).putBackKnownMapOutput(any(MapHost.class), eq(map2ID));
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout=10000) 
  public void testCopyFromHostCompressFailure() throws Exception {
    InMemoryMapOutput<Text, Text> immo = mock(InMemoryMapOutput.class);

    Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection);

    String replyHash = SecureShuffleUtils.generateHash(encHash.getBytes(), key);
    
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH))
        .thenReturn(replyHash);
    ShuffleHeader header = new ShuffleHeader(map1ID.toString(), 10, 10, 1);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    header.write(new DataOutputStream(bout));
    ByteArrayInputStream in = new ByteArrayInputStream(bout.toByteArray());
    when(connection.getInputStream()).thenReturn(in);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(immo);
    
    doThrow(new java.lang.InternalError()).when(immo)
        .shuffle(any(MapHost.class), any(InputStream.class), anyLong(), 
            anyLong(), any(ShuffleClientMetrics.class), any(Reporter.class));

    underTest.copyFromHost(host);
       
    verify(connection)
        .addRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH, 
          encHash);
    verify(ss, times(1)).copyFailed(map1ID, host, true, false);
  }

  @Test(timeout=10000)
  public void testInterruptInMemory() throws Exception {
    final int FETCHER = 2;
    InMemoryMapOutput<Text,Text> immo = spy(new InMemoryMapOutput<Text,Text>(
          job, id, mm, 100, null, true));
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(immo);
    doNothing().when(mm).waitForResource();
    when(ss.getHost()).thenReturn(host);

    String replyHash = SecureShuffleUtils.generateHash(encHash.getBytes(), key);
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    when(connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH))
        .thenReturn(replyHash);
    ShuffleHeader header = new ShuffleHeader(map1ID.toString(), 10, 10, 1);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    header.write(new DataOutputStream(bout));
    final StuckInputStream in =
        new StuckInputStream(new ByteArrayInputStream(bout.toByteArray()));
    when(connection.getInputStream()).thenReturn(in);
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock ignore) throws IOException {
        in.close();
        return null;
      }
    }).when(connection).disconnect();

    Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection, FETCHER);
    underTest.start();
    // wait for read in inputstream
    in.waitForFetcher();
    underTest.shutDown();
    underTest.join(); // rely on test timeout to kill if stuck

    assertTrue(in.wasClosedProperly());
    verify(immo).abort();
  }

  @Test(timeout=10000)
  public void testInterruptOnDisk() throws Exception {
    final int FETCHER = 7;
    Path p = new Path("file:///tmp/foo");
    Path pTmp = OnDiskMapOutput.getTempPath(p, FETCHER);
    FileSystem mFs = mock(FileSystem.class, RETURNS_DEEP_STUBS);
    MapOutputFile mof = mock(MapOutputFile.class);
    when(mof.getInputFileForWrite(any(TaskID.class), anyLong())).thenReturn(p);
    OnDiskMapOutput<Text,Text> odmo = spy(new OnDiskMapOutput<Text,Text>(map1ID,
        id, mm, 100L, job, mof, FETCHER, true, mFs, p));
    when(mm.reserve(any(TaskAttemptID.class), anyLong(), anyInt()))
        .thenReturn(odmo);
    doNothing().when(mm).waitForResource();
    when(ss.getHost()).thenReturn(host);

    String replyHash = SecureShuffleUtils.generateHash(encHash.getBytes(), key);
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getHeaderField(
        SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH)).thenReturn(replyHash);
    ShuffleHeader header = new ShuffleHeader(map1ID.toString(), 10, 10, 1);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    header.write(new DataOutputStream(bout));
    final StuckInputStream in =
        new StuckInputStream(new ByteArrayInputStream(bout.toByteArray()));
    when(connection.getInputStream()).thenReturn(in);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
        .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock ignore) throws IOException {
        in.close();
        return null;
      }
    }).when(connection).disconnect();

    Fetcher<Text,Text> underTest = new FakeFetcher<Text,Text>(job, id, ss, mm,
        r, metrics, except, key, connection, FETCHER);
    underTest.start();
    // wait for read in inputstream
    in.waitForFetcher();
    underTest.shutDown();
    underTest.join(); // rely on test timeout to kill if stuck

    assertTrue(in.wasClosedProperly());
    verify(mFs).create(eq(pTmp));
    verify(mFs).delete(eq(pTmp), eq(false));
    verify(odmo).abort();
  }

  public static class FakeFetcher<K,V> extends Fetcher<K,V> {

    public FakeFetcher(JobConf job, TaskAttemptID reduceId,
        ShuffleSchedulerImpl<K,V> scheduler, MergeManagerImpl<K,V> merger,
        Reporter reporter, ShuffleClientMetrics metrics,
        ExceptionReporter exceptionReporter, SecretKey jobTokenSecret,
        HttpURLConnection connection) {
      super(job, reduceId, scheduler, merger, reporter, metrics,
          exceptionReporter, jobTokenSecret);
      this.connection = connection;
    }

    public FakeFetcher(JobConf job, TaskAttemptID reduceId,
        ShuffleSchedulerImpl<K,V> scheduler, MergeManagerImpl<K,V> merger,
        Reporter reporter, ShuffleClientMetrics metrics,
        ExceptionReporter exceptionReporter, SecretKey jobTokenSecret,
        HttpURLConnection connection, int id) {
      super(job, reduceId, scheduler, merger, reporter, metrics,
          exceptionReporter, jobTokenSecret, id);
      this.connection = connection;
    }

    @Override
    protected void openConnection(URL url) throws IOException {
      if (null == connection) {
        super.openConnection(url);
      }
      // already 'opened' the mocked connection
      return;
    }
  }

  static class StuckInputStream extends FilterInputStream {

    boolean stuck = false;
    volatile boolean closed = false;

    StuckInputStream(InputStream inner) {
      super(inner);
    }

    int freeze() throws IOException {
      synchronized (this) {
        stuck = true;
        notify();
      }
      // connection doesn't throw InterruptedException, but may return some
      // bytes geq 0 or throw an exception
      while (!Thread.currentThread().isInterrupted() || closed) {
        // spin
        if (closed) {
          throw new IOException("underlying stream closed, triggered an error");
        }
      }
      return 0;
    }

    @Override
    public int read() throws IOException {
      int ret = super.read();
      if (ret != -1) {
        return ret;
      }
      return freeze();
    }

    @Override
    public int read(byte[] b) throws IOException {
      int ret = super.read(b);
      if (ret != -1) {
        return ret;
      }
      return freeze();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int ret = super.read(b, off, len);
      if (ret != -1) {
        return ret;
      }
      return freeze();
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    public synchronized void waitForFetcher() throws InterruptedException {
      while (!stuck) {
        wait();
      }
    }

    public boolean wasClosedProperly() {
      return closed;
    }

  }

}

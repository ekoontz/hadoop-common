/*
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
package org.apache.hadoop.mapred;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.test.MockitoMaker.make;
import static org.apache.hadoop.test.MockitoMaker.stub;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

public class TestShuffleHandler {
  static final long MiB = 1024 * 1024;

  @Test public void testSerializeMeta()  throws Exception {
    assertEquals(1, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(1)));
    assertEquals(-1, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(-1)));
    assertEquals(8080, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(8080)));
  }

  @Test public void testShuffleMetrics() throws Exception {
    MetricsSystem ms = new MetricsSystemImpl();
    ShuffleHandler sh = new ShuffleHandler(ms);
    ChannelFuture cf = make(stub(ChannelFuture.class).
        returning(true, false).from.isSuccess());

    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(1*MiB);
    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(2*MiB);

    checkShuffleMetrics(ms, 3*MiB, 0 , 0, 2);

    sh.metrics.operationComplete(cf);
    sh.metrics.operationComplete(cf);

    checkShuffleMetrics(ms, 3*MiB, 1, 1, 0);
  }

  static void checkShuffleMetrics(MetricsSystem ms, long bytes, int failed,
                                  int succeeded, int connections) {
    MetricsSource source = ms.getSource("ShuffleMetrics");
    MetricsRecordBuilder rb = getMetrics(source);
    assertCounter("ShuffleOutputBytes", bytes, rb);
    assertCounter("ShuffleOutputsFailed", failed, rb);
    assertCounter("ShuffleOutputsOK", succeeded, rb);
    assertGauge("ShuffleConnections", connections, rb);
  }

  @Test
  public void testClientClosesConnection() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
                  throws IOException {
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
              Channel ch, String user, String jobId, String mapId, int reduce)
                  throws IOException {
            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i=0; i<100000; ++i) {
              header.write(dob);
            }
            return ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.getChannel().close();
            }
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.getChannel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // simulate a reducer that closes early by reading a single shuffle header
    // then closing the connection
    URL url = new URL("http://127.0.0.1:"
      + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
      + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
    conn.connect();
    DataInputStream input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    input.close();

    shuffleHandler.stop();
    Assert.assertTrue("sendError called when client closed connection",
        failures.size() == 0);
  }
}

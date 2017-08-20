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
import java.nio.ByteBuffer;

/**
 * Implementers of this interface provide a read API that writes to a
 * ByteBuffer, not a byte[].
 */
public interface ByteBufferReadable {
  /**
   * Reads up to buf.remaining() bytes into buf. Callers should use
   * buf.limit(..) to control the size of the desired read.
   * <p/>
   * After a successful call, buf.position() and buf.limit() should be
   * unchanged, and therefore any data can be immediately read from buf.
   * buf.mark() may be cleared or updated.
   * <p/>
   * In the case of an exception, the values of buf.position() and buf.limit()
   * are undefined, and callers should be prepared to recover from this
   * eventuality.
   * <p/>
   * Many implementations will throw {@link UnsupportedOperationException}, so
   * callers that are not confident in support for this method from the
   * underlying filesystem should be prepared to handle that exception.
   * <p/>
   * Implementations should treat 0-length requests as legitimate, and must not
   * signal an error upon their receipt.
   *
   * @param buf
   *          the ByteBuffer to receive the results of the read operation. Up to
   *          buf.limit() - buf.position() bytes may be read.
   * @return the number of bytes available to read from buf
   * @throws IOException
   *           if there is some error performing the read
   */
  public int read(ByteBuffer buf) throws IOException;
}

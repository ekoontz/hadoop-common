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
package org.apache.hadoop.hdfs.server.common;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;

/****************************************************************
 * A GenerationStamp is a Hadoop FS primitive, identified by a long.
 ****************************************************************/
@InterfaceAudience.Private
public class GenerationStamp implements Comparable<GenerationStamp> {
  /**
   * The first valid generation stamp.
   */
  public static final long FIRST_VALID_STAMP = 1000L;

  /**
   * Generation stamp of blocks that pre-date the introduction
   * of a generation stamp.
   */
  public static final long GRANDFATHER_GENERATION_STAMP = 0;

  private AtomicLong genstamp = new AtomicLong();

  /**
   * Create a new instance, initialized to FIRST_VALID_STAMP.
   */
  public GenerationStamp() {
    this(GenerationStamp.FIRST_VALID_STAMP);
  }

  /**
   * Create a new instance, initialized to the specified value.
   */
  GenerationStamp(long stamp) {
    genstamp.set(stamp);
  }

  /**
   * Returns the current generation stamp
   */
  public long getStamp() {
    return genstamp.get();
  }

  /**
   * Sets the current generation stamp
   */
  public void setStamp(long stamp) {
    genstamp.set(stamp);
  }

  /**
   * First increments the counter and then returns the stamp 
   */
  public long nextStamp() {
    return genstamp.incrementAndGet();
  }

  @Override // Comparable
  public int compareTo(GenerationStamp that) {
    long stamp1 = this.genstamp.get();
    long stamp2 = that.genstamp.get();
    return stamp1 < stamp2 ? -1 :
           stamp1 > stamp2 ? 1 : 0;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (!(o instanceof GenerationStamp)) {
      return false;
    }
    return compareTo((GenerationStamp)o) == 0;
  }

  @Override // Object
  public int hashCode() {
    long stamp = genstamp.get();
    return (int) (stamp^(stamp>>>32));
  }
}

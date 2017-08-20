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
package org.apache.hadoop.nfs.nfs3.request;

import java.io.IOException;

import org.apache.hadoop.oncrpc.XDR;

/**
 * SYMLINK3 Request
 */
public class SYMLINK3Request extends RequestWithHandle {
  private final String name;     // The name of the link
  private final SetAttr3 symAttr;
  private final String symData;  // It contains the target
  
  public SYMLINK3Request(XDR xdr) throws IOException {
    super(xdr);
    name = xdr.readString();
    symAttr = new SetAttr3();
    symAttr.deserialize(xdr);
    symData = xdr.readString();
  }

  public String getName() {
    return name;
  }

  public SetAttr3 getSymAttr() {
    return symAttr;
  }

  public String getSymData() {
    return symData;
  }
}
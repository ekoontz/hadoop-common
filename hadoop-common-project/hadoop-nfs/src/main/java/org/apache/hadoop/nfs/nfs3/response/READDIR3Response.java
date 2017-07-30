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
package org.apache.hadoop.nfs.nfs3.response;

import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;

import com.google.common.collect.ObjectArrays;

/**
 * READDIR3 Response
 */
public class READDIR3Response extends NFS3Response {
  private final Nfs3FileAttributes postOpDirAttr;
  private final long cookieVerf;
  private final DirList3 dirList;

  public static class Entry3 {
    private final long fileId;
    private final String name;
    private final long cookie;
    
    public Entry3(long fileId, String name, long cookie) {
      this.fileId = fileId;
      this.name = name;
      this.cookie = cookie;
    }

    long getFileId() {
      return fileId;
    }

    String getName() {
      return name;
    }

    long getCookie() {
      return cookie;
    }
  }

  public static class DirList3 {
    final Entry3 entries[];
    final boolean eof;
    
    public DirList3(Entry3[] entries, boolean eof) {
      this.entries = ObjectArrays.newArray(entries, entries.length);
      System.arraycopy(this.entries, 0, entries, 0, entries.length);
      this.eof = eof;
    }
  }

  public READDIR3Response(int status) {
    this(status, new Nfs3FileAttributes());
  }

  public READDIR3Response(int status, Nfs3FileAttributes postOpAttr) {
    this(status, postOpAttr, 0, null); 
  }

  public READDIR3Response(int status, Nfs3FileAttributes postOpAttr,
      final long cookieVerf, final DirList3 dirList) {
    super(status);
    this.postOpDirAttr = postOpAttr;
    this.cookieVerf = cookieVerf;
    this.dirList = dirList;
  }

  public Nfs3FileAttributes getPostOpAttr() {
    return postOpDirAttr;
  }

  public long getCookieVerf() {
    return cookieVerf;
  }

  public DirList3 getDirList() {
    return dirList;
  }

  @Override
  public XDR send(XDR xdr, int xid) {
    super.send(xdr, xid);
    xdr.writeBoolean(true); // Attributes follow
    postOpDirAttr.serialize(xdr);

    if (getStatus() == Nfs3Status.NFS3_OK) {
      xdr.writeLongAsHyper(cookieVerf);
      Entry3[] f = dirList.entries;
      for (int i = 0; i < f.length; i++) {
        xdr.writeBoolean(true); // Value follows
        xdr.writeLongAsHyper(f[i].getFileId());
        xdr.writeString(f[i].getName());
        xdr.writeLongAsHyper(f[i].getCookie());
      }

      xdr.writeBoolean(false);
      xdr.writeBoolean(dirList.eof);
    }
    return xdr;
  }
}

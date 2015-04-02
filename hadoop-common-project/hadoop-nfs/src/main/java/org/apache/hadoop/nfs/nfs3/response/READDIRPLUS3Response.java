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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.Verifier;

import com.google.common.annotations.VisibleForTesting;

/**
 * READDIRPLUS3 Response
 */
public class READDIRPLUS3Response  extends NFS3Response {
  private Nfs3FileAttributes postOpDirAttr;
  private final long cookieVerf;
  private final DirListPlus3 dirListPlus;

  public static class EntryPlus3 {
    private final long fileId;
    private final String name;
    private final long cookie;
    private final Nfs3FileAttributes nameAttr;
    private final FileHandle objFileHandle;

    public EntryPlus3(long fileId, String name, long cookie,
        Nfs3FileAttributes nameAttr, FileHandle objFileHandle) {
      this.fileId = fileId;
      this.name = name;
      this.cookie = cookie;
      this.nameAttr = nameAttr;
      this.objFileHandle = objFileHandle;
    }

    @VisibleForTesting
    public String getName() {
      return name;
    }
    
    void seralize(XDR xdr) {
      xdr.writeLongAsHyper(fileId);
      xdr.writeString(name);
      xdr.writeLongAsHyper(cookie);
      xdr.writeBoolean(true);
      nameAttr.serialize(xdr);
      xdr.writeBoolean(true);
      objFileHandle.serialize(xdr);
    }
  }

  public static class DirListPlus3 {
    List<EntryPlus3> entries;
    boolean eof;
    
    public DirListPlus3(EntryPlus3[] entries, boolean eof) {
      this.entries = Collections.unmodifiableList(Arrays.asList(entries));
      this.eof = eof;
    }

    @VisibleForTesting
    public List<EntryPlus3> getEntries() {
      return entries;
    }
    
    boolean getEof() {
      return eof;
    }
  }

  @VisibleForTesting
  public DirListPlus3 getDirListPlus() {
    return dirListPlus;
  }
  
  public READDIRPLUS3Response(int status) {
    this(status, null, 0, null);
  }

  public READDIRPLUS3Response(int status, Nfs3FileAttributes postOpDirAttr,
      final long cookieVerf, final DirListPlus3 dirListPlus) {
    super(status);
    this.postOpDirAttr = postOpDirAttr;
    this.cookieVerf = cookieVerf;
    this.dirListPlus = dirListPlus;
  }
  
  @Override
  public XDR writeHeaderAndResponse(XDR out, int xid, Verifier verifier) {
    super.writeHeaderAndResponse(out, xid, verifier);
    out.writeBoolean(true); // attributes follow
    if (postOpDirAttr == null) {
      postOpDirAttr = new Nfs3FileAttributes();
    }
    postOpDirAttr.serialize(out);
    
    if (getStatus() == Nfs3Status.NFS3_OK) {
      out.writeLongAsHyper(cookieVerf);
      for (EntryPlus3 f : dirListPlus.getEntries()) {
        out.writeBoolean(true); // next
        f.seralize(out);
      }

      out.writeBoolean(false);
      out.writeBoolean(dirListPlus.getEof());
    }
    return out;
  }
}

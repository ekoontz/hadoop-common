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

#include "config.h"
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_io_compress_lz4_Lz4Compressor.h"

//****************************
// Simple Functions
//****************************

extern int LZ4_compress   (const char* source, char* dest, int isize);

/*
LZ4_compress() :
 return : the number of bytes in compressed buffer dest
 note : destination buffer must be already allocated.
  To avoid any problem, size it to handle worst cases situations (input data not compressible)
  Worst case size is : "inputsize + 0.4%", with "0.4%" being at least 8 bytes.

*/

static jfieldID Lz4Compressor_clazz;
static jfieldID Lz4Compressor_uncompressedDirectBuf;
static jfieldID Lz4Compressor_uncompressedDirectBufLen;
static jfieldID Lz4Compressor_compressedDirectBuf;
static jfieldID Lz4Compressor_directBufferSize;


JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_lz4_Lz4Compressor_initIDs
(JNIEnv *env, jclass clazz){

  Lz4Compressor_clazz = (*env)->GetStaticFieldID(env, clazz, "clazz",
                                                 "Ljava/lang/Class;");
  Lz4Compressor_uncompressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                           "uncompressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  Lz4Compressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, clazz,
                                                              "uncompressedDirectBufLen", "I");
  Lz4Compressor_compressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                         "compressedDirectBuf",
                                                         "Ljava/nio/Buffer;");
  Lz4Compressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                       "directBufferSize", "I");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_lz4_Lz4Compressor_compressBytesDirect
(JNIEnv *env, jobject thisj){
  // Get members of Lz4Compressor
  jobject clazz = (*env)->GetStaticObjectField(env, thisj, Lz4Compressor_clazz);
  jobject uncompressed_direct_buf = (*env)->GetObjectField(env, thisj, Lz4Compressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, Lz4Compressor_uncompressedDirectBufLen);
  jobject compressed_direct_buf = (*env)->GetObjectField(env, thisj, Lz4Compressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env, thisj, Lz4Compressor_directBufferSize);

  // Get the input direct buffer
  LOCK_CLASS(env, clazz, "Lz4Compressor");
  const char* uncompressed_bytes = (const char*)(*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "Lz4Compressor");

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  LOCK_CLASS(env, clazz, "Lz4Compressor");
  char* compressed_bytes = (char *)(*env)->GetDirectBufferAddress(env, compressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "Lz4Compressor");

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  compressed_direct_buf_len = LZ4_compress(uncompressed_bytes, compressed_bytes, uncompressed_direct_buf_len);
  if (compressed_direct_buf_len < 0){
    THROW(env, "java/lang/InternalError", "LZ4_compress failed");
  }

  (*env)->SetIntField(env, thisj, Lz4Compressor_uncompressedDirectBufLen, 0);

  return (jint)compressed_direct_buf_len;
}


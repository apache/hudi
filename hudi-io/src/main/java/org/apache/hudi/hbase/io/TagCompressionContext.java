/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hbase.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import org.apache.hudi.hbase.Tag;
import org.apache.hudi.hbase.io.util.Dictionary;
import org.apache.hudi.hbase.io.util.StreamUtils;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Context that holds the dictionary for Tag compression and doing the compress/uncompress. This
 * will be used for compressing tags while writing into HFiles and WALs.
 */
@InterfaceAudience.Private
public class TagCompressionContext {
  private final Dictionary tagDict;

  public TagCompressionContext(Class<? extends Dictionary> dictType, int dictCapacity)
      throws SecurityException, NoSuchMethodException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    Constructor<? extends Dictionary> dictConstructor = dictType.getConstructor();
    tagDict = dictConstructor.newInstance();
    tagDict.init(dictCapacity);
  }

  public void clear() {
    tagDict.clear();
  }

  /**
   * Compress tags one by one and writes to the OutputStream.
   * @param out Stream to which the compressed tags to be written
   * @param in Source where tags are available
   * @param offset Offset for the tags bytes
   * @param length Length of all tag bytes
   * @throws IOException
   */
  public void compressTags(OutputStream out, byte[] in, int offset, int length)
      throws IOException {
    int pos = offset;
    int endOffset = pos + length;
    assert pos < endOffset;
    while (pos < endOffset) {
      int tagLen = Bytes.readAsInt(in, pos, Tag.TAG_LENGTH_SIZE);
      pos += Tag.TAG_LENGTH_SIZE;
      Dictionary.write(out, in, pos, tagLen, tagDict);
      pos += tagLen;
    }
  }

  /**
   * Compress tags one by one and writes to the OutputStream.
   * @param out Stream to which the compressed tags to be written
   * @param in Source buffer where tags are available
   * @param offset Offset for the tags byte buffer
   * @param length Length of all tag bytes
   * @throws IOException
   */
  public void compressTags(OutputStream out, ByteBuffer in, int offset, int length)
      throws IOException {
    if (in.hasArray()) {
      compressTags(out, in.array(), offset, length);
    } else {
      int pos = offset;
      int endOffset = pos + length;
      assert pos < endOffset;
      while (pos < endOffset) {
        int tagLen = ByteBufferUtils.readAsInt(in, pos, Tag.TAG_LENGTH_SIZE);
        pos += Tag.TAG_LENGTH_SIZE;
        Dictionary.write(out, in, pos, tagLen, tagDict);
        pos += tagLen;
      }
    }
  }

  /**
   * Uncompress tags from the InputStream and writes to the destination array.
   * @param src Stream where the compressed tags are available
   * @param dest Destination array where to write the uncompressed tags
   * @param offset Offset in destination where tags to be written
   * @param length Length of all tag bytes
   * @throws IOException
   */
  public void uncompressTags(InputStream src, byte[] dest, int offset, int length)
      throws IOException {
    int endOffset = offset + length;
    while (offset < endOffset) {
      byte status = (byte) src.read();
      if (status == Dictionary.NOT_IN_DICTIONARY) {
        int tagLen = StreamUtils.readRawVarint32(src);
        offset = Bytes.putAsShort(dest, offset, tagLen);
        IOUtils.readFully(src, dest, offset, tagLen);
        tagDict.addEntry(dest, offset, tagLen);
        offset += tagLen;
      } else {
        short dictIdx = StreamUtils.toShort(status, (byte) src.read());
        byte[] entry = tagDict.getEntry(dictIdx);
        if (entry == null) {
          throw new IOException("Missing dictionary entry for index " + dictIdx);
        }
        offset = Bytes.putAsShort(dest, offset, entry.length);
        System.arraycopy(entry, 0, dest, offset, entry.length);
        offset += entry.length;
      }
    }
  }

  /**
   * Uncompress tags from the input ByteBuffer and writes to the destination array.
   * @param src Buffer where the compressed tags are available
   * @param dest Destination array where to write the uncompressed tags
   * @param offset Offset in destination where tags to be written
   * @param length Length of all tag bytes
   * @return bytes count read from source to uncompress all tags.
   * @throws IOException
   */
  public int uncompressTags(ByteBuff src, byte[] dest, int offset, int length)
      throws IOException {
    int srcBeginPos = src.position();
    int endOffset = offset + length;
    while (offset < endOffset) {
      byte status = src.get();
      int tagLen;
      if (status == Dictionary.NOT_IN_DICTIONARY) {
        tagLen = StreamUtils.readRawVarint32(src);
        offset = Bytes.putAsShort(dest, offset, tagLen);
        src.get(dest, offset, tagLen);
        tagDict.addEntry(dest, offset, tagLen);
        offset += tagLen;
      } else {
        short dictIdx = StreamUtils.toShort(status, src.get());
        byte[] entry = tagDict.getEntry(dictIdx);
        if (entry == null) {
          throw new IOException("Missing dictionary entry for index " + dictIdx);
        }
        tagLen = entry.length;
        offset = Bytes.putAsShort(dest, offset, tagLen);
        System.arraycopy(entry, 0, dest, offset, tagLen);
        offset += tagLen;
      }
    }
    return src.position() - srcBeginPos;
  }

  /**
   * Uncompress tags from the InputStream and writes to the destination buffer.
   * @param src Stream where the compressed tags are available
   * @param dest Destination buffer where to write the uncompressed tags
   * @param length Length of all tag bytes
   * @throws IOException when the dictionary does not have the entry
   */
  public void uncompressTags(InputStream src, ByteBuffer dest, int length) throws IOException {
    if (dest.hasArray()) {
      uncompressTags(src, dest.array(), dest.arrayOffset() + dest.position(), length);
    } else {
      byte[] tagBuf = new byte[length];
      uncompressTags(src, tagBuf, 0, length);
      dest.put(tagBuf);
    }
  }
}

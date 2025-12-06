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

package org.apache.hudi.io.compress.airlift;

import org.apache.hudi.io.compress.HoodieCompressor;

import io.airlift.compress.zstd.ZstdInputStream;
import io.airlift.compress.zstd.ZstdOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class HoodieAirliftZstdCompressor implements HoodieCompressor {

  @Override
  public int decompress(InputStream compressedInput, byte[] targetByteArray, int offset, int length) throws IOException {
    try (InputStream stream = new ZstdInputStream(compressedInput)) {
      int currentOffset = offset;
      int totalBytes = 0;
      int bytes;
      int remainingLength = length;
      while (true) {
        bytes = stream.read(targetByteArray, currentOffset, remainingLength);
        if (bytes > 0) {
          remainingLength -= bytes;
          currentOffset += bytes;
          totalBytes += bytes;
        } else {
          break;
        }
      }
      return totalBytes;
    }
  }

  @Override
  public byte[] compress(byte[] uncompressedBytes) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (OutputStream stream = new ZstdOutputStream(byteArrayOutputStream)) {
      stream.write(uncompressedBytes);
    }
    return byteArrayOutputStream.toByteArray();
  }

  @Override
  public ByteBuffer compress(ByteBuffer uncompressedBytes) throws IOException {
    byte[] temp = new byte[uncompressedBytes.remaining()];
    uncompressedBytes.get(temp);
    return ByteBuffer.wrap(this.compress(temp));
  }
}

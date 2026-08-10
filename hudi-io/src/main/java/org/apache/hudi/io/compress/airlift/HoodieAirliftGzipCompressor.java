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

import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.compress.HoodieCompressor;

import io.airlift.compress.gzip.JdkGzipHadoopStreams;
import io.airlift.compress.hadoop.HadoopInputStream;
import io.airlift.compress.hadoop.HadoopOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.hudi.io.util.IOUtils.readFully;

/**
 * Implementation of {@link HoodieCompressor} for {@link CompressionCodec#GZIP} compression
 * codec using airlift aircompressor's GZIP decompressor.
 */
public class HoodieAirliftGzipCompressor implements HoodieCompressor {
  private final JdkGzipHadoopStreams gzipStreams;

  public HoodieAirliftGzipCompressor() {
    gzipStreams = new JdkGzipHadoopStreams();
  }

  @Override
  public int decompress(InputStream compressedInput,
                        byte[] targetByteArray,
                        int offset,
                        int length) throws IOException {
    try (HadoopInputStream stream = gzipStreams.createInputStream(compressedInput)) {
      return readFully(stream, targetByteArray, offset, length);
    }
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (HadoopOutputStream gzipOutputStream = gzipStreams.createOutputStream(byteArrayOutputStream)) {
      gzipOutputStream.write(data);
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

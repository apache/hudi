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

package org.apache.hudi.io.compress.builtin;

import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.compress.HoodieCompressor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.hudi.io.util.IOUtils.readFully;

/**
 * Implementation of {@link HoodieCompressor} for {@link CompressionCodec#NONE} compression
 * codec (no compression) by directly reading the input stream.
 */
public class HoodieNoneCompressor implements HoodieCompressor {
  @Override
  public int decompress(InputStream compressedInput,
                        byte[] targetByteArray,
                        int offset,
                        int length) throws IOException {
    return readFully(compressedInput, targetByteArray, offset, length);
  }

  @Override
  public byte[] compress(byte[] uncompressedBytes) {
    return uncompressedBytes;
  }

  @Override
  public ByteBuffer compress(ByteBuffer uncompressedBytes) throws IOException {
    return uncompressedBytes;
  }
}

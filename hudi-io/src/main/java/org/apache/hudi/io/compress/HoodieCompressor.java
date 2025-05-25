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

package org.apache.hudi.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Provides compression and decompression on input data.
 */
public interface HoodieCompressor {
  /**
   * Decompresses the data from {@link InputStream} and writes the decompressed data to the target
   * byte array.
   *
   * @param compressedInput compressed data in {@link InputStream}.
   * @param targetByteArray target byte array to store the decompressed data.
   * @param offset          offset in the target byte array to start to write data.
   * @param length          maximum amount of decompressed data to write.
   * @return size of bytes read.
   * @throws IOException upon error.
   */
  int decompress(InputStream compressedInput,
                 byte[] targetByteArray,
                 int offset,
                 int length) throws IOException;

  /**
   * Compresses data stored in byte array.
   *
   * @param uncompressedBytes  input data in byte array
   * @return output data in byte array
   * @throws IOException       upon error
   */
  byte[] compress(byte[] uncompressedBytes) throws IOException;

  /**
   * Compresses data stored in {@link ByteBuffer}.
   *
   * @param uncompressedBytes  input data in {@link ByteBuffer}
   * @return output data in {@link ByteBuffer}
   * @throws IOException       upon error
   */
  ByteBuffer compress(ByteBuffer uncompressedBytes) throws IOException;
}

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

package org.apache.hudi.hbase.io.encoding;

import java.io.IOException;
import org.apache.hudi.hbase.io.hfile.BlockType;
import org.apache.hudi.hbase.io.hfile.HFileContext;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An encoding context that is created by a writer's encoder, and is shared
 * across the writer's whole lifetime.
 *
 * @see HFileBlockDecodingContext for decoding
 *
 */
@InterfaceAudience.Private
public interface HFileBlockEncodingContext {

  /**
   * @return the block type after encoding
   */
  BlockType getBlockType();

  /**
   * @return the {@link DataBlockEncoding} encoding used
   */
  DataBlockEncoding getDataBlockEncoding();

  /**
   * Do any action that needs to be performed after the encoding.
   * Compression is also included if a non-null compression algorithm is used
   */
  void postEncoding(BlockType blockType) throws IOException;

  /**
   * Releases the resources used.
   */
  void close();

  /**
   * @return HFile context information
   */
  HFileContext getHFileContext();

  /**
   * Sets the encoding state.
   */
  void setEncodingState(EncodingState state);

  /**
   * @return the encoding state
   */
  EncodingState getEncodingState();

  /**
   * @param data encoded bytes with header
   * @param offset the offset in encoded data to start at
   * @param length the number of encoded bytes
   * @return Bytes with header which are ready to write out to disk.
   *         This is compressed and encrypted bytes applying the set compression
   *         algorithm and encryption. The bytes may be changed.
   *         If need a Bytes reference for later use, clone the bytes and use that.
   *         Null if the data doesn't need to be compressed and encrypted.
   */
  Bytes compressAndEncrypt(byte[] data, int offset, int length) throws IOException;
}

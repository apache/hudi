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

package org.apache.hudi.hbase.io.hfile.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hudi.hbase.io.hfile.Cacheable;
import org.apache.hudi.hbase.nio.ByteBuff;

/**
 * A class implementing IOEngine interface supports data services for
 * {@link BucketCache}.
 */
@InterfaceAudience.Private
public interface IOEngine {
  /**
   * @return true if persistent storage is supported for the cache when shutdown
   */
  boolean isPersistent();

  /**
   * IOEngine uses shared memory means, when reading Cacheable from it, those refers to the same
   * memory area as used by the Engine for caching it.
   * @return true when IOEngine using shared memory.
   */
  default boolean usesSharedMemory() {
    return false;
  }

  /**
   * Transfers data from IOEngine to a Cacheable object.
   * @param be maintains an (offset,len,refCnt) inside.
   * @return Cacheable which will wrap the NIO ByteBuffers from IOEngine.
   * @throws IOException when any IO error happen
   * @throws IllegalArgumentException when the length of the ByteBuff read is less than 'len'
   */
  Cacheable read(BucketEntry be) throws IOException;

  /**
   * Transfers data from the given byte buffer to IOEngine
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the IO engine where the first byte to be
   *          written
   * @throws IOException
   */
  void write(ByteBuffer srcBuffer, long offset) throws IOException;

  /**
   * Transfers the data from the given MultiByteBuffer to IOEngine
   * @param srcBuffer the given MultiBytebufffers from which bytes are to be read
   * @param offset the offset in the IO engine where the first byte to be written
   * @throws IOException
   */
  void write(ByteBuff srcBuffer, long offset) throws IOException;

  /**
   * Sync the data to IOEngine after writing
   * @throws IOException
   */
  void sync() throws IOException;

  /**
   * Shutdown the IOEngine
   */
  void shutdown();
}

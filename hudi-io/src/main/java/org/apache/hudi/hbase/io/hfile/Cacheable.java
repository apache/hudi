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

package org.apache.hudi.hbase.io.hfile;

import java.nio.ByteBuffer;

import org.apache.hudi.hbase.io.HeapSize;
import org.apache.hudi.hbase.nio.HBaseReferenceCounted;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Cacheable is an interface that allows for an object to be cached. If using an
 * on heap cache, just use heapsize. If using an off heap cache, Cacheable
 * provides methods for serialization of the object.
 *
 * Some objects cannot be moved off heap, those objects will return a
 * getSerializedLength() of 0.
 *
 */
@InterfaceAudience.Private
public interface Cacheable extends HeapSize, HBaseReferenceCounted {
  /**
   * Returns the length of the ByteBuffer required to serialized the object. If the
   * object cannot be serialized, it should return 0.
   *
   * @return int length in bytes of the serialized form or 0 if the object cannot be cached.
   */
  int getSerializedLength();

  /**
   * Serializes its data into destination.
   * @param destination Where to serialize to
   * @param includeNextBlockMetadata Whether to include nextBlockMetadata in the Cache block.
   */
  void serialize(ByteBuffer destination, boolean includeNextBlockMetadata);

  /**
   * Returns CacheableDeserializer instance which reconstructs original object from ByteBuffer.
   *
   * @return CacheableDeserialzer instance.
   */
  CacheableDeserializer<Cacheable> getDeserializer();

  /**
   * @return the block type of this cached HFile block
   */
  BlockType getBlockType();

  /******************************* ReferenceCounted Interfaces ***********************************/

  /**
   * Increase its reference count, and only when no reference we can free the object's memory.
   */
  default Cacheable retain() {
    return this;
  }

  /**
   * Reference count of this Cacheable.
   */
  default int refCnt() {
    return 0;
  }

  /**
   * Decrease its reference count, and if no reference then free the memory of this object, its
   * backend is usually a {@link org.apache.hudi.hbase.nio.ByteBuff}, and we will put its NIO
   * ByteBuffers back to {@link org.apache.hudi.hbase.io.ByteBuffAllocator}
   */
  default boolean release() {
    return false;
  }
}

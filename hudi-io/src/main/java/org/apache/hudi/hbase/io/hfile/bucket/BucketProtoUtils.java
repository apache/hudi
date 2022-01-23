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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.hudi.hbase.io.ByteBuffAllocator;
import org.apache.hudi.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hudi.hbase.io.hfile.BlockCacheKey;
import org.apache.hudi.hbase.io.hfile.BlockPriority;
import org.apache.hudi.hbase.io.hfile.BlockType;
import org.apache.hudi.hbase.io.hfile.CacheableDeserializerIdManager;
import org.apache.hudi.hbase.io.hfile.HFileBlock;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hudi.hbase.shaded.protobuf.generated.BucketCacheProtos;

@InterfaceAudience.Private
final class BucketProtoUtils {
  private BucketProtoUtils() {

  }

  static BucketCacheProtos.BucketCacheEntry toPB(BucketCache cache) {
    return BucketCacheProtos.BucketCacheEntry.newBuilder()
        .setCacheCapacity(cache.getMaxSize())
        .setIoClass(cache.ioEngine.getClass().getName())
        .setMapClass(cache.backingMap.getClass().getName())
        .putAllDeserializers(CacheableDeserializerIdManager.save())
        .setBackingMap(BucketProtoUtils.toPB(cache.backingMap))
        .setChecksum(ByteString.copyFrom(((PersistentIOEngine) cache.ioEngine).
            calculateChecksum(cache.getAlgorithm()))).build();
  }

  private static BucketCacheProtos.BackingMap toPB(
      Map<BlockCacheKey, BucketEntry> backingMap) {
    BucketCacheProtos.BackingMap.Builder builder = BucketCacheProtos.BackingMap.newBuilder();
    for (Map.Entry<BlockCacheKey, BucketEntry> entry : backingMap.entrySet()) {
      builder.addEntry(BucketCacheProtos.BackingMapEntry.newBuilder()
          .setKey(toPB(entry.getKey()))
          .setValue(toPB(entry.getValue()))
          .build());
    }
    return builder.build();
  }

  private static BucketCacheProtos.BlockCacheKey toPB(BlockCacheKey key) {
    return BucketCacheProtos.BlockCacheKey.newBuilder()
        .setHfilename(key.getHfileName())
        .setOffset(key.getOffset())
        .setPrimaryReplicaBlock(key.isPrimary())
        .setBlockType(toPB(key.getBlockType()))
        .build();
  }

  private static BucketCacheProtos.BlockType toPB(BlockType blockType) {
    switch(blockType) {
      case DATA:
        return BucketCacheProtos.BlockType.data;
      case META:
        return BucketCacheProtos.BlockType.meta;
      case TRAILER:
        return BucketCacheProtos.BlockType.trailer;
      case INDEX_V1:
        return BucketCacheProtos.BlockType.index_v1;
      case FILE_INFO:
        return BucketCacheProtos.BlockType.file_info;
      case LEAF_INDEX:
        return BucketCacheProtos.BlockType.leaf_index;
      case ROOT_INDEX:
        return BucketCacheProtos.BlockType.root_index;
      case BLOOM_CHUNK:
        return BucketCacheProtos.BlockType.bloom_chunk;
      case ENCODED_DATA:
        return BucketCacheProtos.BlockType.encoded_data;
      case GENERAL_BLOOM_META:
        return BucketCacheProtos.BlockType.general_bloom_meta;
      case INTERMEDIATE_INDEX:
        return BucketCacheProtos.BlockType.intermediate_index;
      case DELETE_FAMILY_BLOOM_META:
        return BucketCacheProtos.BlockType.delete_family_bloom_meta;
      default:
        throw new Error("Unrecognized BlockType.");
    }
  }

  private static BucketCacheProtos.BucketEntry toPB(BucketEntry entry) {
    return BucketCacheProtos.BucketEntry.newBuilder()
        .setOffset(entry.offset())
        .setLength(entry.getLength())
        .setDeserialiserIndex(entry.deserializerIndex)
        .setAccessCounter(entry.getAccessCounter())
        .setPriority(toPB(entry.getPriority()))
        .build();
  }

  private static BucketCacheProtos.BlockPriority toPB(BlockPriority p) {
    switch (p) {
      case MULTI:
        return BucketCacheProtos.BlockPriority.multi;
      case MEMORY:
        return BucketCacheProtos.BlockPriority.memory;
      case SINGLE:
        return BucketCacheProtos.BlockPriority.single;
      default:
        throw new Error("Unrecognized BlockPriority.");
    }
  }

  static ConcurrentHashMap<BlockCacheKey, BucketEntry> fromPB(
      Map<Integer, String> deserializers, BucketCacheProtos.BackingMap backingMap,
      Function<BucketEntry, Recycler> createRecycler)
      throws IOException {
    ConcurrentHashMap<BlockCacheKey, BucketEntry> result = new ConcurrentHashMap<>();
    for (BucketCacheProtos.BackingMapEntry entry : backingMap.getEntryList()) {
      BucketCacheProtos.BlockCacheKey protoKey = entry.getKey();
      BlockCacheKey key = new BlockCacheKey(protoKey.getHfilename(), protoKey.getOffset(),
          protoKey.getPrimaryReplicaBlock(), fromPb(protoKey.getBlockType()));
      BucketCacheProtos.BucketEntry protoValue = entry.getValue();
      // TODO:We use ByteBuffAllocator.HEAP here, because we could not get the ByteBuffAllocator
      // which created by RpcServer elegantly.
      BucketEntry value = new BucketEntry(
          protoValue.getOffset(),
          protoValue.getLength(),
          protoValue.getAccessCounter(),
          protoValue.getPriority() == BucketCacheProtos.BlockPriority.memory, createRecycler,
          ByteBuffAllocator.HEAP);
      // This is the deserializer that we stored
      int oldIndex = protoValue.getDeserialiserIndex();
      String deserializerClass = deserializers.get(oldIndex);
      if (deserializerClass == null) {
        throw new IOException("Found deserializer index without matching entry.");
      }
      // Convert it to the identifier for the deserializer that we have in this runtime
      if (deserializerClass.equals(HFileBlock.BlockDeserializer.class.getName())) {
        int actualIndex = HFileBlock.BLOCK_DESERIALIZER.getDeserializerIdentifier();
        value.deserializerIndex = (byte) actualIndex;
      } else {
        // We could make this more plugable, but right now HFileBlock is the only implementation
        // of Cacheable outside of tests, so this might not ever matter.
        throw new IOException("Unknown deserializer class found: " + deserializerClass);
      }
      result.put(key, value);
    }
    return result;
  }

  private static BlockType fromPb(BucketCacheProtos.BlockType blockType) {
    switch (blockType) {
      case data:
        return BlockType.DATA;
      case meta:
        return BlockType.META;
      case trailer:
        return BlockType.TRAILER;
      case index_v1:
        return BlockType.INDEX_V1;
      case file_info:
        return BlockType.FILE_INFO;
      case leaf_index:
        return BlockType.LEAF_INDEX;
      case root_index:
        return BlockType.ROOT_INDEX;
      case bloom_chunk:
        return BlockType.BLOOM_CHUNK;
      case encoded_data:
        return BlockType.ENCODED_DATA;
      case general_bloom_meta:
        return BlockType.GENERAL_BLOOM_META;
      case intermediate_index:
        return BlockType.INTERMEDIATE_INDEX;
      case delete_family_bloom_meta:
        return BlockType.DELETE_FAMILY_BLOOM_META;
      default:
        throw new Error("Unrecognized BlockType.");
    }
  }
}

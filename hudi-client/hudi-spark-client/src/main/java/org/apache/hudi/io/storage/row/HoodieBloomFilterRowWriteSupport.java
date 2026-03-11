/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage.row;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;

import org.apache.spark.sql.HoodieUTF8StringFactory;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Bloom filter write support implementation for Lance file format.
 * Handles UTF8String record keys specific to Spark InternalRow.
 */
public class HoodieBloomFilterRowWriteSupport extends HoodieBloomFilterWriteSupport<UTF8String> {

  private static final HoodieUTF8StringFactory UTF8STRING_FACTORY =
      SparkAdapterSupport$.MODULE$.sparkAdapter().getUTF8StringFactory();

  public HoodieBloomFilterRowWriteSupport(BloomFilter bloomFilter) {
    super(bloomFilter);
  }

  @Override
  protected int compareRecordKey(UTF8String a, UTF8String b) {
    return UTF8STRING_FACTORY.wrapUTF8String(a).compareTo(UTF8STRING_FACTORY.wrapUTF8String(b));
  }

  @Override
  protected byte[] getUTF8Bytes(UTF8String key) {
    return key.getBytes();
  }

  @Override
  protected UTF8String dereference(UTF8String key) {
    // NOTE: [[clone]] is performed here (rather than [[copy]]) to only copy underlying buffer in
    //       cases when [[UTF8String]] is pointing into a buffer storing the whole containing record,
    //       and simply do a pass over when it holds a (immutable) buffer holding just the string
    return key.clone();
  }
}

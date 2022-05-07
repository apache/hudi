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

package org.apache.hudi.common.bloom;

import org.apache.hudi.common.util.Base64CodecUtil;
import org.apache.hudi.exception.HoodieIndexException;

import org.apache.hadoop.util.bloom.Key;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Hoodie's dynamic bloom bounded bloom filter. This is based largely on Hadoop's DynamicBloomFilter, but with a bound
 * on amount of entries to dynamically expand to. Once the entries added reach the bound, false positive ratio may not
 * be guaranteed.
 */
public class HoodieDynamicBoundedBloomFilter implements BloomFilter {

  public static final String TYPE_CODE_PREFIX = "DYNAMIC";
  private InternalDynamicBloomFilter internalDynamicBloomFilter;

  /**
   * Instantiates {@link HoodieDynamicBoundedBloomFilter} with the given args.
   *
   * @param numEntries The total number of entries.
   * @param errorRate  maximum allowable error rate.
   * @param hashType   type of the hashing function (see {@link org.apache.hadoop.util.hash.Hash}).
   * @return the {@link HoodieDynamicBoundedBloomFilter} thus created
   */
  HoodieDynamicBoundedBloomFilter(int numEntries, double errorRate, int hashType, int maxNoOfEntries) {
    // Bit size
    int bitSize = BloomFilterUtils.getBitSize(numEntries, errorRate);
    // Number of the hash functions
    int numHashs = BloomFilterUtils.getNumHashes(bitSize, numEntries);
    this.internalDynamicBloomFilter = new InternalDynamicBloomFilter(bitSize, numHashs, hashType, numEntries,
        maxNoOfEntries);
  }

  /**
   * Generate {@link HoodieDynamicBoundedBloomFilter} from the given {@code serString} serialized string.
   *
   * @param serString the serialized string which represents the {@link HoodieDynamicBoundedBloomFilter}
   * @param typeCode  type code of the bloom filter
   */
  public HoodieDynamicBoundedBloomFilter(String serString, BloomFilterTypeCode typeCode) {
    // ignoring the type code for now, since we have just one version
    byte[] bytes = Base64CodecUtil.decode(serString);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      internalDynamicBloomFilter = new InternalDynamicBloomFilter();
      internalDynamicBloomFilter.readFields(dis);
      dis.close();
    } catch (IOException e) {
      throw new HoodieIndexException("Could not deserialize BloomFilter instance", e);
    }
  }

  @Override
  public void add(String key) {
    internalDynamicBloomFilter.add(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public boolean mightContain(String key) {
    return internalDynamicBloomFilter.membershipTest(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public String serializeToString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      internalDynamicBloomFilter.write(dos);
      byte[] bytes = baos.toByteArray();
      dos.close();
      return Base64CodecUtil.encode(bytes);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not serialize BloomFilter instance", e);
    }
  }

  @Override
  public BloomFilterTypeCode getBloomFilterTypeCode() {
    return BloomFilterTypeCode.DYNAMIC_V0;
  }
}


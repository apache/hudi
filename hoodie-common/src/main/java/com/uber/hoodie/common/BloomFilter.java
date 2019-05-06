/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common;

import com.uber.hoodie.exception.HoodieIndexException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * A Bloom filter implementation built on top of {@link org.apache.hadoop.util.bloom.BloomFilter} or
 * {@link org.apache.hadoop.util.bloom.DynamicBloomFilter}.
 */
public class BloomFilter {

  /**
   * Used in computing the optimal Bloom filter size. This approximately equals 0.480453.
   */
  public static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

  private org.apache.hadoop.util.bloom.BloomFilter bloomFilter = null;

  private org.apache.hadoop.util.bloom.DynamicBloomFilter dynamicBloomFilter = null;

  private boolean isDynamic;

  public BloomFilter(int numEntries, double errorRate, boolean isDynamic) {
    this(numEntries, errorRate, Hash.MURMUR_HASH, isDynamic);
  }

  /**
   * Create a new Bloom filter with the given configurations.
   */
  public BloomFilter(int numEntries, double errorRate, int hashType, boolean isDynamic) {
    // Bit size
    int bitSize = (int) Math.ceil(numEntries * (-Math.log(errorRate) / LOG2_SQUARED));
    // Number of the hash functions
    int numHashs = (int) Math.ceil(Math.log(2) * bitSize / numEntries);
    // The filter
    if (isDynamic) {
      this.dynamicBloomFilter = new org.apache.hadoop.util.bloom.DynamicBloomFilter(bitSize, numHashs, hashType,
          numEntries);
    } else {
      this.bloomFilter = new org.apache.hadoop.util.bloom.BloomFilter(bitSize, numHashs, hashType);
    }
    this.isDynamic = isDynamic;
  }

  /**
   * Create the bloom filter from serialized string.
   */
  public BloomFilter(String filterStr, boolean isDynamic) {
    this.isDynamic = isDynamic;
    byte[] bytes = DatatypeConverter.parseBase64Binary(filterStr);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      if (isDynamic) {
        this.dynamicBloomFilter = new org.apache.hadoop.util.bloom.DynamicBloomFilter();
        this.dynamicBloomFilter.readFields(dis);
      } else {
        this.bloomFilter = new org.apache.hadoop.util.bloom.BloomFilter();
        this.bloomFilter.readFields(dis);
      }
      dis.close();
    } catch (IOException e) {
      throw new HoodieIndexException("Could not deserialize BloomFilter instance", e);
    }
  }

  public void add(String key) {
    if (key == null) {
      throw new NullPointerException("Key cannot by null");
    }
    if (isDynamic) {
      this.dynamicBloomFilter.add(new Key(key.getBytes(StandardCharsets.UTF_8)));
    } else {
      this.bloomFilter.add(new Key(key.getBytes(StandardCharsets.UTF_8)));
    }
  }

  public boolean mightContain(String key) {
    if (key == null) {
      throw new NullPointerException("Key cannot by null");
    }
    if (isDynamic) {
      return this.dynamicBloomFilter.membershipTest(new Key(key.getBytes(StandardCharsets.UTF_8)));
    } else {
      return this.bloomFilter.membershipTest(new Key(key.getBytes(StandardCharsets.UTF_8)));
    }
  }

  /**
   * Serialize the bloom filter as a string.
   */
  public String serializeToString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      if (isDynamic) {
        dynamicBloomFilter.write(dos);
      } else {
        bloomFilter.write(dos);
      }
      byte[] bytes = baos.toByteArray();
      dos.close();
      return DatatypeConverter.printBase64Binary(bytes);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not serialize BloomFilter instance", e);
    }
  }
}

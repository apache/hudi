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

package org.apache.hudi.common;

import org.apache.hudi.exception.HoodieIndexException;

import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import javax.xml.bind.DatatypeConverter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A Bloom filter implementation built on top of {@link org.apache.hadoop.util.bloom.BloomFilter}.
 */
public class BloomFilter {

  /**
   * Used in computing the optimal Bloom filter size. This approximately equals 0.480453.
   */
  public static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

  private org.apache.hadoop.util.bloom.BloomFilter filter = null;

  public BloomFilter(int numEntries, double errorRate) {
    this(numEntries, errorRate, Hash.MURMUR_HASH);
  }

  /**
   * Create a new Bloom filter with the given configurations.
   */
  public BloomFilter(int numEntries, double errorRate, int hashType) {
    // Bit size
    int bitSize = (int) Math.ceil(numEntries * (-Math.log(errorRate) / LOG2_SQUARED));
    // Number of the hash functions
    int numHashs = (int) Math.ceil(Math.log(2) * bitSize / numEntries);
    // The filter
    this.filter = new org.apache.hadoop.util.bloom.BloomFilter(bitSize, numHashs, hashType);
  }

  /**
   * Create the bloom filter from serialized string.
   */
  public BloomFilter(String filterStr) {
    this.filter = new org.apache.hadoop.util.bloom.BloomFilter();
    byte[] bytes = DatatypeConverter.parseBase64Binary(filterStr);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      this.filter.readFields(dis);
      dis.close();
    } catch (IOException e) {
      throw new HoodieIndexException("Could not deserialize BloomFilter instance", e);
    }
  }

  public void add(String key) {
    if (key == null) {
      throw new NullPointerException("Key cannot by null");
    }
    filter.add(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  public boolean mightContain(String key) {
    if (key == null) {
      throw new NullPointerException("Key cannot by null");
    }
    return filter.membershipTest(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Serialize the bloom filter as a string.
   */
  public String serializeToString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      filter.write(dos);
      byte[] bytes = baos.toByteArray();
      dos.close();
      return DatatypeConverter.printBase64Binary(bytes);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not serialize BloomFilter instance", e);
    }
  }
}

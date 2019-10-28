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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hudi.exception.HoodieIndexException;

/**
 * Hudi's Dynamic Bloom Filter based out of {@link org.apache.hadoop.util.bloom.DynamicBloomFilter}
 */
public class HudiDynamicBloomFilter extends DynamicBloomFilter implements BloomFilter {
  /**
   * Used in computing the optimal Bloom filter size. This approximately equals 0.480453.
   */
  static final double LOG2_SQUARED = Math.log(2) * Math.log(2);
  public static final int VERSION = 1;
  private DynamicBloomFilter dynamicBloomFilter;

  /**
   * Instantiates {@link HudiDynamicBloomFilter} with the given args
   * @param numEntries The total number of entries.
   * @param errorRate maximum allowable error rate.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   * @return the {@link HudiDynamicBloomFilter} thus created
   */
  HudiDynamicBloomFilter(int numEntries, double errorRate, int hashType){
    int bitSize = (int) Math.ceil(numEntries * (-Math.log(errorRate) / LOG2_SQUARED));
    // Number of the hash functions
    int numHashs = (int) Math.ceil(Math.log(2) * bitSize / numEntries);
    this.dynamicBloomFilter = new DynamicBloomFilter(bitSize, numHashs, hashType, numEntries);
  }

  /**
   * Generate {@link HudiDynamicBloomFilter} from the given {@code serString} serialized string
   * @param serString the serialized string which represents the {@link HudiDynamicBloomFilter}
   */
  HudiDynamicBloomFilter(String serString){
    byte[] bytes = DatatypeConverter.parseBase64Binary(serString);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      dynamicBloomFilter = new DynamicBloomFilter();
      dynamicBloomFilter.readFields(dis);
      dis.close();
    } catch (IOException e) {
      throw new HoodieIndexException("Could not deserialize BloomFilter instance", e);
    }
  }

  @Override
  public void add(String key) {
    dynamicBloomFilter.add(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public boolean mightContain(String key) {
    return dynamicBloomFilter.membershipTest(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public String serializeToString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      dynamicBloomFilter.write(dos);
      byte[] bytes = baos.toByteArray();
      dos.close();
      return DatatypeConverter.printBase64Binary(bytes);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not serialize BloomFilter instance", e);
    }
  }

  @Override
  public int getBloomIndexVersion() {
    return HudiDynamicBloomFilter.VERSION;
  }
}

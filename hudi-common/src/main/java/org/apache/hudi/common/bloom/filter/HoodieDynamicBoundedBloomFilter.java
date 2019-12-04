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

package org.apache.hudi.common.bloom.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hudi.exception.HoodieIndexException;

/**
 * Hoodie's dynamic bloom bounded bloom filter. This is based largely on Hadoop's DynamicBloomFilter, but with a bound
 * on amount of entries to dynamically expand to. Once the entries added reach the bound, false positive ratio may not
 * be guaranteed.
 */
public class HoodieDynamicBoundedBloomFilter implements BloomFilter {

  public static final String TYPE_CODE_PREFIX = "DYNAMIC";
  public static final String TYPE_CODE = TYPE_CODE_PREFIX + "_V0";
  private LocalDynamicBloomFilter localDynamicBloomFilter;

  /**
   * Instantiates {@link HoodieDynamicBoundedBloomFilter} with the given args
   *
   * @param numEntries The total number of entries.
   * @param errorRate maximum allowable error rate.
   * @param hashType type of the hashing function (see {@link org.apache.hadoop.util.hash.Hash}).
   * @return the {@link HoodieDynamicBoundedBloomFilter} thus created
   */
  HoodieDynamicBoundedBloomFilter(int numEntries, double errorRate, int hashType, int maxNoOfEntries) {
    // Bit size
    int bitSize = BloomFilterUtils.getBitSize(numEntries, errorRate);
    // Number of the hash functions
    int numHashs = BloomFilterUtils.getNumHashes(bitSize, numEntries);
    this.localDynamicBloomFilter = new LocalDynamicBloomFilter(bitSize, numHashs, hashType, numEntries, maxNoOfEntries);
  }

  /**
   * Generate {@link HoodieDynamicBoundedBloomFilter} from the given {@code serString} serialized string
   *
   * @param serString the serialized string which represents the {@link HoodieDynamicBoundedBloomFilter}
   * @param typeCode type code of the bloom filter
   */
  HoodieDynamicBoundedBloomFilter(String serString, String typeCode) {
    // ignoring the type code for now, since we have just one version
    byte[] bytes = DatatypeConverter.parseBase64Binary(serString);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      localDynamicBloomFilter = new LocalDynamicBloomFilter();
      localDynamicBloomFilter.readFields(dis);
      dis.close();
    } catch (IOException e) {
      throw new HoodieIndexException("Could not deserialize BloomFilter instance", e);
    }
  }

  @Override
  public void add(String key) {
    localDynamicBloomFilter.add(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public boolean mightContain(String key) {
    return localDynamicBloomFilter.membershipTest(new Key(key.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public String serializeToString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      localDynamicBloomFilter.write(dos);
      byte[] bytes = baos.toByteArray();
      dos.close();
      return DatatypeConverter.printBase64Binary(bytes);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not serialize BloomFilter instance", e);
    }
  }

  @Override
  public String getBloomFilterTypeCode() {
    return HoodieDynamicBoundedBloomFilter.TYPE_CODE;
  }
}


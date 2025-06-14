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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIndexException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.io.util.IOUtils.getDataInputStream;

/**
 * A Simple Bloom filter implementation built on top of {@link InternalBloomFilter}.
 */

public class SimpleBloomFilter implements BloomFilter {

  private InternalBloomFilter filter;

  /**
   * Create a new Bloom filter with the given configurations.
   *
   * @param numEntries The total number of entries.
   * @param errorRate  maximum allowable error rate.
   * @param hashType   type of the hashing function (see {@link org.apache.hudi.common.util.hash.Hash}).
   */
  public SimpleBloomFilter(int numEntries, double errorRate, int hashType) {
    // Bit size
    int bitSize = BloomFilterUtils.getBitSize(numEntries, errorRate);
    // Number of the hash functions
    int numHashs = BloomFilterUtils.getNumHashes(bitSize, numEntries);
    // The filter
    this.filter = new InternalBloomFilter(bitSize, numHashs, hashType);
  }

  /**
   * Create the bloom filter from serialized string.
   *
   * @param serString serialized string which represents the {@link SimpleBloomFilter}
   */
  public SimpleBloomFilter(String serString) {
    this.filter = new InternalBloomFilter();
    byte[] bytes = Base64CodecUtil.decode(serString);
    try (DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes))) {
      extractAndSetInternalBloomFilter(stream);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not deserialize BloomFilter from string", e);
    }
  }

  /**
   * Creates {@link SimpleBloomFilter} from the given {@link ByteBuffer}.
   *
   * @param byteBuffer {@link ByteBuffer} containing the serialized bloom filter.
   */
  public SimpleBloomFilter(ByteBuffer byteBuffer) {
    this.filter = new InternalBloomFilter();
    try (DataInputStream stream = getDataInputStream(Base64CodecUtil.decode(byteBuffer))) {
      extractAndSetInternalBloomFilter(stream);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not deserialize BloomFilter from byte buffer", e);
    }
  }

  @Override
  public void add(String key) {
    add(getUTF8Bytes(key));
  }

  @Override
  public void add(byte[] keyBytes) {
    if (keyBytes == null) {
      throw new NullPointerException("Key cannot be null");
    }
    filter.add(new Key(keyBytes));
  }

  @Override
  public boolean mightContain(String key) {
    if (key == null) {
      throw new NullPointerException("Key cannot be null");
    }
    return filter.membershipTest(new Key(getUTF8Bytes(key)));
  }

  /**
   * Serialize the bloom filter as a string.
   */
  @Override
  public String serializeToString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      filter.write(dos);
      byte[] bytes = baos.toByteArray();
      dos.close();
      return Base64CodecUtil.encode(bytes);
    } catch (IOException e) {
      throw new HoodieIndexException("Could not serialize BloomFilter instance", e);
    }
  }

  private void writeObject(ObjectOutputStream os)
      throws IOException {
    filter.write(os);
  }

  private void readObject(ObjectInputStream is) throws IOException {
    filter = new InternalBloomFilter();
    filter.readFields(is);
  }

  // @Override
  public void write(DataOutput out) throws IOException {
    out.write(getUTF8Bytes(filter.toString()));
  }

  //@Override
  public void readFields(DataInput in) throws IOException {
    filter = new InternalBloomFilter();
    filter.readFields(in);
  }

  @Override
  public BloomFilterTypeCode getBloomFilterTypeCode() {
    return BloomFilterTypeCode.SIMPLE;
  }

  private void extractAndSetInternalBloomFilter(DataInputStream dis) throws IOException {
    this.filter.readFields(dis);
  }

  @Override
  public void or(BloomFilter otherFilter) {
    if (otherFilter != null) {
      ValidationUtils.checkArgument(otherFilter instanceof SimpleBloomFilter, "SimpleBloomFilter can only perform OR operations with other SimpleBloomFilters.");
      SimpleBloomFilter bloomFilter = (SimpleBloomFilter) otherFilter;
      this.filter.or(bloomFilter.filter);
    }
  }
}

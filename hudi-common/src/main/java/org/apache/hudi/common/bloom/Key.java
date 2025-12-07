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

package org.apache.hudi.common.bloom;

import lombok.Getter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The general behavior of a key that must be stored in a bloom filter.
 * <p>
 * The code in class is adapted from {@link org.apache.hadoop.util.bloom.Key} in Apache Hadoop.
 *
 * @see InternalBloomFilter The general behavior of a bloom filter and how the key is used.
 */
@Getter
public class Key implements Comparable<Key> {
  /**
   * Byte value of key
   * -- GETTER --
   *
   * @return byte[] The value of <i>this</i> key.

   */
  byte[] bytes;

  /**
   * The weight associated to <i>this</i> key.
   * <p>
   * <b>Invariant</b>: if it is not specified, each instance of
   * <code>Key</code> will have a default weight of 1.0
   * -- GETTER --
   *
   * @return Returns the weight associated to <i>this</i> key.

   */
  double weight;

  /**
   * default constructor - use with readFields
   */
  public Key() {
  }

  /**
   * Constructor.
   * <p>
   * Builds a key with a default weight.
   *
   * @param value The byte value of <i>this</i> key.
   */
  public Key(byte[] value) {
    this(value, 1.0);
  }

  /**
   * Constructor.
   * <p>
   * Builds a key with a specified weight.
   *
   * @param value  The value of <i>this</i> key.
   * @param weight The weight associated to <i>this</i> key.
   */
  public Key(byte[] value, double weight) {
    set(value, weight);
  }

  /**
   * @param value
   * @param weight
   */
  public void set(byte[] value, double weight) {
    if (value == null) {
      throw new IllegalArgumentException("value can not be null");
    }
    this.bytes = value;
    this.weight = weight;
  }

  /**
   * Increments the weight of <i>this</i> key with a specified value.
   *
   * @param weight The increment.
   */
  public void incrementWeight(double weight) {
    this.weight += weight;
  }

  /**
   * Increments the weight of <i>this</i> key by one.
   */
  public void incrementWeight() {
    this.weight++;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Key)) {
      return false;
    }
    return this.compareTo((Key) o) == 0;
  }

  @Override
  public int hashCode() {
    int result = 0;
    for (int i = 0; i < bytes.length; i++) {
      result ^= Byte.valueOf(bytes[i]).hashCode();
    }
    result ^= Double.valueOf(weight).hashCode();
    return result;
  }

  /**
   * Serialize the fields of this object to <code>out</code>.
   *
   * @param out <code>DataOutput</code> to serialize this object into.
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
    out.writeDouble(weight);
  }

  /**
   * Deserialize the fields of this object from <code>in</code>.
   *
   * <p>For efficiency, implementations should attempt to re-use storage in the
   * existing object where possible.</p>
   *
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @throws IOException
   */
  public void readFields(DataInput in) throws IOException {
    this.bytes = new byte[in.readInt()];
    in.readFully(this.bytes);
    weight = in.readDouble();
  }

  // Comparable
  @Override
  public int compareTo(Key other) {
    int result = this.bytes.length - other.getBytes().length;
    for (int i = 0; result == 0 && i < bytes.length; i++) {
      result = this.bytes[i] - other.bytes[i];
    }

    if (result == 0) {
      result = (int) (this.weight - other.weight);
    }
    return result;
  }
}

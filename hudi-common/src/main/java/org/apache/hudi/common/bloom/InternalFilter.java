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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.bloom.HashFunction;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Copied from {@link org.apache.hadoop.util.bloom.Filter}. {@link InternalDynamicBloomFilter} needs access to some of
 * protected members of {@link org.apache.hadoop.util.bloom.Filter} and hence had to copy it locally.
 */
abstract class InternalFilter implements Writable {

  private static final int VERSION = -1; // negative to accommodate for old format
  protected int vectorSize;
  protected HashFunction hash;
  protected int nbHash;
  protected int hashType;

  protected InternalFilter() {
  }

  /**
   * Constructor.
   *
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash     The number of hash functions to consider.
   * @param hashType   type of the hashing function (see {@link Hash}).
   */
  protected InternalFilter(int vectorSize, int nbHash, int hashType) {
    this.vectorSize = vectorSize;
    this.nbHash = nbHash;
    this.hashType = hashType;
    this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
  }

  /**
   * Adds a key to <i>this</i> filter.
   *
   * @param key The key to add.
   */
  public abstract void add(Key key);

  /**
   * Determines wether a specified key belongs to <i>this</i> filter.
   *
   * @param key The key to test.
   * @return boolean True if the specified key belongs to <i>this</i> filter. False otherwise.
   */
  public abstract boolean membershipTest(Key key);

  /**
   * Peforms a logical AND between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to AND with.
   */
  public abstract void and(InternalFilter filter);

  /**
   * Peforms a logical OR between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to OR with.
   */
  public abstract void or(InternalFilter filter);

  /**
   * Peforms a logical XOR between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to XOR with.
   */
  public abstract void xor(InternalFilter filter);

  /**
   * Performs a logical NOT on <i>this</i> filter.
   * <p>
   * The result is assigned to <i>this</i> filter.
   */
  public abstract void not();

  /**
   * Adds a list of keys to <i>this</i> filter.
   *
   * @param keys The list of keys.
   */
  public void add(List<Key> keys) {
    if (keys == null) {
      throw new IllegalArgumentException("ArrayList<Key> may not be null");
    }

    for (Key key : keys) {
      add(key);
    }
  } //end add()

  /**
   * Adds a collection of keys to <i>this</i> filter.
   *
   * @param keys The collection of keys.
   */
  public void add(Collection<Key> keys) {
    if (keys == null) {
      throw new IllegalArgumentException("Collection<Key> may not be null");
    }
    for (Key key : keys) {
      add(key);
    }
  } //end add()

  /**
   * Adds an array of keys to <i>this</i> filter.
   *
   * @param keys The array of keys.
   */
  public void add(Key[] keys) {
    if (keys == null) {
      throw new IllegalArgumentException("Key[] may not be null");
    }
    for (Key key : keys) {
      add(key);
    }
  } //end add()

  // Writable interface

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
    out.writeInt(this.nbHash);
    out.writeByte(this.hashType);
    out.writeInt(this.vectorSize);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int ver = in.readInt();
    if (ver > 0) { // old unversioned format
      this.nbHash = ver;
      this.hashType = Hash.JENKINS_HASH;
    } else if (ver == VERSION) {
      this.nbHash = in.readInt();
      this.hashType = in.readByte();
    } else {
      throw new IOException("Unsupported version: " + ver);
    }
    this.vectorSize = in.readInt();
    this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
  }
} //end class
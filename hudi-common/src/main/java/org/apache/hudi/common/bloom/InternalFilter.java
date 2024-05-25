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

import org.apache.hudi.common.util.hash.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Defines the general behavior of a filter.
 * <p>
 * The code in class is adapted from {@link org.apache.hadoop.util.bloom.Filter} in Apache Hadoop.
 * <p>
 * A filter is a data structure which aims at offering a lossy summary of a set <code>A</code>.  The
 * key idea is to map entries of <code>A</code> (also called <i>keys</i>) into several positions
 * in a vector through the use of several hash functions.
 * <p>
 * Typically, a filter will be implemented as a Bloom filter (or a Bloom filter extension).
 * <p>
 * It must be extended in order to define the real behavior.
 *
 * @see Key The general behavior of a key
 * @see HashFunction A hash function
 */
abstract class InternalFilter {
  private static final int VERSION = -1; // negative to accommodate for old format
  /**
   * The vector size of <i>this</i> filter.
   */
  protected int vectorSize;

  /**
   * The hash function used to map a key to several positions in the vector.
   */
  protected HashFunction hash;

  /**
   * The number of hash function to consider.
   */
  protected int nbHash;

  /**
   * Type of hashing function to use.
   */
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
   * Determines whether a specified key belongs to <i>this</i> filter.
   *
   * @param key The key to test.
   * @return boolean True if the specified key belongs to <i>this</i> filter. False otherwise.
   */
  public abstract boolean membershipTest(Key key);

  /**
   * Performs a logical AND between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to AND with.
   */
  public abstract void and(InternalFilter filter);

  /**
   * Performs a logical OR between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to OR with.
   */
  public abstract void or(InternalFilter filter);

  /**
   * Performs a logical XOR between <i>this</i> filter and a specified filter.
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

  /**
   * Serialize the fields of this object to <code>out</code>.
   *
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
    out.writeInt(this.nbHash);
    out.writeByte(this.hashType);
    out.writeInt(this.vectorSize);
  }

  /**
   * Deserialize the fields of this object from <code>in</code>.
   *
   * <p>For efficiency, implementations should attempt to re-use storage in the
   * existing object where possible.</p>
   *
   * @param in <code>DataInput</code> to deserialize this object from.
   * @throws IOException
   */
  public void readFields(DataInput in) throws IOException {
    int ver = in.readInt();
    if (ver > 0) { // old non-versioned format
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
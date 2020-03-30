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

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hoodie's internal dynamic Bloom Filter. This is largely based of {@link org.apache.hadoop.util.bloom.DynamicBloomFilter}
 * with bounds on maximum number of entries. Once the max entries is reached, false positive gaurantees are not
 * honored.
 */
class InternalDynamicBloomFilter extends InternalFilter {

  /**
   * Threshold for the maximum number of key to record in a dynamic Bloom filter row.
   */
  private int nr;

  /**
   * The number of keys recorded in the current standard active Bloom filter.
   */
  private int currentNbRecord;
  private int maxNr;
  private boolean reachedMax = false;
  private int curMatrixIndex = 0;

  /**
   * The matrix of Bloom filter.
   */
  private org.apache.hadoop.util.bloom.BloomFilter[] matrix;

  /**
   * Zero-args constructor for the serialization.
   */
  public InternalDynamicBloomFilter() {
  }

  /**
   * Constructor.
   * <p>
   * Builds an empty Dynamic Bloom filter.
   *
   * @param vectorSize The number of bits in the vector.
   * @param nbHash     The number of hash function to consider.
   * @param hashType   type of the hashing function (see {@link org.apache.hadoop.util.hash.Hash}).
   * @param nr         The threshold for the maximum number of keys to record in a dynamic Bloom filter row.
   */
  public InternalDynamicBloomFilter(int vectorSize, int nbHash, int hashType, int nr, int maxNr) {
    super(vectorSize, nbHash, hashType);

    this.nr = nr;
    this.currentNbRecord = 0;
    this.maxNr = maxNr;

    matrix = new org.apache.hadoop.util.bloom.BloomFilter[1];
    matrix[0] = new org.apache.hadoop.util.bloom.BloomFilter(this.vectorSize, this.nbHash, this.hashType);
  }

  @Override
  public void add(Key key) {
    if (key == null) {
      throw new NullPointerException("Key can not be null");
    }

    org.apache.hadoop.util.bloom.BloomFilter bf = getActiveStandardBF();

    if (bf == null) {
      addRow();
      bf = matrix[matrix.length - 1];
      currentNbRecord = 0;
    }

    bf.add(key);

    currentNbRecord++;
  }

  @Override
  public void and(InternalFilter filter) {
    if (!(filter instanceof InternalDynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    InternalDynamicBloomFilter dbf = (InternalDynamicBloomFilter) filter;

    if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    for (int i = 0; i < matrix.length; i++) {
      matrix[i].and(dbf.matrix[i]);
    }
  }

  @Override
  public boolean membershipTest(Key key) {
    if (key == null) {
      return true;
    }

    for (BloomFilter bloomFilter : matrix) {
      if (bloomFilter.membershipTest(key)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void not() {
    for (BloomFilter bloomFilter : matrix) {
      bloomFilter.not();
    }
  }

  @Override
  public void or(InternalFilter filter) {
    if (!(filter instanceof InternalDynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }

    InternalDynamicBloomFilter dbf = (InternalDynamicBloomFilter) filter;

    if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].or(dbf.matrix[i]);
    }
  }

  @Override
  public void xor(InternalFilter filter) {
    if (!(filter instanceof InternalDynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }
    InternalDynamicBloomFilter dbf = (InternalDynamicBloomFilter) filter;

    if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }

    for (int i = 0; i < matrix.length; i++) {
      matrix[i].xor(dbf.matrix[i]);
    }
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();

    for (BloomFilter bloomFilter : matrix) {
      res.append(bloomFilter);
      res.append(Character.LINE_SEPARATOR);
    }
    return res.toString();
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(nr);
    out.writeInt(currentNbRecord);
    out.writeInt(matrix.length);
    for (BloomFilter bloomFilter : matrix) {
      bloomFilter.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    nr = in.readInt();
    currentNbRecord = in.readInt();
    int len = in.readInt();
    matrix = new org.apache.hadoop.util.bloom.BloomFilter[len];
    for (int i = 0; i < matrix.length; i++) {
      matrix[i] = new org.apache.hadoop.util.bloom.BloomFilter();
      matrix[i].readFields(in);
    }
  }

  /**
   * Adds a new row to <i>this</i> dynamic Bloom filter.
   */
  private void addRow() {
    BloomFilter[] tmp = new BloomFilter[matrix.length + 1];
    System.arraycopy(matrix, 0, tmp, 0, matrix.length);
    tmp[tmp.length - 1] = new BloomFilter(vectorSize, nbHash, hashType);
    matrix = tmp;
  }

  /**
   * Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
   *
   * @return BloomFilter The active standard Bloom filter.
   * <code>Null</code> otherwise.
   */
  private BloomFilter getActiveStandardBF() {
    if (reachedMax) {
      return matrix[curMatrixIndex++ % matrix.length];
    }

    if (currentNbRecord >= nr && (matrix.length * nr) < maxNr) {
      return null;
    } else if (currentNbRecord >= nr && (matrix.length * nr) >= maxNr) {
      reachedMax = true;
      return matrix[0];
    }
    return matrix[matrix.length - 1];
  }
}
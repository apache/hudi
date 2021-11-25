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

package org.apache.hudi.optimize;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Converts between Hilbert index ({@code BigInteger}) and N-dimensional points.
 *
 * Note:
 * <a href="https://github.com/davidmoten/hilbert-curve/blob/master/src/main/java/org/davidmoten/hilbert/HilbertCurve.java">GitHub</a>).
 * the Licensed of above link is also http://www.apache.org/licenses/LICENSE-2.0
 */
public final class HilbertCurve {

  private final int bits;
  private final int dimensions;
  // cached calculations
  private final int length;

  private HilbertCurve(int bits, int dimensions) {
    this.bits = bits;
    this.dimensions = dimensions;
    // cache a calculated values for small perf improvements
    this.length = bits * dimensions;
  }

  /**
   * Returns a builder for and object that performs transformations for a Hilbert
   * curve with the given number of bits.
   *
   * @param bits depth of the Hilbert curve. If bits is one, this is the top-level Hilbert curve
   * @return builder for object to do transformations with the Hilbert Curve
   */
  public static Builder bits(int bits) {
    return new Builder(bits);
  }

  /**
   * Builds a {@link HilbertCurve} instance.
   */
  public static final class Builder {
    final int bits;

    private Builder(int bits) {
      if (bits <= 0  || bits >= 64) {
        throw new IllegalArgumentException(String.format("bits must be greater than zero and less than 64, now found bits value: %s", bits));
      }
      this.bits = bits;
    }

    public HilbertCurve dimensions(int dimensions) {
      if (dimensions < 2) {
        throw new IllegalArgumentException(String.format("dimensions must be at least 2, now found dimensions value: %s", dimensions));
      }
      return new HilbertCurve(bits, dimensions);
    }
  }

  /**
   * Converts a point to its Hilbert curve index.
   *
   * @param point an array of dimensions
   * @return hilbert index
   * @throws IllegalArgumentException if length of point array is not equal to the number of dimensions.
   */
  public BigInteger index(long[] point) {
    if (point.length != dimensions) {
      throw new IllegalArgumentException(String.format("length of point array must equal to the number of dimensions"));
    }
    return toIndex(transposedIndex(bits, point));
  }

  public byte[] indexBytes(long[] point) {
    if (point.length != dimensions) {
      throw new IllegalArgumentException(String.format("length of point array must equal to the number of dimensions"));
    }
    return toIndexBytes(transposedIndex(bits, point));
  }

  /**
   * Converts a {@link BigInteger} index (distance along the Hilbert Curve from 0)
   * to a point of dimensions defined in the constructor of {@code this}.
   *
   * @param index hilbert index
   * @return array of longs being the point
   * @throws NullPointerException if index is null
   * @throws IllegalArgumentException if index is negative
   */
  public long[] point(BigInteger index) {
    if (index == null) {
      throw new NullPointerException("index must not be null");
    }
    if (index.signum() == -1) {
      throw new IllegalArgumentException("index cannot be negative");
    }
    return transposedIndexToPoint(bits, transpose(index));
  }

  public void point(BigInteger index, long[] x) {
    if (index == null) {
      throw new NullPointerException("index must not be null");
    }
    if (index.signum() == -1) {
      throw new IllegalArgumentException("index cannot be negative");
    }
    Arrays.fill(x, 0);
    transpose(index, x);
    transposedIndexToPoint(bits, x);
  }

  public void point(long i, long[] x) {
    point(BigInteger.valueOf(i), x);
  }

  /**
   * Converts a {@code long} index (distance along the Hilbert Curve from 0) to a
   * point of dimensions defined in the constructor of {@code this}.
   *
   * @param index hilbert index
   * @return array of longs being the point
   * @throws IllegalArgumentException if index is negative
   */
  public long[] point(long index) {
    return point(BigInteger.valueOf(index));
  }

  /**
   * Returns the transposed representation of the Hilbert curve index.
   * The Hilbert index is expressed internally as an array of transposed bits.
   * Example: 5 bits for each of n=3 coordinates.
   * 15-bit Hilbert integer = A B C D E F G H I J K L M N O is stored as its Transpose:
   * X[0] = A D G J M
   * X[1] = B E H K N
   * X[2] = C F I L O
   *
   * @param index index to be tranposed
   * @return transposed index
   */
  long[] transpose(BigInteger index) {
    long[] x = new long[dimensions];
    transpose(index, x);
    return x;
  }

  private void transpose(BigInteger index, long[] x) {
    byte[] b = index.toByteArray();
    for (int idx = 0; idx < 8 * b.length; idx++) {
      if ((b[b.length - 1 - idx / 8] & (1L << (idx % 8))) != 0) {
        int dim = (length - idx - 1) % dimensions;
        int shift = (idx / dimensions) % bits;
        x[dim] |= 1L << shift;
      }
    }
  }

  /**
   * Given the axes (coordinates) of a point in N-Dimensional space, find the
   * distance to that point along the Hilbert curve. That distance will be
   * transposed; broken into pieces and distributed into an array.
   * The number of dimensions is the length of the hilbertAxes array.
   * Note: In Skilling's paper, this function is called AxestoTranspose.
   *
   * @param bits
   * @param point Point in N-space.
   * @return The Hilbert distance (or index) as a transposed Hilbert index.
   */
  static long[] transposedIndex(int bits, long[] point) {
    final long M = 1L << (bits - 1);
    final int n = point.length; // n: Number of dimensions
    final long[] x = Arrays.copyOf(point, n);
    long p;
    long q;
    long t;
    int i;
    // Inverse undo
    for (q = M; q > 1; q >>= 1) {
      p = q - 1;
      for (i = 0; i < n; i++) {
        if ((x[i] & q) != 0) {
          x[0] ^= p; // invert
        } else {
          t = (x[0] ^ x[i]) & p;
          x[0] ^= t;
          x[i] ^= t;
        }
      }
    } // exchange
    // Gray encode
    for (i = 1; i < n; i++) {
      x[i] ^= x[i - 1];
    }
    t = 0;
    for (q = M; q > 1; q >>= 1) {
      if ((x[n - 1] & q) != 0) {
        t ^= q - 1;
      }
    }
    for (i = 0; i < n; i++) {
      x[i] ^= t;
    }
    return x;
  }

  /**
   * Converts the Hilbert transposed index into an N-dimensional point expressed as a vector of {@code long}.
   * In Skilling's paper this function is named {@code TransposeToAxes}.
   *
   * @param bits
   * @param x
   * @return the coordinates of the point represented by the transposed index on the Hilbert curve.
   */
  static long[] transposedIndexToPoint(int bits, long[] x) {
    final long N = 2L << (bits - 1);
    // Note that x is mutated by this method (as a performance improvement
    // to avoid allocation)
    int n = x.length; // number of dimensions
    long p;
    long q;
    long t;
    int i;
    // Gray decode by H ^ (H/2)
    t = x[n - 1] >> 1;
    // Corrected error in Skilling's paper on the following line. The
    // appendix had i >= 0 leading to negative array index.
    for (i = n - 1; i > 0; i--) {
      x[i] ^= x[i - 1];
    }
    x[0] ^= t;
    // Undo excess work
    for (q = 2; q != N; q <<= 1) {
      p = q - 1;
      for (i = n - 1; i >= 0; i--) {
        if ((x[i] & q) != 0L) {
          x[0] ^= p; // invert
        } else {
          t = (x[0] ^ x[i]) & p;
          x[0] ^= t;
          x[i] ^= t;
        }
      }
    } // exchange
    return x;
  }

  // Quote from Paul Chernoch
  // Interleaving means take one bit from the first matrix element, one bit
  // from the next, etc, then take the second bit from the first matrix
  // element, second bit from the second, all the way to the last bit of the
  // last element. Combine those bits in that order into a single BigInteger,
  // which can have as many bits as necessary. This converts the array into a
  // single number.
  BigInteger toIndex(long[] transposedIndex) {
    return new BigInteger(1, toIndexBytes(transposedIndex));
  }

  byte[] toIndexBytes(long[] transposedIndex) {
    byte[] b = new byte[length];
    int bIndex = length - 1;
    long mask = 1L << (bits - 1);
    for (int i = 0; i < bits; i++) {
      for (int j = 0; j < transposedIndex.length; j++) {
        if ((transposedIndex[j] & mask) != 0) {
          b[length - 1 - bIndex / 8] |= 1 << (bIndex % 8);
        }
        bIndex--;
      }
      mask >>= 1;
    }
    // b is expected to be BigEndian
    return b;
  }
}
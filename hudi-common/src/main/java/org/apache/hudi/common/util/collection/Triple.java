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

package org.apache.hudi.common.util.collection;

import java.io.Serializable;

/**
 * (NOTE: Adapted from Apache commons-lang3)
 * <p>
 * A triple consisting of three elements.
 * </p>
 *
 * <p>
 * This class is an abstract implementation defining the basic API. It refers to the elements as 'left', 'middle' and
 * 'right'.
 * </p>
 *
 * <p>
 * Subclass implementations may be mutable or immutable. However, there is no restriction on the type of the stored
 * objects that may be stored. If mutable objects are stored in the triple, then the triple itself effectively becomes
 * mutable.
 * </p>
 *
 * @param <L> the left element type
 * @param <M> the middle element type
 * @param <R> the right element type
 */
public abstract class Triple<L, M, R> implements Comparable<Triple<L, M, R>>, Serializable {

  /**
   * Serialization version.
   */
  private static final long serialVersionUID = 1L;

  /**
   * <p>
   * Obtains an immutable triple of from three objects inferring the generic types.
   * </p>
   *
   * <p>
   * This factory allows the triple to be created using inference to obtain the generic types.
   * </p>
   *
   * @param <L> the left element type
   * @param <M> the middle element type
   * @param <R> the right element type
   * @param left the left element, may be null
   * @param middle the middle element, may be null
   * @param right the right element, may be null
   * @return a triple formed from the three parameters, not null
   */
  public static <L, M, R> Triple<L, M, R> of(final L left, final M middle, final R right) {
    return new ImmutableTriple<L, M, R>(left, middle, right);
  }

  // -----------------------------------------------------------------------

  /**
   * <p>
   * Gets the left element from this triple.
   * </p>
   *
   * @return the left element, may be null
   */
  public abstract L getLeft();

  /**
   * <p>
   * Gets the middle element from this triple.
   * </p>
   *
   * @return the middle element, may be null
   */
  public abstract M getMiddle();

  /**
   * <p>
   * Gets the right element from this triple.
   * </p>
   *
   * @return the right element, may be null
   */
  public abstract R getRight();

  // -----------------------------------------------------------------------

  /**
   * <p>
   * Compares the triple based on the left element, followed by the middle element, finally the right element. The types
   * must be {@code Comparable}.
   * </p>
   *
   * @param other the other triple, not null
   * @return negative if this is less, zero if equal, positive if greater
   */
  @Override
  public int compareTo(final Triple<L, M, R> other) {
    checkComparable(this);
    checkComparable(other);

    Comparable thisLeft = (Comparable) getLeft();
    Comparable otherLeft = (Comparable) other.getLeft();

    if (thisLeft.compareTo(otherLeft) == 0) {
      return Pair.of(getMiddle(), getRight()).compareTo(Pair.of(other.getMiddle(), other.getRight()));
    } else {
      return thisLeft.compareTo(otherLeft);
    }
  }

  /**
   * <p>
   * Compares this triple to another based on the three elements.
   * </p>
   *
   * @param obj the object to compare to, null returns false
   * @return true if the elements of the triple are equal
   */
  @SuppressWarnings("deprecation") // ObjectUtils.equals(Object, Object) has been deprecated in 3.2
  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Triple<?, ?, ?>) {
      final Triple<?, ?, ?> other = (Triple<?, ?, ?>) obj;
      return getLeft().equals(other.getLeft()) && getMiddle().equals(other.getMiddle())
          && getRight().equals(other.getRight());
    }
    return false;
  }

  /**
   * <p>
   * Returns a suitable hash code.
   * </p>
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return (getLeft() == null ? 0 : getLeft().hashCode()) ^ (getMiddle() == null ? 0 : getMiddle().hashCode())
        ^ (getRight() == null ? 0 : getRight().hashCode());
  }

  /**
   * <p>
   * Returns a String representation of this triple using the format {@code ($left,$middle,$right)}.
   * </p>
   *
   * @return a string describing this object, not null
   */
  @Override
  public String toString() {
    return "(" + getLeft() + ',' + getMiddle() + ','
        + getRight() + ')';
  }

  /**
   * <p>
   * Formats the receiver using the given format.
   * </p>
   *
   * <p>
   * This uses {@link java.util.Formattable} to perform the formatting. Three variables may be used to embed the left
   * and right elements. Use {@code %1$s} for the left element, {@code %2$s} for the middle and {@code %3$s} for the
   * right element. The default format used by {@code toString()} is {@code (%1$s,%2$s,%3$s)}.
   * </p>
   *
   * @param format the format string, optionally containing {@code %1$s}, {@code %2$s} and {@code %3$s}, not null
   * @return the formatted string, not null
   */
  public String toString(final String format) {
    return String.format(format, getLeft(), getMiddle(), getRight());
  }

  private void checkComparable(Triple<L, M, R> triplet) {
    if (!(triplet.getLeft() instanceof Comparable) || !(triplet.getMiddle() instanceof Comparable)
        || !(triplet.getRight() instanceof Comparable)) {
      throw new IllegalArgumentException("Elements of Triple must implement Comparable :" + triplet);
    }
  }
}


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
import java.util.Map;

/**
  * (NOTE: Adapted from Apache commons-lang3)
  *
  * <p>A pair consisting of two elements.
  *
  * <p>This class is an abstract implementation defining the basic API. It refers to the elements as
  * 'left' and 'right'. It also implements the {@code Map.Entry} interface where the key is 'left'
  * and the value is 'right'.
  *
  * <p>Subclass implementations may be mutable or immutable. However, there is no restriction on the
  * type of the stored objects that may be stored. If mutable objects are stored in the pair, then
  * the pair itself effectively becomes mutable.
  *
  * @param <L> the left element type
  * @param <R> the right element type
  */
public abstract class Pair<L, R> implements Map.Entry<L, R>, Comparable<Pair<L, R>>, Serializable {

    /** Serialization version. */
    private static final long serialVersionUID = 4954918890077093841L;

    /**
      * Obtains an immutable pair of from two objects inferring the generic types.
      *
      * <p>This factory allows the pair to be created using inference to obtain the generic types.
      *
      * @param <L> the left element type
      * @param <R> the right element type
      * @param left the left element, may be null
      * @param right the right element, may be null
      * @return a pair formed from the two parameters, not null
      */
    public static <L, R> Pair<L, R> of(final L left, final R right) {
        return new ImmutablePair<>(left, right);
    }

    // -----------------------------------------------------------------------

    /**
      * Gets the left element from this pair.
      *
      * <p>When treated as a key-value pair, this is the key.
      *
      * @return the left element, may be null
      */
    public abstract L getLeft();

    /**
      * Gets the right element from this pair.
      *
      * <p>When treated as a key-value pair, this is the value.
      *
      * @return the right element, may be null
      */
    public abstract R getRight();

    /**
      * Gets the key from this pair.
      *
      * <p>This method implements the {@code Map.Entry} interface returning the left element as the
      * key.
      *
      * @return the left element as the key, may be null
      */
    @Override
    public final L getKey() {
        return getLeft();
    }

    /**
      * Gets the value from this pair.
      *
      * <p>This method implements the {@code Map.Entry} interface returning the right element as the
      * value.
      *
      * @return the right element as the value, may be null
      */
    @Override
    public R getValue() {
        return getRight();
    }

    // -----------------------------------------------------------------------

    /**
      * Compares the pair based on the left element followed by the right element. The types must be
      * {@code Comparable}.
      *
      * @param other the other pair, not null
      * @return negative if this is less, zero if equal, positive if greater
      */
    @Override
    public int compareTo(final Pair<L, R> other) {

        checkComparable(this);
        checkComparable(other);

        Comparable thisLeft = (Comparable) getLeft();
        Comparable thisRight = (Comparable) getRight();
        Comparable otherLeft = (Comparable) other.getLeft();
        Comparable otherRight = (Comparable) other.getRight();

        if (thisLeft.compareTo(otherLeft) == 0) {
            return thisRight.compareTo(otherRight);
        } else {
            return thisLeft.compareTo(otherLeft);
        }
    }

    /**
      * Compares this pair to another based on the two elements.
      *
      * @param obj the object to compare to, null returns false
      * @return true if the elements of the pair are equal
      */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Map.Entry<?, ?>) {
            final Map.Entry<?, ?> other = (Map.Entry<?, ?>) obj;
            return getKey().equals(other.getKey()) && getValue().equals(other.getValue());
        }
        return false;
    }

    /**
      * Returns a suitable hash code. The hash code follows the definition in {@code Map.Entry}.
      *
      * @return the hash code
      */
    @Override
    public int hashCode() {
        // see Map.Entry API specification
        return (getKey() == null ? 0 : getKey().hashCode())
                ^ (getValue() == null ? 0 : getValue().hashCode());
    }

    /**
      * Returns a String representation of this pair using the format {@code ($left,$right)}.
      *
      * @return a string describing this object, not null
      */
    @Override
    public String toString() {
        return new StringBuilder()
                .append('(')
                .append(getLeft())
                .append(',')
                .append(getRight())
                .append(')')
                .toString();
    }

    /**
      * Formats the receiver using the given format.
      *
      * <p>This uses {@link java.util.Formattable} to perform the formatting. Two variables may be used
      * to embed the left and right elements. Use {@code %1$s} for the left element (key) and {@code
      * %2$s} for the right element (value). The default format used by {@code toString()} is {@code
      * (%1$s,%2$s)}.
      *
      * @param format the format string, optionally containing {@code %1$s} and {@code %2$s}, not null
      * @return the formatted string, not null
      */
    public String toString(final String format) {
        return String.format(format, getLeft(), getRight());
    }

    private void checkComparable(Pair<L, R> pair) {
        if (!(pair.getLeft() instanceof Comparable) || !(pair.getRight() instanceof Comparable)) {
            throw new IllegalArgumentException("Elements of Pair must implement Comparable :" + pair);
        }
    }
}

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

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A wrapper class to manage multiple ordering fields across Hudi.
 *
 * <p>Currently only multiple fields ordering value utilizes this abstraction,
 * for single field ordering value, {@link Comparable} is used directly instead.</p>
 *
 * @see org.apache.hudi.common.util.OrderingValues
 */
public class ArrayComparable implements Comparable<ArrayComparable>, Serializable, Collection {
  private static final long serialVersionUID = 1L;

  private final Comparable[] values;

  public ArrayComparable(Comparable[] values) {
    this.values = values;
  }

  public List<Comparable> getValues() {
    return Arrays.asList(values);
  }

  public ArrayComparable apply(Function<Comparable, Comparable> mappingFunction) {
    return new ArrayComparable(Arrays.stream(values).map(mappingFunction).toArray(Comparable[]::new));
  }

  public boolean isValueSameClass(ArrayComparable other) {
    for (int i = 0; i < this.values.length; i++) {
      if (this.values[i].getClass() != other.values[i].getClass()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(ArrayComparable otherOrderingValue) {
    int minLength = Math.min(values.length, otherOrderingValue.values.length);
    // Compare elements up to the shorter array's length
    for (int i = 0; i < minLength; i++) {
      int comparingValue = values[i].compareTo(otherOrderingValue.values[i]);
      if (comparingValue != 0) {
        return comparingValue;
      }
    }
    // If all compared elements are equal, the shorter array comes first
    return Integer.compare(values.length, otherOrderingValue.values.length);
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean contains(Object o) {
    for (Comparable value : values) {
      if (value.equals(o)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public @NotNull Iterator iterator() {
    return Arrays.stream(values).iterator();
  }

  @Override
  public @NotNull Object[] toArray() {
    return values;
  }

  @Override
  public boolean add(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(@NotNull Collection c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(@NotNull Collection c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(@NotNull Collection c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(@NotNull Collection c) {
    return new HashSet<>(Arrays.asList(values)).containsAll(c);
  }

  @Override
  public @NotNull Object[] toArray(@NotNull Object[] a) {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArrayComparable that = (ArrayComparable) o;
    if (values.length != that.values.length) {
      return false;
    }
    for (int i = 0; i < values.length; i++) {
      Comparable objComparable = values[i];
      Comparable otherObjComparable = that.values[i];
      if (objComparable == null && otherObjComparable == null) {
        // if both are null continue
        continue;
      } else if (objComparable == null || otherObjComparable == null) {
        // One comparable is null while other is not null
        return false;
      } else if (!objComparable.equals(otherObjComparable)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(values);
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}

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

package org.apache.hudi.index;

import org.apache.hudi.common.util.Option;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Marker implements Comparable<Marker> {

  public enum Bound {
    BELOW,
    EXACTLY,
    ABOVE
  }

  private final Type type;
  private final Option<Object> value;
  private final Bound bound;

  public Marker(Type type, Option<Object> value, Bound bound) {
    if (!value.isPresent() && bound == Bound.EXACTLY) {
      throw new IllegalArgumentException("bound cannot be exact with no value!");
    }

    this.type = type;
    this.value = value;
    this.bound = bound;
  }

  @Override
  public int hashCode() {
    int hash = Objects.hash(type, bound);
    if (value.isPresent()) {
      hash = hash * 31 + (int) value.get().hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Marker other = (Marker) obj;
    return Objects.equals(this.type, other.type)
            && Objects.equals(this.bound, other.bound)
            && ((this.value.isPresent()) == (other.value.isPresent()));
  }

  public Type getType() {
    return type;
  }

  public Bound getBound() {
    return bound;
  }

  @Override
  public int compareTo(Marker o) {
    checkTypeCompatibility(o);
    if (isUpperUnbounded()) {
      return o.isUpperUnbounded() ? 0 : 1;
    }
    if (isLowerUnbounded()) {
      return o.isLowerUnbounded() ? 0 : -1;
    }
    if (o.isUpperUnbounded()) {
      return -1;
    }
    if (o.isLowerUnbounded()) {
      return 1;
    }
    // INVARIANT: value and o.value are present

    int compare = value.get().toString().compareTo(o.value.get().toString());
    if (compare == 0) {
      if (bound == o.bound) {
        return 0;
      }
      if (bound == Bound.BELOW) {
        return -1;
      }
      if (bound == Bound.ABOVE) {
        return 1;
      }
      // INVARIANT: bound == EXACTLY
      return (o.bound == Bound.BELOW) ? 1 : -1;
    }
    return compare;
  }

  public boolean isUpperUnbounded() {
    return !value.isPresent() && bound == Bound.BELOW;
  }

  public boolean isLowerUnbounded() {
    return !value.isPresent() && bound == Bound.ABOVE;
  }

  private void checkTypeCompatibility(Marker marker) {
    if (!type.equals(marker.getType())) {
      throw new IllegalArgumentException(String.format("Mismatched Marker types: %s vs %s", type, marker.getType()));
    }
  }

  public static Marker lowerUnbounded(Type type) {
    return new Marker(type, Option.empty(), Bound.ABOVE);
  }

  public static Marker upperUnbounded(Type type) {
    return new Marker(type, Option.empty(), Bound.BELOW);
  }

  public boolean isAdjacent(Marker other) {
    checkTypeCompatibility(other);
    if (isUpperUnbounded() || isLowerUnbounded() || other.isUpperUnbounded() || other.isLowerUnbounded()) {
      return false;
    }
    if (type.compareTo(other.type) != 0) {
      return false;
    }
    return (bound == Bound.EXACTLY && other.bound != Bound.EXACTLY)
            || (bound != Bound.EXACTLY && other.bound == Bound.EXACTLY);
  }

  public static Marker min(Marker marker1, Marker marker2) {
    return marker1.compareTo(marker2) <= 0 ? marker1 : marker2;
  }

  public static Marker max(Marker marker1, Marker marker2) {
    return marker1.compareTo(marker2) >= 0 ? marker1 : marker2;
  }

  public static Marker above(Type type, Object value) {
    requireNonNull(type, "type is null");
    requireNonNull(value, "value is null");
    return create(type, Option.of(value), Bound.ABOVE);
  }

  public static Marker exactly(Type type, Object value) {
    requireNonNull(type, "type is null");
    requireNonNull(value, "value is null");
    return create(type, Option.of(value), Bound.EXACTLY);
  }

  public static Marker below(Type type, Object value) {
    requireNonNull(type, "type is null");
    requireNonNull(value, "value is null");
    return create(type, Option.of(value), Bound.BELOW);
  }

  private static Marker create(Type type, Option<Object> value, Bound bound) {
    return new Marker(type, value, bound);
  }
}

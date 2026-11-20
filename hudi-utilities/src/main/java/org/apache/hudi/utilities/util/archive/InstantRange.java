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

package org.apache.hudi.utilities.util.archive;

import java.util.Objects;

/**
 * A closed range of instant times, or the unbounded range covering every instant.
 *
 * <p>Instant times compare as strings, which is exact rather than approximate: {@code
 * InstantComparison}'s predicates are all {@code commit1.compareTo(commit2)}.
 */
public final class InstantRange {

  private static final InstantRange ALL = new InstantRange(null, null);

  private final String min;
  private final String max;

  private InstantRange(String min, String max) {
    this.min = min;
    this.max = max;
  }

  /** The range covering every instant. Used when a file's true coverage could not be determined. */
  public static InstantRange all() {
    return ALL;
  }

  /**
   * @param min the inclusive lower bound
   * @param max the inclusive upper bound; must not sort before {@code min}
   */
  public static InstantRange of(String min, String max) {
    Objects.requireNonNull(min, "min");
    Objects.requireNonNull(max, "max");
    if (min.compareTo(max) > 0) {
      throw new IllegalArgumentException("min " + min + " sorts after max " + max);
    }
    return new InstantRange(min, max);
  }

  /** True when this range's bounds are unknown, so it must be assumed to cover everything. */
  public boolean isAll() {
    return this == ALL;
  }

  public boolean contains(String instant) {
    return isAll() || (min.compareTo(instant) <= 0 && max.compareTo(instant) >= 0);
  }

  public String min() {
    return min;
  }

  public String max() {
    return max;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof InstantRange)) {
      return false;
    }
    InstantRange that = (InstantRange) other;
    return Objects.equals(min, that.min) && Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max);
  }

  @Override
  public String toString() {
    return isAll() ? "[all]" : "[" + min + ", " + max + "]";
  }
}

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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Range {

  private final Marker low;
  private final Marker high;

  public Range(Marker low, Marker high) {
    requireNonNull(low, "value is null");
    requireNonNull(high, "value is null");
    if (!low.getType().equals(high.getType())) {
      throw new IllegalArgumentException(String.format("Marker types do not match: %s vs %s", low.getType(), high.getType()));
    }
    if (low.getBound() == Marker.Bound.BELOW) {
      throw new IllegalArgumentException("low bound must be EXACTLY or ABOVE");
    }
    if (high.getBound() == Marker.Bound.ABOVE) {
      throw new IllegalArgumentException("high bound must be EXACTLY or BELOW");
    }
    if (low.compareTo(high) > 0) {
      throw new IllegalArgumentException("low must be less than or equal to high");
    }
    this.low = low;
    this.high = high;
  }

  @Override
  public int hashCode() {
    return Objects.hash(low, high);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Range other = (Range) obj;
    return Objects.equals(this.low, other.low)
            && Objects.equals(this.high, other.high);
  }

  public Marker getLow() {
    return low;
  }

  public Marker getHigh() {
    return high;
  }

  public Type getType() {
    return low.getType();
  }

  public boolean isAll() {
    return low.isLowerUnbounded() && high.isUpperUnbounded();
  }

  public static Range all(Type type) {
    return new Range(Marker.lowerUnbounded(type), Marker.upperUnbounded(type));
  }

  private void checkTypeCompatibility(Range range) {
    if (!getType().equals(range.getType())) {
      throw new IllegalArgumentException(String.format("Mismatched Range types: %s vs %s", getType(), range.getType()));
    }
  }

  private void checkTypeCompatibility(Marker marker) {
    if (!getType().equals(marker.getType())) {
      throw new IllegalArgumentException(String.format("Marker of %s does not match Range of %s", marker.getType(), getType()));
    }
  }

  public boolean overlaps(Range other) {
    checkTypeCompatibility(other);
    return this.getLow().compareTo(other.getHigh()) <= 0
            && other.getLow().compareTo(this.getHigh()) <= 0;
  }

  public Range span(Range other) {
    checkTypeCompatibility(other);
    Marker lowMarker = Marker.min(low, other.getLow());
    Marker highMarker = Marker.max(high, other.getHigh());
    return new Range(lowMarker, highMarker);
  }

  public boolean includes(Marker marker) {
    requireNonNull(marker, "marker is null");
    checkTypeCompatibility(marker);
    return low.compareTo(marker) <= 0 && high.compareTo(marker) >= 0;
  }
}

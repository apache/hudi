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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;
import static org.apache.hudi.index.Type.BOOLEAN;

public class ValueSet {

  private final Type type;
  private final NavigableMap<Marker, Range> ranges;

  public Type getType() {
    return type;
  }

  public Map<Marker, Range> getRanges() {
    return ranges;
  }

  public ValueSet(Type type, NavigableMap<Marker, Range> ranges) {
    this.type = type;
    this.ranges = ranges;
  }

  public boolean isNone() {
    return ranges.isEmpty();
  }

  public boolean isAll() {
    return ranges.size() == 1 && ranges.values().iterator().next().isAll();
  }

  public static ValueSet all(Type type) {
    return copyOf(type, Collections.singletonList(Range.all(type)));
  }

  static ValueSet copyOf(Type type, Iterable<Range> ranges) {
    return new Builder(type).addAll(ranges).build();
  }

  static class Builder {
    private final Type type;
    private final List<Range> ranges = new ArrayList<>();

    Builder(Type type) {
      requireNonNull(type, "type is null");
      this.type = type;
    }

    Builder add(Range range) {
      if (!type.equals(range.getType())) {
        throw new IllegalArgumentException(String.format("Range type %s does not match builder type %s", range.getType(), type));
      }

      ranges.add(range);
      return this;
    }

    Builder addAll(Iterable<Range> ranges) {
      for (Range range : ranges) {
        add(range);
      }
      return this;
    }

    ValueSet build() {
      Collections.sort(ranges, Comparator.comparing(Range::getLow));

      NavigableMap<Marker, Range> result = new TreeMap<>();

      Range current = null;
      for (Range next : ranges) {
        if (current == null) {
          current = next;
          continue;
        }

        if (current.overlaps(next) || current.getHigh().isAdjacent(next.getLow())) {
          current = current.span(next);
        } else {
          result.put(current.getLow(), current);
          current = next;
        }
      }

      if (current != null) {
        result.put(current.getLow(), current);
      }

      // TODO find a more generic way to do this
      if (type == BOOLEAN) {
        boolean trueAllowed = false;
        boolean falseAllowed = false;
        for (Map.Entry<Marker, Range> entry : result.entrySet()) {
          if (entry.getValue().includes(Marker.exactly(BOOLEAN, true))) {
            trueAllowed = true;
          }
          if (entry.getValue().includes(Marker.exactly(BOOLEAN, false))) {
            falseAllowed = true;
          }
        }

        if (trueAllowed && falseAllowed) {
          result = new TreeMap<>();
          result.put(Range.all(BOOLEAN).getLow(), Range.all(BOOLEAN));
          return new ValueSet(BOOLEAN, result);
        }
      }

      return new ValueSet(type, result);
    }
  }
}

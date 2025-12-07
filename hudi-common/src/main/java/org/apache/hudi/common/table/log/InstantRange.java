/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * An instant range used for incremental reader filtering.
 */
@Getter
public abstract class InstantRange implements Serializable {
  private static final long serialVersionUID = 1L;

  protected final Option<String> startInstant;
  protected final Option<String> endInstant;

  public InstantRange(String startInstant, String endInstant) {
    this.startInstant = Option.ofNullable(startInstant);
    this.endInstant = Option.ofNullable(endInstant);
  }

  /**
   * Returns the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public abstract boolean isInRange(String instant);

  @Override
  public String toString() {
    return "InstantRange{"
        + "startInstant='" + (startInstant.isEmpty() ? "-INF" : startInstant.get()) + '\''
        + ", endInstant='" + (endInstant.isEmpty() ? "+INF" : endInstant.get()) + '\''
        + ", rangeType='" + this.getClass().getSimpleName() + '\''
        + '}';
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Represents a range type.
   */
  public enum RangeType {
    /** Start instant is not included (>) and end instant is included (<=). */
    OPEN_CLOSED,
    /** Both start and end instants are included (>=, <=). */
    CLOSED_CLOSED,
    /** Exact match of instants. */
    EXACT_MATCH,
    /** Composition of multiple ranges. */
    COMPOSITION
  }

  private static class OpenClosedRange extends InstantRange {

    public OpenClosedRange(String startInstant, String endInstant) {
      super(Objects.requireNonNull(startInstant), endInstant);
    }

    @Override
    public boolean isInRange(String instant) {
      boolean validAgainstStart = compareTimestamps(instant, GREATER_THAN, startInstant.get());
      // if there is an end instant, check against it, otherwise assume +INF and its always valid.
      boolean validAgainstEnd = endInstant
              .map(e -> compareTimestamps(instant, LESSER_THAN_OR_EQUALS, e))
              .orElse(true);
      return validAgainstStart && validAgainstEnd;
    }
  }

  private static class OpenClosedRangeNullableBoundary extends InstantRange {

    public OpenClosedRangeNullableBoundary(String startInstant, String endInstant) {
      super(startInstant, endInstant);
      ValidationUtils.checkArgument(!startInstant.isEmpty() || !endInstant.isEmpty(),
          "At least one of start and end instants should be specified.");
    }

    @Override
    public boolean isInRange(String instant) {
      boolean validAgainstStart = startInstant
              .map(s -> compareTimestamps(instant, GREATER_THAN, s))
              .orElse(true);
      boolean validAgainstEnd = endInstant
              .map(e -> compareTimestamps(instant, LESSER_THAN_OR_EQUALS, e))
              .orElse(true);

      return validAgainstStart && validAgainstEnd;
    }
  }

  private static class ClosedClosedRange extends InstantRange {

    public ClosedClosedRange(String startInstant, String endInstant) {
      super(Objects.requireNonNull(startInstant), endInstant);
    }

    @Override
    public boolean isInRange(String instant) {
      boolean validAgainstStart = compareTimestamps(instant, GREATER_THAN_OR_EQUALS, startInstant.get());
      boolean validAgainstEnd = endInstant
              .map(e -> compareTimestamps(instant, LESSER_THAN_OR_EQUALS, e))
              .orElse(true);
      return validAgainstStart && validAgainstEnd;
    }
  }

  private static class ClosedClosedRangeNullableBoundary extends InstantRange {

    public ClosedClosedRangeNullableBoundary(String startInstant, String endInstant) {
      super(startInstant, endInstant);
      ValidationUtils.checkArgument(!startInstant.isEmpty() || !endInstant.isEmpty(),
          "At least one of start and end instants should be specified.");
    }

    @Override
    public boolean isInRange(String instant) {
      boolean validAgainstStart = startInstant
              .map(s -> compareTimestamps(instant, GREATER_THAN_OR_EQUALS, s))
              .orElse(true);
      boolean validAgainstEnd = endInstant
              .map(e -> compareTimestamps(instant, LESSER_THAN_OR_EQUALS, e))
              .orElse(true);
      return validAgainstStart && validAgainstEnd;
    }
  }

  /**
   * Class to assist in checking if an instant is part of a set of instants.
   */
  private static class ExactMatchRange extends InstantRange {
    Set<String> instants;

    public ExactMatchRange(Set<String> instants) {
      super(Collections.min(instants), Collections.max(instants));
      this.instants = instants;
    }

    @Override
    public boolean isInRange(String instant) {
      return this.instants.contains(instant);
    }
  }

  /**
   * Composition of multiple instant ranges in disjunctive form.
   */
  private static class CompositionRange extends InstantRange {
    List<InstantRange> instantRanges;

    public CompositionRange(List<InstantRange> instantRanges) {
      super(null, null);
      this.instantRanges = Objects.requireNonNull(instantRanges, "Instant ranges should not be null");
    }

    @Override
    public boolean isInRange(String instant) {
      for (InstantRange range : instantRanges) {
        if (range.isInRange(instant)) {
          return true;
        }
      }
      return false;
    }
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Builder for {@link InstantRange}.
   */
  public static class Builder {
    private String startInstant;
    private String endInstant;
    private RangeType rangeType;
    private boolean nullableBoundary = false;
    private Set<String> explicitInstants;
    private List<InstantRange> instantRanges;

    private Builder() {
    }

    public Builder startInstant(String startInstant) {
      this.startInstant = startInstant;
      return this;
    }

    public Builder endInstant(String endInstant) {
      this.endInstant = endInstant;
      return this;
    }

    public Builder rangeType(RangeType rangeType) {
      this.rangeType = rangeType;
      return this;
    }

    public Builder nullableBoundary(boolean nullable) {
      this.nullableBoundary = nullable;
      return this;
    }

    public Builder explicitInstants(Set<String> instants) {
      this.explicitInstants = instants;
      return this;
    }

    public Builder instantRanges(InstantRange... instantRanges) {
      this.instantRanges = Arrays.stream(instantRanges).collect(Collectors.toList());
      return this;
    }

    public InstantRange build() {
      ValidationUtils.checkState(this.rangeType != null, "Range type is required");
      switch (rangeType) {
        case OPEN_CLOSED:
          return nullableBoundary
              ? new OpenClosedRangeNullableBoundary(startInstant, endInstant)
              : new OpenClosedRange(startInstant, endInstant);
        case CLOSED_CLOSED:
          return nullableBoundary
              ? new ClosedClosedRangeNullableBoundary(startInstant, endInstant)
              : new ClosedClosedRange(startInstant, endInstant);
        case EXACT_MATCH:
          return new ExactMatchRange(this.explicitInstants);
        case COMPOSITION:
          return new CompositionRange(this.instantRanges);
        default:
          throw new AssertionError();
      }
    }
  }
}

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

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * A instant commits range used for incremental reader filtering.
 */
public abstract class InstantRange implements Serializable {
  private static final long serialVersionUID = 1L;

  protected final String startInstant;
  protected final String endInstant;

  public InstantRange(String startInstant, String endInstant) {
    this.startInstant = startInstant;
    this.endInstant = endInstant;
  }

  /**
   * Returns the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public String getStartInstant() {
    return startInstant;
  }

  public String getEndInstant() {
    return endInstant;
  }

  public abstract boolean isInRange(String instant);

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Represents a range type.
   */
  public static enum RangeType {
    OPEN_CLOSE, CLOSE_CLOSE
  }

  private static class OpenCloseRange extends InstantRange {

    public OpenCloseRange(String startInstant, String endInstant) {
      super(Objects.requireNonNull(startInstant), endInstant);
    }

    @Override
    public boolean isInRange(String instant) {
      // No need to do comparison:
      // HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant)
      // because the logic is ensured by the log scanner
      return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN, startInstant);
    }
  }

  private static class OpenCloseRangeNullableBoundary extends InstantRange {

    public OpenCloseRangeNullableBoundary(String startInstant, String endInstant) {
      super(startInstant, endInstant);
      ValidationUtils.checkArgument(startInstant != null || endInstant != null,
          "Start and end instants can not both be null");
    }

    @Override
    public boolean isInRange(String instant) {
      if (startInstant == null) {
        return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant);
      } else if (endInstant == null) {
        return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN, startInstant);
      } else {
        return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN, startInstant)
            && HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant);
      }
    }
  }

  private static class CloseCloseRange extends InstantRange {

    public CloseCloseRange(String startInstant, String endInstant) {
      super(Objects.requireNonNull(startInstant), endInstant);
    }

    @Override
    public boolean isInRange(String instant) {
      // No need to do comparison:
      // HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant)
      // because the logic is ensured by the log scanner
      return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN_OR_EQUALS, startInstant);
    }
  }

  private static class CloseCloseRangeNullableBoundary extends InstantRange {

    public CloseCloseRangeNullableBoundary(String startInstant, String endInstant) {
      super(startInstant, endInstant);
      ValidationUtils.checkArgument(startInstant != null || endInstant != null,
          "Start and end instants can not both be null");
    }

    @Override
    public boolean isInRange(String instant) {
      if (startInstant == null) {
        return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant);
      } else if (endInstant == null) {
        return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN_OR_EQUALS, startInstant);
      } else {
        return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN_OR_EQUALS, startInstant)
            && HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant);
      }
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

    public InstantRange build() {
      ValidationUtils.checkState(this.rangeType != null, "Range type is required");
      switch (rangeType) {
        case OPEN_CLOSE:
          return nullableBoundary
              ? new OpenCloseRangeNullableBoundary(startInstant, endInstant)
              : new OpenCloseRange(startInstant, endInstant);
        case CLOSE_CLOSE:
          return nullableBoundary
              ? new CloseCloseRangeNullableBoundary(startInstant, endInstant)
              : new CloseCloseRange(startInstant, endInstant);
        default:
          throw new AssertionError();
      }
    }
  }
}

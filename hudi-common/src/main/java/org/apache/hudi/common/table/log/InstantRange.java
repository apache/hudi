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
    this.startInstant = Objects.requireNonNull(startInstant);
    this.endInstant = Objects.requireNonNull(endInstant);
  }

  public static InstantRange getInstance(String startInstant, String endInstant, RangeType rangeType) {
    switch (rangeType) {
      case OPEN_CLOSE:
        return new OpenCloseRange(startInstant, endInstant);
      case CLOSE_CLOSE:
        return new CloseCloseRange(startInstant, endInstant);
      default:
        throw new AssertionError();
    }
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
  public enum RangeType {
    OPEN_CLOSE, CLOSE_CLOSE
  }

  private static class OpenCloseRange extends InstantRange {

    public OpenCloseRange(String startInstant, String endInstant) {
      super(startInstant, endInstant);
    }

    @Override
    public boolean isInRange(String instant) {
      // No need to do comparison:
      // HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant)
      // because the logic is ensured by the log scanner
      return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN, startInstant);
    }
  }

  private static class CloseCloseRange extends InstantRange {

    public CloseCloseRange(String startInstant, String endInstant) {
      super(startInstant, endInstant);
    }

    @Override
    public boolean isInRange(String instant) {
      // No need to do comparison:
      // HoodieTimeline.compareTimestamps(instant, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant)
      // because the logic is ensured by the log scanner
      return HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN_OR_EQUALS, startInstant);
    }
  }
}

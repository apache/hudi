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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.v2.BaseTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

/**
 * Hand-built timelines for tests that need no real table on disk: a {@link HoodieTimeline} straight
 * over a list of {@link HoodieInstant}s, through the same {@code BaseTimelineV2} the meta client's
 * {@code TimelineFactory} would produce.
 */
final class TestTimelines {

  private TestTimelines() {
  }

  static HoodieInstant instant(HoodieInstant.State state, String action, String time) {
    return new HoodieInstant(state, action, time, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  static HoodieTimeline of(HoodieInstant... instants) {
    return new BaseTimelineV2(Arrays.stream(instants), new HoodieInstantReader() {
    });
  }

  /**
   * A timeline whose instants carry real content: {@code content.get(instant)} bytes, read back
   * through the same avro deserialisation path a real table's plans go through. Use {@link
   * #serialize} to build well-formed entries, or hand-craft bytes (e.g. garbage) to exercise a read
   * failure.
   */
  static HoodieTimeline withContent(Map<HoodieInstant, byte[]> content, HoodieInstant... instants) {
    return new BaseTimelineV2(Arrays.stream(instants), new HoodieInstantReader() {
      @Override
      public InputStream getContentStream(HoodieInstant instant) {
        byte[] bytes = content.get(instant);
        if (bytes == null) {
          throw new HoodieIOException("No content stubbed for instant " + instant);
        }
        return new ByteArrayInputStream(bytes);
      }
    });
  }

  /** Serialises an avro record the way a real instant file would be written, for {@link #withContent}. */
  static <T extends SpecificRecordBase> byte[] serialize(T metadata, Class<T> clazz) {
    try {
      return TimelineMetadataUtils.serializeAvroMetadata(metadata, clazz).get();
    } catch (IOException e) {
      throw new HoodieIOException("Could not serialise " + clazz, e);
    }
  }

  /** A one-entry {@link InstantStateIndex}: {@code instantTime} recorded at {@code state}. */
  static InstantStateIndex indexOf(String instantTime, InstantState state) {
    HoodieInstant.State timelineState;
    switch (state) {
      case COMPLETED:
        timelineState = HoodieInstant.State.COMPLETED;
        break;
      case INFLIGHT:
        timelineState = HoodieInstant.State.INFLIGHT;
        break;
      case REQUESTED:
        timelineState = HoodieInstant.State.REQUESTED;
        break;
      default:
        throw new IllegalArgumentException("Cannot build a timeline entry for " + state);
    }
    return InstantStateIndex.from(of(instant(timelineState, HoodieTimeline.COMMIT_ACTION, instantTime)));
  }
}

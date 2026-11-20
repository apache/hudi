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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.common.util.ValidationUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The "closest keyframe every X" rule as a stream (a key frame every X seconds): fed
 * a video's keyframe timestamps in ascending order and then the end of the stream, it answers which keyframes are chosen. For
 * each target {@code t = j * interval} ({@code j = 0, 1, ...}, {@code t <= last frame}) the chosen keyframe minimises
 * {@code |keyframe - t|}; a tie goes to the earlier keyframe. A keyframe chosen by several consecutive targets is reported once,
 * so the chosen keyframes come out distinct and ascending.
 *
 * <p>Timestamps are in any one unit, relative to one origin (the frame source's: the container's start time), never negative;
 * the first keyframe may be after it (a trimmed or delayed video). The state is the previous
 * keyframe, the next target and the last chosen keyframe: nothing grows with the video's length (memory use
 * is hard capped regardless of the file size). One instance serves one video.
 */
public final class KeyframeSchedule {

  private static final long NONE = -1L;

  private final long interval;
  private long nextTarget;
  private long previous = NONE;
  private long lastChosen = NONE;
  private boolean ended;

  /**
   * @param interval the target spacing, positive
   */
  public KeyframeSchedule(long interval) {
    ValidationUtils.checkArgument(interval > 0, () -> "keyframe interval must be positive, got " + interval);
    this.interval = interval;
  }

  /**
   * Feeds the next keyframe. Every target up to {@code keyframe} is decided now: each chooses the previous keyframe or this one.
   * So at most two keyframes are newly chosen per call: the previous one (when no target chose it yet) and then this one. A
   * keyframe not after the previous one is ignored.
   *
   * @return the newly chosen keyframes, ascending (zero, one or two)
   */
  public List<Long> onKeyframe(long keyframe) {
    ValidationUtils.checkState(!ended, "keyframe after the end of the stream");
    ValidationUtils.checkArgument(keyframe >= 0, () -> "keyframe timestamp must be >= 0, got " + keyframe);
    if (previous != NONE && keyframe <= previous) {
      return Collections.emptyList();
    }
    List<Long> chosen = new ArrayList<>(2);
    while (nextTarget <= keyframe) {
      // targets up to the previous keyframe were decided by the previous call, so previous < nextTarget here
      long candidate = previous == NONE || keyframe - nextTarget < nextTarget - previous ? keyframe : previous;
      if (candidate != lastChosen) {
        chosen.add(candidate);
        lastChosen = candidate;
      }
      if (candidate == keyframe) {
        // every later target up to this keyframe is closer to it still
        nextTarget += ((keyframe - nextTarget) / interval + 1) * interval;
      } else {
        nextTarget += interval;
      }
    }
    previous = keyframe;
    return chosen;
  }

  /**
   * Ends the stream: the targets after the last keyframe and up to the last frame choose the last keyframe.
   *
   * @param lastFrame the timestamp of the video's last frame
   * @return the last keyframe when it is newly chosen (zero or one keyframe)
   */
  public List<Long> onEnd(long lastFrame) {
    ValidationUtils.checkState(!ended, "the stream already ended");
    ended = true;
    if (previous != NONE && nextTarget <= lastFrame && previous != lastChosen) {
      lastChosen = previous;
      return Collections.singletonList(previous);
    }
    return Collections.emptyList();
  }
}

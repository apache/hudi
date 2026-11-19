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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The streaming closest-keyframe-every-X rule. */
class TestKeyframeSchedule {

  @Test
  void keyframesOnTheGridAreEachChosenOnce() {
    // keyframes every 2 s, targets every 1 s: every keyframe is chosen, the odd targets tie and go to the earlier keyframe
    assertEquals(Arrays.asList(0L, 2000L, 4000L, 6000L), run(1000, new long[] {0, 2000, 4000, 6000}, 6500));
  }

  @Test
  void sparseTargetsSkipKeyframes() {
    // targets every 3 s over keyframes every 1 s: 0, 3, 6, 9
    assertEquals(Arrays.asList(0L, 3000L, 6000L, 9000L),
        run(3000, new long[] {0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000}, 10500));
  }

  @Test
  void tieGoesToTheEarlierKeyframe() {
    // target 3000 is 1000 from both 2000 and 4000; target 1000 is 1000 from both 0 and 2000
    assertEquals(Arrays.asList(0L, 2000L), run(3000, new long[] {0, 2000, 4000}, 4000));
    assertEquals(Arrays.asList(0L, 2000L), run(1000, new long[] {0, 2000}, 2000));
  }

  @Test
  void oneCallCanChooseThePreviousKeyframeAndThisOne() {
    // keyframes 0, 3, 10 with targets every 4: target 4 is closest to 3 (decided when 10 arrives), target 8 to 10
    KeyframeSchedule schedule = new KeyframeSchedule(4);
    assertEquals(Collections.singletonList(0L), schedule.onKeyframe(0));
    assertEquals(Collections.emptyList(), schedule.onKeyframe(3));
    assertEquals(Arrays.asList(3L, 10L), schedule.onKeyframe(10));
    assertEquals(Collections.emptyList(), schedule.onEnd(11));
  }

  @Test
  void targetsAfterTheLastKeyframeChooseIt() {
    // keyframes 0 and 1000, last frame at 9000, targets every 5000: target 5000 (and nothing else) picks 1000 at the end
    assertEquals(Arrays.asList(0L, 1000L), run(5000, new long[] {0, 1000}, 9000));
    // a last keyframe past every target is never chosen
    assertEquals(Collections.singletonList(0L), run(5000, new long[] {0, 4000}, 4500));
  }

  @Test
  void singleKeyframeIsChosenOnce() {
    assertEquals(Collections.singletonList(0L), run(1000, new long[] {0}, 30000));
  }

  @Test
  void keyframeNotAfterThePreviousIsIgnored() {
    KeyframeSchedule schedule = new KeyframeSchedule(1000);
    assertEquals(Collections.singletonList(0L), schedule.onKeyframe(0));
    assertEquals(Collections.emptyList(), schedule.onKeyframe(0));
    assertEquals(Collections.singletonList(1000L), schedule.onKeyframe(1000));
    assertEquals(Collections.emptyList(), schedule.onKeyframe(500));
  }

  @Test
  void matchesTheBruteForceRuleOnRandomStreams() {
    Random random = new Random(42);
    for (int trial = 0; trial < 500; trial++) {
      int count = 1 + random.nextInt(40);
      long[] keyframes = new long[count];
      long t = 0;
      for (int i = 0; i < count; i++) {
        keyframes[i] = t;
        t += 1 + random.nextInt(5000);
      }
      long lastFrame = keyframes[count - 1] + random.nextInt(6000);
      long interval = 1 + random.nextInt(4000);
      assertEquals(bruteForce(interval, keyframes, lastFrame), run(interval, keyframes, lastFrame),
          "interval " + interval + " keyframes " + Arrays.toString(keyframes) + " last " + lastFrame);
    }
  }

  @Test
  void refusesANonPositiveIntervalAndUseAfterTheEnd() {
    assertThrows(IllegalArgumentException.class, () -> new KeyframeSchedule(0));
    KeyframeSchedule schedule = new KeyframeSchedule(1000);
    schedule.onEnd(0);
    assertThrows(IllegalStateException.class, () -> schedule.onKeyframe(0));
    assertThrows(IllegalStateException.class, () -> schedule.onEnd(0));
  }

  private static List<Long> run(long interval, long[] keyframes, long lastFrame) {
    KeyframeSchedule schedule = new KeyframeSchedule(interval);
    List<Long> chosen = new ArrayList<>();
    for (long keyframe : keyframes) {
      chosen.addAll(schedule.onKeyframe(keyframe));
    }
    chosen.addAll(schedule.onEnd(lastFrame));
    return chosen;
  }

  /** The rule as written: every target, the closest keyframe (ties earlier), consecutive repeats collapsed. */
  private static List<Long> bruteForce(long interval, long[] keyframes, long lastFrame) {
    List<Long> chosen = new ArrayList<>();
    for (long target = 0; target <= lastFrame; target += interval) {
      long best = keyframes[0];
      for (long keyframe : keyframes) {
        if (Math.abs(keyframe - target) < Math.abs(best - target)) {
          best = keyframe;
        }
      }
      if (chosen.isEmpty() || chosen.get(chosen.size() - 1).longValue() != best) {
        chosen.add(best);
      }
    }
    return chosen;
  }
}

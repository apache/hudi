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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests against {@link CheckpointUtils}.
 */
public class TestCheckpointUtils {
  private static final String TEST_TOPIC_NAME = "hoodie_test";

  @Test
  public void testStringToOffsets() {
    OffsetRange[] ranges =
        CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
            makeOffsetMap(new int[] {0, 1}, new long[] {300000, 350000}), 1000000L, 0);
    String checkpointStr = CheckpointUtils.offsetsToStr(ranges);
    Map<TopicPartition, Long> offsetMap = CheckpointUtils.strToOffsets(checkpointStr);
    assertEquals(2, offsetMap.size());
    Set<TopicPartition> topicPartitions = new HashSet<>(2);
    TopicPartition partition0 = new TopicPartition(TEST_TOPIC_NAME, 0);
    TopicPartition partition1 = new TopicPartition(TEST_TOPIC_NAME, 1);
    topicPartitions.add(partition0);
    topicPartitions.add(partition1);
    assertEquals(topicPartitions, offsetMap.keySet());
    assertEquals(300000, offsetMap.get(partition0));
    assertEquals(350000, offsetMap.get(partition1));
  }

  @Test
  public void testOffsetToString() {
    OffsetRange[] ranges =
        CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
            makeOffsetMap(new int[] {0, 1}, new long[] {300000, 350000}), 1000000L, 0);
    assertEquals(TEST_TOPIC_NAME + ",0:300000,1:350000", CheckpointUtils.offsetsToStr(ranges));

    ranges = new OffsetRange[] {
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 0, 100),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 100, 200),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 100, 200),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 200, 300)};
    assertEquals(TEST_TOPIC_NAME + ",0:200,1:300", CheckpointUtils.offsetsToStr(ranges));
  }

  @Test
  public void testComputeOffsetRangesWithoutMinPartitions() {
    // test totalNewMessages()
    long totalMsgs = CheckpointUtils.totalNewMessages(new OffsetRange[] {OffsetRange.apply(TEST_TOPIC_NAME, 0, 0, 100),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 100, 200)});
    assertEquals(200, totalMsgs);

    // should consume all the full data
    OffsetRange[] ranges =
        CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
            makeOffsetMap(new int[] {0, 1}, new long[] {300000, 350000}), 1000000L, 0);
    assertEquals(200000, CheckpointUtils.totalNewMessages(ranges));

    // should only consume upto limit
    ranges = CheckpointUtils.computeOffsetRanges(
        makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1}, new long[] {300000, 350000}), 10000, 0);
    assertEquals(10000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(200000, ranges[0].fromOffset());
    assertEquals(205000, ranges[0].untilOffset());
    assertEquals(250000, ranges[1].fromOffset());
    assertEquals(255000, ranges[1].untilOffset());

    // should also consume from new partitions.
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {300000, 350000, 100000}), 1000000L, 0);
    assertEquals(300000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(3, ranges.length);

    // for skewed offsets, does not starve any partition & can catch up
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {200010, 350000, 10000}), 100000, 0);
    assertEquals(100000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(5, ranges.length);
    assertEquals(0, ranges[0].partition());
    assertEquals(10, ranges[0].count());
    assertEquals(1, ranges[1].partition());
    assertEquals(33333, ranges[1].count());
    assertEquals(33333, ranges[2].count());
    assertEquals(23324, ranges[3].count());
    assertEquals(2, ranges[4].partition());
    assertEquals(10000, ranges[4].count());

    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {200010, 350000, 10000}), 1000000, 0);
    assertEquals(110010, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(10, ranges[0].count());
    assertEquals(36670, ranges[1].count());
    assertEquals(36670, ranges[2].count());
    assertEquals(26660, ranges[3].count());
    assertEquals(10000, ranges[4].count());

    // not all partitions consume same entries.
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1, 2, 3, 4}, new long[] {0, 0, 0, 0, 0}),
        makeOffsetMap(new int[] {0, 1, 2, 3, 4}, new long[] {100, 1000, 1000, 1000, 1000}), 1001, 0);
    assertEquals(1001, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(100, ranges[0].count());
    assertEquals(200, ranges[1].count());
    assertEquals(101, ranges[2].count());
    assertEquals(200, ranges[3].count());
    assertEquals(200, ranges[4].count());
    assertEquals(200, ranges[5].count());
  }

  @Test
  public void testComputeOffsetRangesWithMinPartitions() {
    // default(0) minPartitions
    OffsetRange[] ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0}, new long[] {0}),
        makeOffsetMap(new int[] {0}, new long[] {1000}), 300, 0);
    assertEquals(1, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(300, ranges[0].untilOffset());
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {0, 0}),
        makeOffsetMap(new int[] {0, 1}, new long[] {1000, 1000}), 300, 0);
    assertEquals(2, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(150, ranges[0].untilOffset());
    assertEquals(0, ranges[1].fromOffset());
    assertEquals(150, ranges[1].untilOffset());

    // N TopicPartitions to N offset ranges
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1, 2}, new long[] {0, 0, 0}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {1000, 1000, 1000}), 300, 3);
    assertEquals(3, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(100, ranges[0].untilOffset());
    assertEquals(0, ranges[1].fromOffset());
    assertEquals(100, ranges[1].untilOffset());
    assertEquals(0, ranges[1].fromOffset());
    assertEquals(100, ranges[1].untilOffset());

    // 1 TopicPartition to N offset ranges
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0}, new long[] {0}),
        makeOffsetMap(new int[] {0}, new long[] {1000}), 300, 3);
    assertEquals(3, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(100, ranges[0].untilOffset());
    assertEquals(100, ranges[1].fromOffset());
    assertEquals(200, ranges[1].untilOffset());
    assertEquals(200, ranges[2].fromOffset());
    assertEquals(300, ranges[2].untilOffset());

    // N skewed TopicPartitions to M offset ranges
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {0, 0}),
        makeOffsetMap(new int[] {0, 1}, new long[] {100, 500}), 600, 3);
    assertEquals(4, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(100, ranges[0].untilOffset());
    assertEquals(0, ranges[1].fromOffset());
    assertEquals(200, ranges[1].untilOffset());
    assertEquals(200, ranges[2].fromOffset());
    assertEquals(400, ranges[2].untilOffset());
    assertEquals(400, ranges[3].fromOffset());
    assertEquals(500, ranges[3].untilOffset());

    // range inexact multiple of minPartitions
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0}, new long[] {0}),
        makeOffsetMap(new int[] {0}, new long[] {100}), 600, 3);
    assertEquals(4, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(33, ranges[0].untilOffset());
    assertEquals(33, ranges[1].fromOffset());
    assertEquals(66, ranges[1].untilOffset());
    assertEquals(66, ranges[2].fromOffset());
    assertEquals(99, ranges[2].untilOffset());
    assertEquals(99, ranges[3].fromOffset());
    assertEquals(100, ranges[3].untilOffset());

    // do not ignore empty ranges
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {100, 0}),
        makeOffsetMap(new int[] {0, 1}, new long[] {100, 600}), 600, 3);
    assertEquals(4, ranges.length);
    assertEquals(0, ranges[0].partition());
    assertEquals(100, ranges[0].fromOffset());
    assertEquals(100, ranges[0].untilOffset());
    assertEquals(1, ranges[1].partition());
    assertEquals(0, ranges[1].fromOffset());
    assertEquals(200, ranges[1].untilOffset());
    assertEquals(1, ranges[2].partition());
    assertEquals(200, ranges[2].fromOffset());
    assertEquals(400, ranges[2].untilOffset());
    assertEquals(400, ranges[3].fromOffset());
    assertEquals(600, ranges[3].untilOffset());

    // all empty ranges, do not ignore empty ranges
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {100, 0}),
        makeOffsetMap(new int[] {0, 1}, new long[] {100, 0}), 600, 3);
    assertEquals(0, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(2, ranges.length);
    assertEquals(0, ranges[0].partition());
    assertEquals(100, ranges[0].fromOffset());
    assertEquals(100, ranges[0].untilOffset());
    assertEquals(1, ranges[1].partition());
    assertEquals(0, ranges[1].fromOffset());
    assertEquals(0, ranges[1].untilOffset());

    // minPartitions more than maxEvents
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0}, new long[] {0}),
        makeOffsetMap(new int[] {0}, new long[] {2}), 600, 3);
    assertEquals(2, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(1, ranges[0].untilOffset());
    assertEquals(1, ranges[1].fromOffset());
    assertEquals(2, ranges[1].untilOffset());
  }

  @Test
  public void testSplitAndMergeRanges() {
    OffsetRange range = OffsetRange.apply(TEST_TOPIC_NAME, 0, 0, 100);
    OffsetRange[] ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {0, 0}),
        makeOffsetMap(new int[] {0, 1}, new long[] {100, 500}), 600, 4);
    assertEquals(5, ranges.length);
    OffsetRange[] mergedRanges = CheckpointUtils.mergeRangesByTopicPartition(ranges);
    assertEquals(2, mergedRanges.length);
    assertEquals(0, mergedRanges[0].partition());
    assertEquals(0, mergedRanges[0].fromOffset());
    assertEquals(100, mergedRanges[0].untilOffset());
    assertEquals(1, mergedRanges[1].partition());
    assertEquals(0, mergedRanges[1].fromOffset());
    assertEquals(500, mergedRanges[1].untilOffset());

    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0}, new long[] {0}),
        makeOffsetMap(new int[] {0}, new long[] {1000}), 300, 3);
    assertEquals(3, ranges.length);
    mergedRanges = CheckpointUtils.mergeRangesByTopicPartition(ranges);
    assertEquals(1, mergedRanges.length);
    assertEquals(0, mergedRanges[0].fromOffset());
    assertEquals(300, mergedRanges[0].untilOffset());
  }

  @Test
  public void testNumAllocatedEventsGreaterThanNumActualEvents() {
    int[] partitions = new int[] {0, 1, 2, 3, 4};
    long[] committedOffsets =
        new long[] {76888767, 76725043, 76899767, 76833267, 76952055};
    long[] latestOffsets =
        new long[] {77005407, 76768151, 76985456, 76917973, 77080447};
    long numEvents = 400000;
    long minPartitions = 20;
    OffsetRange[] ranges =
        KafkaOffsetGen.CheckpointUtils.computeOffsetRanges(
            makeOffsetMap(partitions, committedOffsets),
            makeOffsetMap(partitions, latestOffsets),
            numEvents,
            minPartitions);

    long totalNewMsgs = KafkaOffsetGen.CheckpointUtils.totalNewMessages(ranges);
    assertEquals(400000, totalNewMsgs);
    for (OffsetRange range : ranges) {
      if (range.fromOffset() > range.untilOffset()) {
        throw new IllegalArgumentException("Invalid offset range " + range);
      }
    }
    long eventPerPartition = numEvents / minPartitions;
    long rangesWhereDiffIsLessThanEventsPerPartition = Arrays.stream(ranges).filter(offsetRange -> offsetRange.untilOffset() - offsetRange.fromOffset() <= eventPerPartition).count();
    assertEquals(ranges.length, rangesWhereDiffIsLessThanEventsPerPartition);
    OffsetRange[] expectedRanges = new OffsetRange[] {
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 76888767, 76908767),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 76908767, 76928767),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 76928767, 76948767),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 76948767, 76968767),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 76968767, 76988767),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 76725043, 76745043),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 76745043, 76765043),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 76765043, 76768151),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 76899767, 76919767),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 76919767, 76939767),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 76939767, 76959767),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 76959767, 76979767),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 76979767, 76985456),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 76833267, 76853267),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 76853267, 76873267),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 76873267, 76893267),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 76893267, 76913267),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 76913267, 76917973),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 76952055, 76972055),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 76972055, 76992055),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 76992055, 77012055),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 77012055, 77032055),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 77032055, 77038552),
    };
    assertArrayEquals(expectedRanges, ranges);
  }

  @Test
  public void testNumAllocatedEventsLesserThanNumActualEvents() {
    int[] partitions = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
    long[] committedOffsets =
        new long[] {788543084, 787746335, 788016034, 788171708, 788327954, 788055939, 788179691, 788004145, 788105897, 788496138, 788317057, 788325907, 788287519, 787958075, 788403560, 788118894,
            788383733, 787273821};
    long[] latestOffsets =
        new long[] {788946534, 788442557, 788712188, 788867819, 789023943, 788752030, 788875648, 788700234, 788802091, 789192155, 789013192, 789021874, 788983544, 788654092, 789099516, 788814985,
            789079650, 787273821};
    long numEvents = 10000000;
    long minPartitions = 36;

    OffsetRange[] ranges =
        KafkaOffsetGen.CheckpointUtils.computeOffsetRanges(
            makeOffsetMap(partitions, committedOffsets),
            makeOffsetMap(partitions, latestOffsets),
            numEvents,
            minPartitions);
    for (OffsetRange range : ranges) {
      if (range.fromOffset() > range.untilOffset()) {
        throw new IllegalArgumentException("Invalid offset range " + range);
      }
    }
    assertEquals(10000000, KafkaOffsetGen.CheckpointUtils.totalNewMessages(ranges));
    assertEquals(41, ranges.length);
    long eventPerPartition = numEvents / minPartitions;
    long rangesWhereDiffIsLessThanEventsPerPartition = Arrays.stream(ranges).filter(offsetRange -> offsetRange.untilOffset() - offsetRange.fromOffset() <= eventPerPartition).count();
    assertEquals(ranges.length, rangesWhereDiffIsLessThanEventsPerPartition);
    OffsetRange[] expectedRanges = new OffsetRange[] {
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 788543084, 788820861),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 788820861, 788946534),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 787746335, 788024112),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 788024112, 788301889),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 788301889, 788442557),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 788016034, 788293811),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 788293811, 788571588),
        OffsetRange.apply(TEST_TOPIC_NAME, 2, 788571588, 788712188),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 788171708, 788449485),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 788449485, 788727262),
        OffsetRange.apply(TEST_TOPIC_NAME, 3, 788727262, 788867819),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 788327954, 788605731),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 788605731, 788883508),
        OffsetRange.apply(TEST_TOPIC_NAME, 4, 788883508, 789023943),
        OffsetRange.apply(TEST_TOPIC_NAME, 5, 788055939, 788333716),
        OffsetRange.apply(TEST_TOPIC_NAME, 5, 788333716, 788611493),
        OffsetRange.apply(TEST_TOPIC_NAME, 5, 788611493, 788752030),
        OffsetRange.apply(TEST_TOPIC_NAME, 6, 788179691, 788457468),
        OffsetRange.apply(TEST_TOPIC_NAME, 6, 788457468, 788735245),
        OffsetRange.apply(TEST_TOPIC_NAME, 6, 788735245, 788740134),
        OffsetRange.apply(TEST_TOPIC_NAME, 7, 788004145, 788281922),
        OffsetRange.apply(TEST_TOPIC_NAME, 7, 788281922, 788559699),
        OffsetRange.apply(TEST_TOPIC_NAME, 8, 788105897, 788383674),
        OffsetRange.apply(TEST_TOPIC_NAME, 8, 788383674, 788661451),
        OffsetRange.apply(TEST_TOPIC_NAME, 9, 788496138, 788773915),
        OffsetRange.apply(TEST_TOPIC_NAME, 9, 788773915, 789051692),
        OffsetRange.apply(TEST_TOPIC_NAME, 10, 788317057, 788594834),
        OffsetRange.apply(TEST_TOPIC_NAME, 10, 788594834, 788872611),
        OffsetRange.apply(TEST_TOPIC_NAME, 11, 788325907, 788603684),
        OffsetRange.apply(TEST_TOPIC_NAME, 11, 788603684, 788881461),
        OffsetRange.apply(TEST_TOPIC_NAME, 12, 788287519, 788565296),
        OffsetRange.apply(TEST_TOPIC_NAME, 12, 788565296, 788843073),
        OffsetRange.apply(TEST_TOPIC_NAME, 13, 787958075, 788235852),
        OffsetRange.apply(TEST_TOPIC_NAME, 13, 788235852, 788513629),
        OffsetRange.apply(TEST_TOPIC_NAME, 14, 788403560, 788681337),
        OffsetRange.apply(TEST_TOPIC_NAME, 14, 788681337, 788959114),
        OffsetRange.apply(TEST_TOPIC_NAME, 15, 788118894, 788396671),
        OffsetRange.apply(TEST_TOPIC_NAME, 15, 788396671, 788674448),
        OffsetRange.apply(TEST_TOPIC_NAME, 16, 788383733, 788661510),
        OffsetRange.apply(TEST_TOPIC_NAME, 16, 788661510, 788939287),
        OffsetRange.apply(TEST_TOPIC_NAME, 17, 787273821, 787273821),
    };
    assertArrayEquals(expectedRanges, ranges);
  }

  private static Map<TopicPartition, Long> makeOffsetMap(int[] partitions, long[] offsets) {
    Map<TopicPartition, Long> map = new HashMap<>();
    for (int i = 0; i < partitions.length; i++) {
      map.put(new TopicPartition(TEST_TOPIC_NAME, partitions[i]), offsets[i]);
    }
    return map;
  }
}

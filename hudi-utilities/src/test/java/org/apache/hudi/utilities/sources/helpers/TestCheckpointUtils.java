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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
  public void testOffsetStringfy() {
    OffsetRange[] ranges =
        CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
            makeOffsetMap(new int[] {0, 1}, new long[] {300000, 350000}), 1000000L, 0);
    assertEquals(TEST_TOPIC_NAME + ",0:200000->300000,1:250000->350000", CheckpointUtils.offsetsStringfy(ranges));

    ranges = new OffsetRange[] {
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 0, 100),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 100, 200),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 100, 200),
        OffsetRange.apply(TEST_TOPIC_NAME, 1, 200, 300)};
    assertEquals(TEST_TOPIC_NAME + ",0:0->100,0:100->200,1:100->200,1:200->300", CheckpointUtils.offsetsStringfy(ranges));
  }

  @Test
  public void testComputeOffsetRanges() {
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
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
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
    assertEquals(10, ranges[0].count());
    assertEquals(89990, ranges[1].count());
    assertEquals(10000, ranges[2].count());

    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {200010, 350000, 10000}), 1000000, 0);
    assertEquals(110010, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(10, ranges[0].count());
    assertEquals(100000, ranges[1].count());
    assertEquals(10000, ranges[2].count());

    // not all partitions consume same entries.
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1, 2, 3, 4}, new long[] {0, 0, 0, 0, 0}),
        makeOffsetMap(new int[] {0, 1, 2, 3, 4}, new long[] {100, 1000, 1000, 1000, 1000}), 1001, 0);
    assertEquals(1001, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(100, ranges[0].count());
    assertEquals(226, ranges[1].count());
    assertEquals(226, ranges[2].count());
    assertEquals(226, ranges[3].count());
    assertEquals(223, ranges[4].count());
  }

  @Test
  public void testSplitRangeToMinPartitions() {
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
    assertEquals(166, ranges[1].untilOffset());
    assertEquals(166, ranges[2].fromOffset());
    assertEquals(333, ranges[2].untilOffset());
    assertEquals(333, ranges[3].fromOffset());
    assertEquals(500, ranges[3].untilOffset());

    // range inexact multiple of minPartitions
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0}, new long[] {0}),
        makeOffsetMap(new int[] {0}, new long[] {100}), 600, 3);
    assertEquals(3, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(33, ranges[0].untilOffset());
    assertEquals(33, ranges[1].fromOffset());
    assertEquals(66, ranges[1].untilOffset());
    assertEquals(66, ranges[2].fromOffset());
    assertEquals(100, ranges[2].untilOffset());

    // ignore empty ranges
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {100, 0}),
        makeOffsetMap(new int[] {0, 1}, new long[] {100, 600}), 600, 3);
    assertEquals(3, ranges.length);
    assertEquals(0, ranges[0].fromOffset());
    assertEquals(200, ranges[0].untilOffset());
    assertEquals(200, ranges[1].fromOffset());
    assertEquals(400, ranges[1].untilOffset());
    assertEquals(400, ranges[2].fromOffset());
    assertEquals(600, ranges[2].untilOffset());

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
        makeOffsetMap(new int[] {0, 1}, new long[] {100, 500}), 600, 3);
    assertEquals(4, ranges.length);
    OffsetRange[] mergedRanges = CheckpointUtils.mergeRangesByTp(ranges);
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
    mergedRanges = CheckpointUtils.mergeRangesByTp(ranges);
    assertEquals(1, mergedRanges.length);
    assertEquals(0, mergedRanges[0].fromOffset());
    assertEquals(300, mergedRanges[0].untilOffset());
  }

  private static Map<TopicPartition, Long> makeOffsetMap(int[] partitions, long[] offsets) {
    Map<TopicPartition, Long> map = new HashMap<>();
    for (int i = 0; i < partitions.length; i++) {
      map.put(new TopicPartition(TEST_TOPIC_NAME, partitions[i]), offsets[i]);
    }
    return map;
  }
}

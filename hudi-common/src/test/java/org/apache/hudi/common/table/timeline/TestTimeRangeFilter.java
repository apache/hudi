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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.table.timeline.IntervalType.CLOSED;
import static org.apache.hudi.common.table.timeline.IntervalType.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimeRangeFilter {

  private HoodieArchivedTimeline.TimeRangeFilter january;
  private HoodieArchivedTimeline.TimeRangeFilter february;
  private HoodieArchivedTimeline.TimeRangeFilter marchOpen;
  private HoodieArchivedTimeline.TimeRangeFilter aprilClosed;
  private HoodieArchivedTimeline.TimeRangeFilter negativeInfinity;
  private HoodieArchivedTimeline.TimeRangeFilter positiveInfinity;
  private HoodieArchivedTimeline.TimeRangeFilter fullRange;

  @BeforeEach
  public void setUp() {

    january = new HoodieArchivedTimeline.TimeRangeFilter("20240101", "20240131");
    february = new HoodieArchivedTimeline.TimeRangeFilter("20240201", "20240229");
    marchOpen = new HoodieArchivedTimeline.OpenOpenTimeRangeFilter("20240301", "20240331");
    aprilClosed = new HoodieArchivedTimeline.TimeRangeFilter("20240401", "20240430");

    // range with infinity
    negativeInfinity = new HoodieArchivedTimeline.EndTsFilter("20240601");
    positiveInfinity = new HoodieArchivedTimeline.StartTsFilter("20240501");
    fullRange = new HoodieArchivedTimeline.FullFilter();
  }

  @Test
  public void testIntersectingRanges() {
    HoodieArchivedTimeline.TimeRangeFilter januaryOverlap = new HoodieArchivedTimeline.TimeRangeFilter("20240115", "20240215");
    assertTrue(january.intersects(januaryOverlap));
    assertTrue(february.intersects(januaryOverlap));
  }

  @Test
  public void testNonIntersectingRanges() {
    HoodieArchivedTimeline.TimeRangeFilter december = new HoodieArchivedTimeline.TimeRangeFilter("20231201", "20231231");
    assertFalse(january.intersects(december));
    assertFalse(february.intersects(december));
  }

  @Test
  public void testSameRange() {
    HoodieArchivedTimeline.TimeRangeFilter januaryCopy = new HoodieArchivedTimeline.TimeRangeFilter("20240101", "20240131");
    assertTrue(january.intersects(januaryCopy));
  }

  @Test
  public void testAdjacentClosedRanges() {
    HoodieArchivedTimeline.TimeRangeFilter endOfJan = new HoodieArchivedTimeline.TimeRangeFilter("20240131", "20240228");
    assertTrue(january.intersects(endOfJan));
  }

  @Test
  public void testAdjacentOpenRanges() {
    HoodieArchivedTimeline.TimeRangeFilter afterJan = new HoodieArchivedTimeline.OpenClosedTimeRangeFilter("20240131", "20240228");
    assertFalse(january.intersects(afterJan));
  }

  @Test
  public void testAdjacentMixedRanges() {
    // 左闭右开 vs 左开右闭
    HoodieArchivedTimeline.TimeRangeFilter leftClosedRightOpen = new HoodieArchivedTimeline.ClosedOpenTimeRangeFilter("20240131", "20240228");
    HoodieArchivedTimeline.TimeRangeFilter leftOpenRightClosed = new HoodieArchivedTimeline.OpenClosedTimeRangeFilter("20240131", "20240228");
    assertTrue(leftClosedRightOpen.intersects(leftOpenRightClosed));
  }

  /* 无穷区间测试 */

  @Test
  public void testNegativeInfinityFinite() {
    assertTrue(negativeInfinity.intersects(january));
    assertTrue(negativeInfinity.intersects(february));
    assertTrue(negativeInfinity.intersects(positiveInfinity));
  }

  @Test
  public void testPositiveInfinityFinite() {
    HoodieArchivedTimeline.TimeRangeFilter june = new HoodieArchivedTimeline.TimeRangeFilter("20240601", "20240630");
    assertTrue(positiveInfinity.intersects(june));
    assertFalse(positiveInfinity.intersects(aprilClosed));
    assertTrue(positiveInfinity.intersects(negativeInfinity));
  }

  @Test
  public void testFullRangeIntersections() {
    assertTrue(fullRange.intersects(january));
    assertTrue(fullRange.intersects(february));
    assertTrue(fullRange.intersects(negativeInfinity));
    assertTrue(fullRange.intersects(positiveInfinity));
    assertTrue(fullRange.intersects(fullRange));
  }

  @Test
  public void testNegativeInfinityBoundary() {
    HoodieArchivedTimeline.TimeRangeFilter negativeInfinityOpen = new HoodieArchivedTimeline.TimeRangeFilter(
        Option.empty(), OPEN, Option.of("20240601"), OPEN);

    HoodieArchivedTimeline.TimeRangeFilter juneStart = new HoodieArchivedTimeline.TimeRangeFilter("20240601", "20240615");
    assertFalse(negativeInfinityOpen.intersects(juneStart));

    HoodieArchivedTimeline.TimeRangeFilter negativeInfinityClosed = new HoodieArchivedTimeline.TimeRangeFilter(
        Option.empty(), OPEN, Option.of("20240601"), CLOSED);

    assertTrue(negativeInfinityClosed.intersects(juneStart));
  }

  @Test
  public void testPositiveInfinityBoundary() {
    HoodieArchivedTimeline.TimeRangeFilter positiveInfinityOpen = new HoodieArchivedTimeline.TimeRangeFilter(
        Option.of("20240501"), OPEN, Option.empty(), OPEN);

    HoodieArchivedTimeline.TimeRangeFilter aprilEnd = new HoodieArchivedTimeline.TimeRangeFilter("20240425", "20240501");
    assertFalse(positiveInfinityOpen.intersects(aprilEnd));

    HoodieArchivedTimeline.TimeRangeFilter positiveInfinityClosed = new HoodieArchivedTimeline.TimeRangeFilter(
        Option.of("20240501"), CLOSED, Option.empty(), OPEN);

    assertTrue(positiveInfinityClosed.intersects(aprilEnd));
  }

  @Test
  public void testSinglePointRange() {
    HoodieArchivedTimeline.TimeRangeFilter singlePoint = new HoodieArchivedTimeline.TimeRangeFilter("20240115", "20240115");
    assertTrue(january.intersects(singlePoint));

    HoodieArchivedTimeline.TimeRangeFilter adjacentPoint = new HoodieArchivedTimeline.TimeRangeFilter("20240131", "20240131");
    assertTrue(january.intersects(adjacentPoint));

    HoodieArchivedTimeline.TimeRangeFilter outsidePoint = new HoodieArchivedTimeline.TimeRangeFilter("20240201", "20240201");
    assertFalse(january.intersects(outsidePoint));
  }

  @Test
  public void testInfiniteStartEnd() {
    HoodieArchivedTimeline.TimeRangeFilter infiniteStart = new HoodieArchivedTimeline.EndTsFilter("20240101");
    HoodieArchivedTimeline.TimeRangeFilter infiniteEnd = new HoodieArchivedTimeline.StartTsFilter("20240101");

    assertTrue(infiniteStart.intersects(infiniteEnd));
  }

  @Test
  public void testSameBoundaryDifferentTypes() {
    HoodieArchivedTimeline.TimeRangeFilter closedBoundary = new HoodieArchivedTimeline.ClosedClosedTimeRangeFilter("20240131", "20240228");
    HoodieArchivedTimeline.TimeRangeFilter openBoundary = new HoodieArchivedTimeline.OpenOpenTimeRangeFilter("20240131", "20240228");

    assertTrue(january.intersects(closedBoundary));
    assertFalse(january.intersects(openBoundary));
  }

  @Test()
  public void testNullOther() {
    assertThrows(IllegalArgumentException.class, () -> january.intersects(null));
  }

  @Test
  public void testToString() {
    assertEquals("[20240101, 20240131]", january.toString());
    assertEquals("(-∞, 20240601]", negativeInfinity.toString());
    assertEquals("[20240501, +∞)", positiveInfinity.toString());
    assertEquals("(-∞, +∞)", fullRange.toString());
    assertEquals("(20240301, 20240331)", marchOpen.toString());
  }

  @Test
  public void testNegativeInfinityWithBoundary() {
    HoodieArchivedTimeline.TimeRangeFilter atBoundary = new HoodieArchivedTimeline.TimeRangeFilter("20240601", "20240615");
    assertTrue(negativeInfinity.intersects(atBoundary));

    HoodieArchivedTimeline.TimeRangeFilter beforeBoundary = new HoodieArchivedTimeline.TimeRangeFilter("20240501", "20240531");
    assertTrue(negativeInfinity.intersects(beforeBoundary));

    HoodieArchivedTimeline.TimeRangeFilter afterBoundary = new HoodieArchivedTimeline.TimeRangeFilter("20240602", "20240615");
    assertFalse(negativeInfinity.intersects(afterBoundary));
  }

  @Test
  public void testPositiveInfinityWithBoundary() {
    HoodieArchivedTimeline.TimeRangeFilter atBoundary = new HoodieArchivedTimeline.TimeRangeFilter("20240415", "20240501");
    assertTrue(positiveInfinity.intersects(atBoundary));

    HoodieArchivedTimeline.TimeRangeFilter beforeBoundary = new HoodieArchivedTimeline.TimeRangeFilter("20240401", "20240430");
    assertFalse(positiveInfinity.intersects(beforeBoundary));

    HoodieArchivedTimeline.TimeRangeFilter afterBoundary = new HoodieArchivedTimeline.TimeRangeFilter("20240502", "20240515");
    assertTrue(positiveInfinity.intersects(afterBoundary));
  }

  @Test
  public void testPartialOverlapWithInfinity() {
    HoodieArchivedTimeline.TimeRangeFilter overlapStart = new HoodieArchivedTimeline.TimeRangeFilter("20231201", "20240115");
    assertTrue(negativeInfinity.intersects(overlapStart));
    assertTrue(january.intersects(overlapStart));

    HoodieArchivedTimeline.TimeRangeFilter overlapEnd = new HoodieArchivedTimeline.TimeRangeFilter("20240115", "20240215");
    assertTrue(negativeInfinity.intersects(overlapEnd));
    assertTrue(january.intersects(overlapEnd));
    assertTrue(february.intersects(overlapEnd));
  }

  @Test
  public void testInfiniteWithOpenInterval() {
    HoodieArchivedTimeline.TimeRangeFilter negativeOpen = new HoodieArchivedTimeline.TimeRangeFilter(Option.empty(), CLOSED, Option.of("20240601"), OPEN);

    HoodieArchivedTimeline.TimeRangeFilter boundaryPoint = new HoodieArchivedTimeline.TimeRangeFilter("20240601", "20240601");
    assertFalse(negativeOpen.intersects(boundaryPoint));

    HoodieArchivedTimeline.TimeRangeFilter beforeBoundary = new HoodieArchivedTimeline.TimeRangeFilter("20240531", "20240531");
    assertTrue(negativeOpen.intersects(beforeBoundary));
  }

  @Test
  public void testComplexScenarios() {
    HoodieArchivedTimeline.TimeRangeFilter negToMay = new HoodieArchivedTimeline.EndTsFilter("20240531");
    HoodieArchivedTimeline.TimeRangeFilter aprilToPos = new HoodieArchivedTimeline.StartTsFilter("20240401");
    assertTrue(negToMay.intersects(aprilToPos));

    HoodieArchivedTimeline.TimeRangeFilter openApril = new HoodieArchivedTimeline.ClosedOpenTimeRangeFilter("20240401", "20240430");
    HoodieArchivedTimeline.TimeRangeFilter openMay = new HoodieArchivedTimeline.OpenClosedTimeRangeFilter("20240430", "20240531");
    assertFalse(openApril.intersects(openMay));

    assertTrue(fullRange.intersects(openApril));
    assertTrue(fullRange.intersects(negToMay));
    assertTrue(fullRange.intersects(aprilToPos));
  }

}

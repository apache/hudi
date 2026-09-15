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

package org.apache.hudi.common.util;

import org.apache.hudi.common.util.TimestampLogicalTypeClassifier.Bucket;
import org.apache.hudi.common.util.TimestampLogicalTypeClassifier.DataShape;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.util.TimestampLogicalTypeClassifier.LogicalTimestampType.LOCAL_TIMESTAMP_MICROS;
import static org.apache.hudi.common.util.TimestampLogicalTypeClassifier.LogicalTimestampType.LOCAL_TIMESTAMP_MILLIS;
import static org.apache.hudi.common.util.TimestampLogicalTypeClassifier.LogicalTimestampType.NONE;
import static org.apache.hudi.common.util.TimestampLogicalTypeClassifier.LogicalTimestampType.TIMESTAMP_MICROS;
import static org.apache.hudi.common.util.TimestampLogicalTypeClassifier.LogicalTimestampType.TIMESTAMP_MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests {@link TimestampLogicalTypeClassifier}.
 */
public class TestTimestampLogicalTypeClassifier {

  // 2025-06-01 as millis (~13 digits) and micros (~16 digits).
  private static final long MILLIS_2025 = 1748736000000L;
  private static final long MICROS_2025 = 1748736000000000L;
  // The year-9999 micros sentinel seen on real tables.
  private static final long YEAR_9999_MICROS = 253402214400000000L;

  @Test
  public void testValueShape() {
    assertEquals(DataShape.MILLIS, TimestampLogicalTypeClassifier.classifyValueShape(MILLIS_2025));
    assertEquals(DataShape.MICROS, TimestampLogicalTypeClassifier.classifyValueShape(MICROS_2025));
    // Zero, negative, near-epoch, and sentinels are not judgeable.
    assertEquals(DataShape.UNKNOWN, TimestampLogicalTypeClassifier.classifyValueShape(0L));
    assertEquals(DataShape.UNKNOWN, TimestampLogicalTypeClassifier.classifyValueShape(-1L));
    assertEquals(DataShape.UNKNOWN, TimestampLogicalTypeClassifier.classifyValueShape(1000L));
    assertEquals(DataShape.UNKNOWN, TimestampLogicalTypeClassifier.classifyValueShape(YEAR_9999_MICROS));
  }

  @Test
  public void testReduceShape() {
    assertEquals(DataShape.MICROS, TimestampLogicalTypeClassifier.reduceShape(null, DataShape.MICROS));
    assertEquals(DataShape.MICROS, TimestampLogicalTypeClassifier.reduceShape(DataShape.MICROS, DataShape.MICROS));
    // UNKNOWN samples do not pollute a settled shape.
    assertEquals(DataShape.MICROS, TimestampLogicalTypeClassifier.reduceShape(DataShape.MICROS, DataShape.UNKNOWN));
    assertEquals(DataShape.MICROS, TimestampLogicalTypeClassifier.reduceShape(DataShape.UNKNOWN, DataShape.MICROS));
    // Genuinely mixed precision across files (for example a wrongly flipped table) surfaces as ambiguous.
    assertEquals(DataShape.AMBIGUOUS, TimestampLogicalTypeClassifier.reduceShape(DataShape.MICROS, DataShape.MILLIS));
  }

  @Test
  public void testBuckets() {
    // Genuinely correct micros (the Apna case): all three signals agree.
    assertEquals(Bucket.CORRECT, TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MICROS, TIMESTAMP_MICROS, DataShape.MICROS));
    // Legit millis.
    assertEquals(Bucket.CORRECT, TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MILLIS, TIMESTAMP_MILLIS, DataShape.MILLIS));
    // The 0.14.1 drift: label micros, values millis.
    assertEquals(Bucket.LEGACY_0X_BUG, TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MICROS, TIMESTAMP_MICROS, DataShape.MILLIS));
    // Symmetric inverse.
    assertEquals(Bucket.LEGACY_0X_BUG, TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MILLIS, TIMESTAMP_MILLIS, DataShape.MICROS));
    // Dropped logical type: bare long, timestamp-shaped values.
    assertEquals(Bucket.DROPPED_LOGICAL_TYPE, TimestampLogicalTypeClassifier.classifyBucket(NONE, NONE, DataShape.MICROS));
    // No logical type and no timestamp-shaped data: not a timestamp column at all.
    assertEquals(Bucket.UNAFFECTED, TimestampLogicalTypeClassifier.classifyBucket(NONE, NONE, DataShape.UNKNOWN));
    // A labeled timestamp column with an unjudgeable value shape cannot be classified.
    assertEquals(Bucket.AMBIGUOUS, TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MICROS, TIMESTAMP_MICROS, DataShape.UNKNOWN));
    assertEquals(Bucket.AMBIGUOUS, TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MICROS, TIMESTAMP_MICROS, DataShape.AMBIGUOUS));
    // Table and file disagree with no clean repair reading.
    assertEquals(Bucket.DIVERGENT, TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MICROS, TIMESTAMP_MILLIS, DataShape.MICROS));
  }

  @Test
  public void testSuggestedOverrideToken() {
    assertEquals("timestamp-millis",
        TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.LEGACY_0X_BUG, DataShape.MILLIS, false).get());
    assertEquals("timestamp-micros",
        TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.CORRECT, DataShape.MICROS, false).get());
    assertEquals("local-timestamp-micros",
        TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.DROPPED_LOGICAL_TYPE, DataShape.MICROS, true).get());
    assertEquals("local-timestamp-millis",
        TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.DROPPED_LOGICAL_TYPE, DataShape.MILLIS, true).get());
    // Millis-side symmetry: DROPPED with millis data pins to timestamp-millis / local-timestamp-millis.
    assertEquals("timestamp-millis",
        TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.DROPPED_LOGICAL_TYPE, DataShape.MILLIS, false).get());
    assertEquals("timestamp-millis",
        TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.CORRECT, DataShape.MILLIS, false).get());
    // Ambiguous / divergent / unaffected: no automatic suggestion.
    assertFalse(TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.AMBIGUOUS, DataShape.UNKNOWN, false).isPresent());
    assertFalse(TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.UNAFFECTED, DataShape.UNKNOWN, false).isPresent());
    assertFalse(TimestampLogicalTypeClassifier.suggestedOverrideToken(Bucket.DIVERGENT, DataShape.MICROS, false).isPresent());
  }

  @Test
  public void testBucketsMillisSideSymmetry() {
    // Mirror the micros cases in testBuckets() with the millis side. The symmetric coverage guards
    // against a future edit accidentally handling only one direction.
    assertEquals(Bucket.CORRECT,
        TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MILLIS, TIMESTAMP_MILLIS, DataShape.MILLIS));
    // 0.14.1 drift on the millis side: label millis, values micros.
    assertEquals(Bucket.LEGACY_0X_BUG,
        TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MILLIS, TIMESTAMP_MILLIS, DataShape.MICROS));
    // A millis-labeled column with unjudgeable value shape.
    assertEquals(Bucket.AMBIGUOUS,
        TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MILLIS, TIMESTAMP_MILLIS, DataShape.UNKNOWN));
    // Table + file disagree in the reverse direction — falls through to DIVERGENT.
    assertEquals(Bucket.DIVERGENT,
        TimestampLogicalTypeClassifier.classifyBucket(TIMESTAMP_MILLIS, TIMESTAMP_MICROS, DataShape.MILLIS));
    // Dropped local-timestamp-millis: bare long everywhere, values millis. Covers the 0.x drop of
    // local-timestamp logical types, millis side.
    assertEquals(Bucket.DROPPED_LOGICAL_TYPE,
        TimestampLogicalTypeClassifier.classifyBucket(NONE, NONE, DataShape.MILLIS));
  }

  @Test
  public void testBucketsLocalTimestampVariants() {
    // Local-timestamp variants must classify identically to their non-local counterparts —
    // the bug happens against the same three signals, only the resulting override token differs.
    assertEquals(Bucket.CORRECT,
        TimestampLogicalTypeClassifier.classifyBucket(LOCAL_TIMESTAMP_MICROS, LOCAL_TIMESTAMP_MICROS, DataShape.MICROS));
    assertEquals(Bucket.CORRECT,
        TimestampLogicalTypeClassifier.classifyBucket(LOCAL_TIMESTAMP_MILLIS, LOCAL_TIMESTAMP_MILLIS, DataShape.MILLIS));
    assertEquals(Bucket.LEGACY_0X_BUG,
        TimestampLogicalTypeClassifier.classifyBucket(LOCAL_TIMESTAMP_MICROS, LOCAL_TIMESTAMP_MICROS, DataShape.MILLIS));
    assertEquals(Bucket.LEGACY_0X_BUG,
        TimestampLogicalTypeClassifier.classifyBucket(LOCAL_TIMESTAMP_MILLIS, LOCAL_TIMESTAMP_MILLIS, DataShape.MICROS));
  }
}

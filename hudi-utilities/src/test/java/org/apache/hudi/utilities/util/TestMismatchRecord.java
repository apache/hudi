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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMismatchRecord {

  /**
   * The shuffle sorts values, so the ordering has to be total and deterministic: two runs over an
   * unchanged table must repartition identically.
   */
  @Test
  void compareToOrdersByPartitionThenFileName() {
    MismatchRecord first = suspect("2024/01/01", "fg1_0-1-0_1005.parquet");
    MismatchRecord second = suspect("2024/01/02", "fg1_0-1-0_1005.parquet");
    MismatchRecord samePartitionLaterName = suspect("2024/01/01", "fg2_0-1-0_1005.parquet");

    assertTrue(first.compareTo(second) < 0);
    assertTrue(second.compareTo(first) > 0);
    assertEquals(0, first.compareTo(first));
    assertTrue(first.compareTo(samePartitionLaterName) < 0);
    assertTrue(samePartitionLaterName.compareTo(first) > 0);
  }

  /**
   * A null partition or file name must not throw. This comparator runs inside a Spark shuffle, where
   * a NullPointerException kills the stage rather than failing one record.
   */
  @Test
  void compareToIsNullSafeOnBothFields() {
    MismatchRecord noPartition = suspect(null, "fg1_0-1-0_1005.parquet");
    MismatchRecord noFileName = suspect("2024/01/01", null);
    MismatchRecord complete = suspect("2024/01/01", "fg1_0-1-0_1005.parquet");

    assertEquals(0, noPartition.compareTo(noPartition));
    assertEquals(0, noFileName.compareTo(noFileName));
    // Whatever order is chosen, it must be consistent in both directions and must not throw.
    assertEquals(-Integer.signum(complete.compareTo(noPartition)),
        Integer.signum(noPartition.compareTo(complete)));
    assertEquals(-Integer.signum(complete.compareTo(noFileName)),
        Integer.signum(noFileName.compareTo(complete)));
  }

  /** Sorting a list must be stable and repeatable — this is what the shuffle relies on. */
  @Test
  void sortingIsRepeatable() {
    List<MismatchRecord> records = new ArrayList<>(Arrays.asList(
        suspect("2024/01/02", "b.parquet"),
        suspect("2024/01/01", "b.parquet"),
        suspect("2024/01/01", "a.parquet")));
    List<MismatchRecord> other = new ArrayList<>(records);
    Collections.shuffle(other, new java.util.Random(42));

    Collections.sort(records);
    Collections.sort(other);

    for (int i = 0; i < records.size(); i++) {
      assertEquals(records.get(i).getPartition(), other.get(i).getPartition());
      assertEquals(records.get(i).getFileName(), other.get(i).getFileName());
    }
  }

  private static MismatchRecord suspect(String partition, String fileName) {
    return MismatchRecord.newBuilder()
        .withPartition(partition)
        .withPattern(MismatchPattern.DANGLING_FILE_SUSPECTS)
        .withFileGroupId("fg1")
        .withBaseInstantTime("1005")
        .withFileName(fileName)
        .withFileType(MismatchRecord.FileType.BASE)
        .withInstant("1005", MismatchRecord.InstantSource.FILE_NAME)
        .withSuspectReason(MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE)
        .withDuplicateSet(2, null)
        .withFailsValidation(true)
        .build();
  }
}

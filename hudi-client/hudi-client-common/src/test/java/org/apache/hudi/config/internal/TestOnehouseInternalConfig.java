/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.config.internal;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.config.internal.OnehouseInternalConfig.MARKER_NUM_FILE_ENTRIES_TO_PRINT;
import static org.apache.hudi.config.internal.OnehouseInternalConfig.RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_READS;
import static org.apache.hudi.config.internal.OnehouseInternalConfig.RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_WRITES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOnehouseInternalConfig {

  @Test
  void testOnehouseInternalConfigProperties() {

    // test defaults
    OnehouseInternalConfig onehouseInternalConfig = OnehouseInternalConfig.newBuilder().build();
    assertEquals(500, onehouseInternalConfig.getIntOrDefault(MARKER_NUM_FILE_ENTRIES_TO_PRINT));
    assertFalse(onehouseInternalConfig.getBooleanOrDefault(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_WRITES));
    assertFalse(onehouseInternalConfig.getBooleanOrDefault(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_READS));

    // test non default overrides
    onehouseInternalConfig = OnehouseInternalConfig.newBuilder()
        .withNumFileEntriesToPrintForMarkers(3)
        .withRecordIndexValidateAgainstFilesPartitionsOnReads(false)
        .withRecordIndexValidateAgainstFilesPartitionsOnWrites(false).build();
    assertEquals(3, onehouseInternalConfig.getInt(MARKER_NUM_FILE_ENTRIES_TO_PRINT));
    assertFalse(onehouseInternalConfig.getBoolean(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_WRITES));
    assertFalse(onehouseInternalConfig.getBoolean(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_READS));

    onehouseInternalConfig = OnehouseInternalConfig.newBuilder()
        .withNumFileEntriesToPrintForMarkers(1000).withRecordIndexValidateAgainstFilesPartitionsOnReads(true)
        .withRecordIndexValidateAgainstFilesPartitionsOnWrites(true).build();
    assertEquals(1000, onehouseInternalConfig.getInt(MARKER_NUM_FILE_ENTRIES_TO_PRINT));
    assertTrue(onehouseInternalConfig.getBoolean(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_WRITES));
    assertTrue(onehouseInternalConfig.getBoolean(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_READS));
  }
}

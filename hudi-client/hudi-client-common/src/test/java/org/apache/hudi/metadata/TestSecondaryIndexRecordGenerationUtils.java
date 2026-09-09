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
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests validation and error propagation before secondary-index record generation.
 */
class TestSecondaryIndexRecordGenerationUtils {

  @Test
  void rejectsLogFileInsertsBeforeReadingFileSlices() {
    // Log-file inserts cannot be reconstructed without a base-file slice.
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath("p1/.fileid-1_014.log.1_1-0-1");
    writeStat.setNumInserts(1);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/table").build();

    assertThrows(HoodieIOException.class,
        () -> SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
            Collections.singletonList(writeStat), "001", null, null, null, null, writeConfig));
  }

  @Test
  void wrapsTableSchemaResolutionFailure() {
    // Wrap schema lookup failures in the metadata utility's exception type.
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/table").build();

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil =
             mockStatic(HoodieTableMetadataUtil.class, CALLS_REAL_METHODS)) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(metaClient))
          .thenThrow(new IllegalStateException("no schema"));

      assertThrows(HoodieException.class,
          () -> SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
              Collections.emptyList(), "001", null, null, metaClient, null, writeConfig));
    }
  }
}

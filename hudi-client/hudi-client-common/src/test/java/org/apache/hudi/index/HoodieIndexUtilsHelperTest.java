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

package org.apache.hudi.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieMergedReadHandle;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.index.HoodieIndexUtilsTest.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HoodieIndexUtilsHelperTest {

  @Mock
  private HoodieWriteConfig writeConfig;

  @Mock
  private HoodieTable hoodieTable;

  @Mock
  private HoodieTableMetaClient metaClient;

  @Mock
  private HoodieActiveTimeline activeTimeline;

  @Mock
  private HoodieMergedReadHandle mergedReadHandle;

  private static final String PARTITION_PATH = "2024/01/01";
  private static final String FILE_ID = "file-123";
  private static final String INSTANT_TIME = "20240101000000";

  @BeforeEach
  void setUp() {
    // Setup meta client and timeline mocks
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterCompletedInstants()).thenReturn(activeTimeline);
    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi").withSchema(String.valueOf(SCHEMA)).build();
  }

  @Test
  void testGetExistingRecordsWithNoInstants() {
    // Setup timeline with no instants
    when(activeTimeline.lastInstant()).thenReturn(Option.empty());

    // Create test input data
    List<Pair<String, String>> locations = Collections.singletonList(
        Pair.of(PARTITION_PATH, FILE_ID)
    );
    HoodieData<Pair<String, String>> partitionLocations = HoodieListData.eager(locations);

    // Execute test
    HoodieData<HoodieRecord<Object>> result = HoodieIndexUtilsHelper.getExistingRecords(
        partitionLocations, writeConfig, hoodieTable
    );

    // Verify results
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetExistingRecordsWithCompletedInstant() {
    // Setup timeline with a completed instant
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", INSTANT_TIME);
    when(activeTimeline.lastInstant()).thenReturn(Option.of(instant));

    // Mock MergedReadHandle behavior
    List<HoodieRecord> mockRecords = Arrays.asList(
        mock(HoodieRecord.class),
        mock(HoodieRecord.class)
    );

    try (MockedConstruction<HoodieMergedReadHandle> mockedConstruction =
             mockConstruction(HoodieMergedReadHandle.class,
                 (mock, context) -> when(mock.getMergedRecords()).thenReturn(mockRecords))) {

      // Create test input data
      List<Pair<String, String>> locations = Collections.singletonList(
          Pair.of(PARTITION_PATH, FILE_ID)
      );
      HoodieData<Pair<String, String>> partitionLocations = HoodieListData.eager(locations);

      // Execute test
      HoodieData<HoodieRecord<Object>> result = HoodieIndexUtilsHelper.getExistingRecords(
          partitionLocations, writeConfig, hoodieTable
      );

      // Verify results
      assertEquals(2, result.count());
      List<HoodieRecord<Object>> resultList = result.collectAsList();
      assertEquals(mockRecords, resultList);
    }
  }
}
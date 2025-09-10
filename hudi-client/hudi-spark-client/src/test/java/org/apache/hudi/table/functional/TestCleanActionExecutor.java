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

package org.apache.hudi.table.functional;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.action.clean.CleanPlanner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_PARSER;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.convertMetadataToByteArray;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests Clean action executor.
 */
public class TestCleanActionExecutor {

  private static final StorageConfiguration<?> CONF = getDefaultStorageConf();
  private final HoodieEngineContext context = new HoodieLocalEngineContext(CONF);
  private final HoodieTable<?, ?, ?, ?> mockHoodieTable = mock(HoodieTable.class);
  private HoodieTableMetaClient metaClient;
  private HoodieStorage storage;

  private static String PARTITION1 = "partition1";

  String earliestInstant = "20231204194919610";
  String earliestInstantMinusThreeDays = "20231201194919610";

  @BeforeEach
  void setUp() {
    metaClient = mock(HoodieTableMetaClient.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    storage = spy(HoodieStorageUtils.getStorage(CONF));
    when(metaClient.getStorage()).thenReturn(storage);
    when(mockHoodieTable.getStorage()).thenReturn(storage);
  }

  @ParameterizedTest
  @EnumSource(CleanFailureType.class)
  void testPartialCleanFailure(CleanFailureType failureType) throws IOException {
    HoodieWriteConfig config = getCleanByCommitsConfig();
    String fileGroup = UUID.randomUUID() + "-0";
    HoodieBaseFile baseFile = new HoodieBaseFile(String.format("/tmp/base/%s_1-0-1_%s.parquet", fileGroup, "001"));
    HoodieStorage localStorage = HoodieStorageUtils.getStorage(baseFile.getPath(), CONF);
    StoragePath filePath = new StoragePath(baseFile.getPath());

    if (failureType == CleanFailureType.TRUE_ON_DELETE) {
      when(storage.deleteFile(filePath)).thenReturn(true);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_FALSE) {
      when(storage.deleteFile(filePath)).thenReturn(false);
      when(storage.exists(filePath)).thenReturn(false);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_TRUE) {
      when(storage.deleteFile(filePath)).thenReturn(false);
      when(storage.exists(filePath)).thenReturn(true);
    } else if (failureType == CleanFailureType.FILE_NOT_FOUND_EXC_ON_DELETE) {
      when(storage.deleteFile(filePath)).thenThrow(new FileNotFoundException("throwing file not found exception"));
    } else {
      // run time exception
      when(storage.deleteFile(filePath)).thenThrow(new RuntimeException("throwing run time exception"));
    }
    // we have to create the actual file after setting up mock logic for storage
    // otherwise the file created would be deleted when setting up because storage is a spy
    localStorage.create(filePath);

    Map<String, List<HoodieCleanFileInfo>> partitionCleanFileInfoMap = new HashMap<>();
    List<HoodieCleanFileInfo> cleanFileInfos = Collections.singletonList(new HoodieCleanFileInfo(baseFile.getPath(), false));
    partitionCleanFileInfoMap.put(PARTITION1, cleanFileInfos);
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant(earliestInstant, HoodieTimeline.COMMIT_ACTION, HoodieInstant.State.COMPLETED.name()), earliestInstantMinusThreeDays,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name(), Collections.emptyMap(), CleanPlanner.LATEST_CLEAN_PLAN_VERSION, partitionCleanFileInfoMap, Collections.emptyList(), Collections.emptyMap());

    // add clean to the timeline.
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(mockHoodieTable.getActiveTimeline()).thenReturn(activeTimeline);
    HoodieInstant cleanInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "002");
    HoodieActiveTimeline cleanTimeline = mock(HoodieActiveTimeline.class);
    when(activeTimeline.getCleanerTimeline()).thenReturn(cleanTimeline);
    when(cleanTimeline.getInstants()).thenReturn(Collections.singletonList(cleanInstant));
    when(activeTimeline.readCleanerPlan(cleanInstant)).thenReturn(cleanerPlan);
    when(activeTimeline.readCleanerInfoAsBytes(cleanInstant)).thenReturn(Option.of(convertMetadataToByteArray(cleanerPlan)));

    when(mockHoodieTable.getCleanTimeline()).thenReturn(cleanTimeline);
    when(mockHoodieTable.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(mockHoodieTable.getInstantFileNameGenerator()).thenReturn(INSTANT_FILE_NAME_GENERATOR);
    when(mockHoodieTable.getInstantFileNameParser()).thenReturn(INSTANT_FILE_NAME_PARSER);
    HoodieTimeline inflightsAndRequestedTimeline = mock(HoodieTimeline.class);
    when(cleanTimeline.filterInflightsAndRequested()).thenReturn(inflightsAndRequestedTimeline);
    when(inflightsAndRequestedTimeline.getInstants()).thenReturn(Collections.singletonList(cleanInstant));
    when(activeTimeline.transitionCleanRequestedToInflight(any())).thenReturn(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "002"));
    when(mockHoodieTable.getMetadataWriter("002")).thenReturn(Option.empty());

    TransactionManager mockTransactionManager = mock(TransactionManager.class);
    when(mockHoodieTable.getTxnManager()).thenReturn(Option.of(mockTransactionManager));

    CleanActionExecutor cleanActionExecutor = new CleanActionExecutor(context, config, mockHoodieTable, "002");
    if (failureType == CleanFailureType.TRUE_ON_DELETE) {
      assertCleanExecutionSuccess(cleanActionExecutor, filePath);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_FALSE) {
      assertCleanExecutionSuccess(cleanActionExecutor, filePath);
    } else if (failureType == CleanFailureType.FALSE_ON_DELETE_IS_EXISTS_TRUE) {
      assertCleanExecutionFailure(cleanActionExecutor);
    } else if (failureType == CleanFailureType.FILE_NOT_FOUND_EXC_ON_DELETE) {
      assertCleanExecutionSuccess(cleanActionExecutor, filePath);
    } else {
      // run time exception
      assertCleanExecutionFailure(cleanActionExecutor);
    }
  }

  private void assertCleanExecutionFailure(CleanActionExecutor cleanActionExecutor) {
    assertThrows(HoodieException.class, cleanActionExecutor::execute);
  }

  private void assertCleanExecutionSuccess(CleanActionExecutor cleanActionExecutor, StoragePath filePath) {
    HoodieCleanMetadata cleanMetadata = cleanActionExecutor.execute();
    assertTrue(cleanMetadata.getPartitionMetadata().containsKey(PARTITION1));
    HoodieCleanPartitionMetadata cleanPartitionMetadata = cleanMetadata.getPartitionMetadata().get(PARTITION1);
    assertTrue(cleanPartitionMetadata.getDeletePathPatterns().contains(filePath.getName()));
  }

  private static HoodieWriteConfig getCleanByCommitsConfig() {
    return HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
  }

  enum CleanFailureType {
    TRUE_ON_DELETE,
    FALSE_ON_DELETE_IS_EXISTS_FALSE,
    FALSE_ON_DELETE_IS_EXISTS_TRUE,
    FILE_NOT_FOUND_EXC_ON_DELETE,
    RUNTIME_EXC_ON_DELETE
  }
}

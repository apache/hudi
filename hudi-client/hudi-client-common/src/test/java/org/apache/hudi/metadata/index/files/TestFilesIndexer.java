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
 */

package org.apache.hudi.metadata.index.files;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.util.Lazy;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;

class TestFilesIndexer {

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeDataEmptyInput() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieData<HoodieRecord> allPartitions = mock(HoodieData.class);

    when(engineContext.parallelize(any(List.class), org.mockito.ArgumentMatchers.eq(1))).thenReturn(allPartitions);

    ExposedFilesIndexer indexer = new ExposedFilesIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionInitialization> initializationList = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
    assertEquals(1, initializationList.size());

    assertEquals(MetadataPartitionType.FILES.getPartitionPath(), initializationList.get(0).indexPartitionName());
    assertSame(allPartitions, initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords());
  }

  @Test
  void testInitializeDataWithRealEngineContext() throws IOException {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    Map<String, List<FileInfo>> files = new HashMap<>();
    files.put("p1", Collections.singletonList(FileInfo.of("f1.parquet", 100L)));
    files.put("p2", Collections.singletonList(FileInfo.of("f2.parquet", 200L)));

    ExposedFilesIndexer indexer = new ExposedFilesIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionInitialization> initializationList = indexer.callGetData("001", "002", files, Lazy.lazily(Collections::emptyList));
    assertEquals(1, initializationList.size());

    assertEquals(MetadataPartitionType.FILES.getPartitionPath(), initializationList.get(0).indexPartitionName());
    // 1 partition-list record + 2 partition-file records
    HoodieData<HoodieRecord> indexRecords = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords();
    assertEquals(3L, indexRecords.count());

    List<HoodieRecord> records = indexRecords.collectAsList();
    Set<String> keys = records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
    assertEquals(new HashSet<>(Arrays.asList(HoodieTableMetadata.RECORDKEY_PARTITION_LIST, "p1", "p2")), keys);

    HoodieMetadataPayload partitionListPayload = (HoodieMetadataPayload) records.stream()
        .filter(r -> HoodieTableMetadata.RECORDKEY_PARTITION_LIST.equals(r.getRecordKey()))
        .findFirst().orElseThrow(() -> new AssertionError("partition-list record not found"))
        .getData();
    assertEquals(new HashSet<>(Arrays.asList("p1", "p2")), new HashSet<>(partitionListPayload.getFilenames()));

    Map<String, HoodieMetadataPayload> filesPayloadByPartition = records.stream()
        .filter(r -> !HoodieTableMetadata.RECORDKEY_PARTITION_LIST.equals(r.getRecordKey()))
        .collect(Collectors.toMap(HoodieRecord::getRecordKey, r -> (HoodieMetadataPayload) r.getData()));
    assertEquals(Collections.singletonList("f1.parquet"), filesPayloadByPartition.get("p1").getFilenames());
    assertEquals(Collections.singletonList("f2.parquet"), filesPayloadByPartition.get("p2").getFilenames());
  }

  private static class ExposedFilesIndexer extends FilesIndexer {
    ExposedFilesIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig, HoodieTableMetaClient dataTableMetaClient) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient);
    }

    List<IndexPartitionInitialization> callGetData(String dataTableInstantTime, String instantTimeForPartition,
                                                   Map<String, List<FileInfo>> partitionIdToAllFilesMap,
                                                   Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
      return buildInitialization(dataTableInstantTime, instantTimeForPartition, partitionIdToAllFilesMap, lazyLatestMergedPartitionFileSliceList);
    }
  }
}

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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexRestoreContext;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileInfo;

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

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestFilesIndexer {

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeDataEmptyInput() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieData<HoodieRecord> allPartitions = mock(HoodieData.class);

    when(engineContext.parallelize(any(List.class), org.mockito.ArgumentMatchers.eq(1))).thenReturn(allPartitions);

    FilesIndexer indexer = new FilesIndexer(engineContext, writeConfig, metaClient);
    List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
        "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
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

    FilesIndexer indexer = new FilesIndexer(engineContext, writeConfig, metaClient);
    List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
        "001", "002", files, Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
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

  @Test
  void testBuildUpdateGeneratesPartitionListAndFileRecords() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath("p1/f1.parquet");
    writeStat.setFileSizeInBytes(123L);
    commitMetadata.addWriteStat("p1", writeStat);

    FilesIndexer indexer = new FilesIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
        "003",
        mock(HoodieBackedTableMetadata.class),
        Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
        commitMetadata));

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.FILES.getPartitionPath(), result.get(0).indexPartitionName());

    List<HoodieRecord> records = result.get(0).indexRecords().collectAsList();
    assertEquals(2, records.size());
    Set<String> keys = records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
    assertEquals(new HashSet<>(Arrays.asList(HoodieTableMetadata.RECORDKEY_PARTITION_LIST, "p1")), keys);

    Map<String, HoodieMetadataPayload> payloadByKey = records.stream()
        .collect(Collectors.toMap(HoodieRecord::getRecordKey, r -> (HoodieMetadataPayload) r.getData()));
    assertEquals(Collections.singletonList("p1"), payloadByKey.get(HoodieTableMetadata.RECORDKEY_PARTITION_LIST).getFilenames());
    assertEquals(Collections.singletonList("f1.parquet"), payloadByKey.get("p1").getFilenames());
  }

  @Test
  void testBuildCleanGeneratesDeleteRecordsForFilesAndPartitions() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    Map<String, HoodieCleanPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put("p1", new HoodieCleanPartitionMetadata(
        "p1",
        "KEEP_LATEST_COMMITS",
        Collections.singletonList("f1.parquet"),
        Collections.emptyList(),
        Collections.emptyList(),
        false));
    partitionMetadata.put("p2", new HoodieCleanPartitionMetadata(
        "p2",
        "KEEP_LATEST_COMMITS",
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        true));
    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata(
        "004",
        100L,
        1,
        "003",
        "003",
        partitionMetadata,
        2,
        Collections.emptyMap(),
        Collections.emptyMap());

    FilesIndexer indexer = new FilesIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result = indexer.buildClean(IndexCleanContext.of("005", cleanMetadata));

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.FILES.getPartitionPath(), result.get(0).indexPartitionName());

    List<HoodieRecord> records = result.get(0).indexRecords().collectAsList();
    assertEquals(3, records.size());
    Map<String, HoodieMetadataPayload> payloadByKey = records.stream()
        .collect(Collectors.toMap(HoodieRecord::getRecordKey, r -> (HoodieMetadataPayload) r.getData()));
    assertTrue(payloadByKey.containsKey("p1"));
    assertTrue(payloadByKey.containsKey("p2"));
    assertTrue(payloadByKey.containsKey(HoodieTableMetadata.RECORDKEY_PARTITION_LIST));
    assertEquals(Collections.singletonList("f1.parquet"), payloadByKey.get("p1").getDeletions());
    assertEquals(Collections.singletonList("p2"), payloadByKey.get(HoodieTableMetadata.RECORDKEY_PARTITION_LIST).getDeletions());
  }

  @Test
  void testBuildRestoreGeneratesRecordsForAddedDeletedFilesAndDeletedPartitions() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    Map<String, List<FileInfo>> filesAdded = new HashMap<>();
    filesAdded.put("p1", Collections.singletonList(FileInfo.of("f2.parquet", 456L)));

    Map<String, List<String>> filesDeleted = new HashMap<>();
    filesDeleted.put("p2", Collections.singletonList("f3.parquet"));

    FilesIndexer indexer = new FilesIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result =
        indexer.buildRestore(IndexRestoreContext.of("006", Collections.singletonList("p3"), filesAdded, filesDeleted));

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.FILES.getPartitionPath(), result.get(0).indexPartitionName());

    List<HoodieRecord> records = result.get(0).indexRecords().collectAsList();
    assertEquals(3, records.size());
    Map<String, HoodieMetadataPayload> payloadByKey = records.stream()
        .collect(Collectors.toMap(HoodieRecord::getRecordKey, r -> (HoodieMetadataPayload) r.getData()));
    assertEquals(Collections.singletonList("f2.parquet"), payloadByKey.get("p1").getFilenames());
    assertEquals(Collections.singletonList("f3.parquet"), payloadByKey.get("p2").getDeletions());
    assertEquals(Collections.singletonList("p3"), payloadByKey.get(HoodieTableMetadata.RECORDKEY_PARTITION_LIST).getDeletions());
  }
}

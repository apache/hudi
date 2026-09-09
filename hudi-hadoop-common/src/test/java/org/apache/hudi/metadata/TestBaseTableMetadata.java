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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestBaseTableMetadata {

  @TempDir
  Path tempDir;

  private HoodieTableMetaClient metaClient;
  private String basePath;

  @BeforeEach
  void setUp() throws Exception {
    basePath = tempDir.toString();
    metaClient = HoodieTestUtils.init(basePath);
  }

  @Test
  void testReadFailuresAreWrappedWithMetadataContext() {
    TestingTableMetadata metadata = newMetadata();
    metadata.failSingleReads = true;
    assertThrows(HoodieMetadataException.class, metadata::getAllPartitionPaths);

    metadata.failSingleReads = false;
    metadata.failBulkReads = true;
    assertThrows(HoodieMetadataException.class,
        () -> metadata.getAllFilesInPartitions(Collections.singletonList(basePath)));
  }

  @Test
  void testDisabledIndexesAndEmptyBloomFilterLookup() {
    TestingTableMetadata metadata = newMetadata();
    assertFalse(metadata.getBloomFilter("", "file.parquet", MetadataPartitionType.BLOOM_FILTERS.getPartitionPath()).isPresent());
    assertTrue(metadata.getBloomFilters(
        Collections.singletonList(Pair.of("", "file.parquet")),
        MetadataPartitionType.BLOOM_FILTERS.getPartitionPath()).isEmpty());
    assertTrue(metadata.getColumnStats(
        Collections.singletonList(Pair.of("", "file.parquet")),
        Collections.singletonList("column")).isEmpty());

    metaClient.getTableConfig().setMetadataPartitionState(
        metaClient, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), true);
    metadata = newMetadata();
    assertFalse(metadata.getBloomFilter(
        "", "file.parquet", MetadataPartitionType.BLOOM_FILTERS.getPartitionPath()).isPresent());
    assertTrue(metadata.getBloomFilters(
        Collections.emptyList(), MetadataPartitionType.BLOOM_FILTERS.getPartitionPath()).isEmpty());
  }

  @Test
  void testPayloadFailuresAndMissingColumnStats() {
    HoodieMetadataPayload payload = new HoodieMetadataPayload(
        "bad", MetadataPartitionType.FILES.getRecordType(), Collections.emptyMap()) {
      @Override
      public List<StoragePathInfo> getFileList(
          HoodieStorage storage, StoragePath partitionPath) {
        throw new HoodieException("corrupt payload");
      }
    };

    TestingTableMetadata firstMetadata = newMetadata();
    firstMetadata.singleRecord = Option.of(payload);
    assertThrows(HoodieException.class,
        () -> firstMetadata.getAllFilesInPartition(new StoragePath(basePath)));

    HoodieMetadataPayload inconsistentPayload =
        HoodieMetadataPayload.createPartitionListRecord(
            Collections.singletonList("ghost"), true).getData();
    firstMetadata.singleRecord = Option.of(inconsistentPayload);
    assertThrows(HoodieMetadataException.class, firstMetadata::getAllPartitionPaths);

    metaClient.getTableConfig().setMetadataPartitionState(
        metaClient, MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true);
    TestingTableMetadata metadata = newMetadata();
    HoodieMetadataPayload missingColumnStats =
        HoodieMetadataPayload.createPartitionFilesRecord(
            "", Collections.singletonMap("file.parquet", 1L),
            Collections.emptyList()).getData();
    metadata.pairRecords = HoodieListPairData.eager(
        Collections.singletonList(Pair.of("missing-key", missingColumnStats)));
    assertTrue(metadata.getColumnStats(
        Collections.singletonList(Pair.of("", "file.parquet")),
        Collections.singletonList("column")).isEmpty());
  }

  @Test
  void testProtectedReadContextAccessors() {
    TestingTableMetadata metadata = newMetadata();
    assertNotNull(metadata.storageConfiguration());
    assertEquals("00000000000000", metadata.latestDataInstant());
  }

  @Test
  void testHoodieBackedMetadataStaysDisabledWithoutMetadataTable() {
    HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        null,
        metaClient.getStorage(),
        HoodieMetadataConfig.newBuilder().enable(false).build(),
        basePath);

    assertFalse(metadata.isMetadataTableInitialized());
    assertFalse(metadata.getSyncedInstantTime().isPresent());
    assertFalse(metadata.getLatestCompactionTime().isPresent());
    metadata.close();
  }

  private TestingTableMetadata newMetadata() {
    return new TestingTableMetadata(
        null, metaClient.getStorage(),
        HoodieMetadataConfig.newBuilder()
            .enable(true)
            .ignoreSpuriousDeletes(false)
            .build(),
        basePath);
  }

  private static class TestingTableMetadata extends BaseTableMetadata {
    private boolean failSingleReads;
    private boolean failBulkReads;
    private Option<HoodieMetadataPayload> singleRecord = Option.empty();
    private HoodiePairData<String, HoodieMetadataPayload> pairRecords =
        HoodieListPairData.eager(Collections.emptyList());

    TestingTableMetadata(HoodieEngineContext engineContext,
                         HoodieStorage storage,
                         HoodieMetadataConfig metadataConfig,
                         String dataBasePath) {
      super(engineContext, storage, metadataConfig, dataBasePath);
      isMetadataTableInitialized = true;
    }

    @Override
    protected Option<HoodieMetadataPayload> readFilesIndexRecords(String key, String partitionName) {
      if (failSingleReads) {
        throw new HoodieException("single read failed");
      }
      return singleRecord;
    }

    @Override
    public List<String> getPartitionPathWithPathPrefixUsingFilterExpression(
        List<String> relativePathPrefixes,
        Types.RecordType partitionFields,
        Expression expression) {
      return Collections.emptyList();
    }

    @Override
    public List<String> getPartitionPathWithPathPrefixes(List<String> relativePathPrefixes) {
      return Collections.emptyList();
    }

    @Override
    public HoodiePairData<String, HoodieMetadataPayload> readIndexRecordsWithKeys(
        HoodieData<? extends RawKey> rawKeys, String partitionName) {
      if (failBulkReads) {
        throw new HoodieException("bulk read failed");
      }
      return pairRecords;
    }

    @Override
    protected HoodiePairData<String, HoodieMetadataPayload> readIndexRecordsWithKeys(
        HoodieData<? extends RawKey> rawKeys,
        String partitionName,
        Option<String> dataTablePartition) {
      return readIndexRecordsWithKeys(rawKeys, partitionName);
    }

    @Override
    public HoodiePairData<String, String> readSecondaryIndexDataTableRecordKeysWithKeys(
        HoodieData<String> keys, String partitionName) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    @Override
    public HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexLocationsWithKeys(
        HoodieData<String> secondaryKeys, String partitionName) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    @Override
    public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(
        HoodieData<String> recordKeys) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    @Override
    public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(
        HoodieData<String> recordKeys, Option<String> dataTablePartition) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    @Override
    public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(
        HoodieData<? extends RawKey> rawKeys,
        String partitionName,
        boolean shouldLoadInMemory) {
      return HoodieListData.eager(Collections.emptyList());
    }

    @Override
    public Map<Pair<String, StoragePath>, List<StoragePathInfo>> listPartitions(
        List<Pair<String, StoragePath>> partitionPathList) {
      return Collections.emptyMap();
    }

    @Override
    public Option<String> getSyncedInstantTime() {
      return Option.empty();
    }

    @Override
    public Option<String> getLatestCompactionTime() {
      return Option.empty();
    }

    @Override
    public void reset() {
    }

    @Override
    public void close() {
    }

    @Override
    public int getNumFileGroupsForPartition(MetadataPartitionType partition) {
      return 0;
    }

    @Override
    public Map<String, List<FileSlice>> getBucketizedFileGroupsForPartitionedRLI(
        MetadataPartitionType partition) {
      return Collections.emptyMap();
    }

    StorageConfiguration<?> storageConfiguration() {
      return getStorageConf();
    }

    String latestDataInstant() {
      return getLatestDataInstantTime();
    }
  }
}

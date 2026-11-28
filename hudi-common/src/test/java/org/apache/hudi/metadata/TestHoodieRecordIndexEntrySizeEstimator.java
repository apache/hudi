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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieRecordIndexEntrySizeEstimator.KeyColumnSizes;
import org.apache.hudi.metadata.HoodieRecordIndexEntrySizeEstimator.KeyStats;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.metadata.stats.ValueMetadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_FILEID_ENCODING_UUID;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestHoodieRecordIndexEntrySizeEstimator {

  private static final String INSTANT = "20261119100000000";

  @Test
  void keyStatsSumTheFootersAndDropTheLengthPrefix() {
    // 1100 keys of 36 bytes, each stored after a 4-byte length: 44000 uncompressed bytes
    Option<KeyStats> stats = KeyStats.of(Arrays.asList(new KeyColumnSizes(40_000, 20_680, 1_000), new KeyColumnSizes(4_000, 2_000, 100)));
    assertTrue(stats.isPresent());
    assertEquals(36.0, stats.get().getKeyBytes(), 1e-9);
    assertEquals(22_680.0 / 44_000, stats.get().getCompressionRatio(), 1e-9);
  }

  @Test
  void keyStatsAreEmptyWithoutKeys() {
    assertFalse(KeyStats.of(Collections.emptyList()).isPresent());
    assertFalse(KeyStats.of(Collections.singletonList(KeyColumnSizes.NONE)).isPresent());
  }

  @Test
  void entrySizeAddsTheValueToTheScaledCompressedKey() {
    // 3.3 + 1.3 * 0.517 * 36 = 27.5
    assertEquals(28, HoodieRecordIndexEntrySizeEstimator.entrySize(3.3, new KeyStats(36, 0.517), 1.3));
    assertEquals(1, HoodieRecordIndexEntrySizeEstimator.entrySize(0, new KeyStats(0.1, 0.01), 1.3));
  }

  @Test
  void dictionaryEncodedKeyColumnIsNotTakenForTheKeySize() {
    String key = "8f14e45f-ceea-467a-9a36-4f8b2b1c0a11";
    // plain: 1000 keys of 36 bytes, each after a 4-byte length
    assertTrue(HoodieRecordIndexEntrySizeEstimator.isPlainEncodedSize(keyColumnStats(key, 1_000, 40_000)));
    // dictionary: 1000 values of one repeated key, a few bytes each
    assertFalse(HoodieRecordIndexEntrySizeEstimator.isPlainEncodedSize(keyColumnStats(key, 1_000, 6_000)));
  }

  @Test
  void valueBytesGrowWithTheFileGroupsAndPartitionsNeighbouringEntriesPointTo() {
    double fewFileGroups = HoodieRecordIndexEntrySizeEstimator.evaluateValueBytes(fileSlices(1, 4), RECORD_INDEX_FIELD_FILEID_ENCODING_UUID);
    double manyFileGroups = HoodieRecordIndexEntrySizeEstimator.evaluateValueBytes(fileSlices(1_000, 4), RECORD_INDEX_FIELD_FILEID_ENCODING_UUID);
    // measured against the record index HFiles of 1M rows: 3.3 bytes with 4 file groups in 1 partition, and 9.2 bytes
    // with 4000 file groups in 1000 partitions
    assertTrue(fewFileGroups > 2 && fewFileGroups < 5, "few file groups: " + fewFileGroups);
    assertTrue(manyFileGroups > 7.5 && manyFileGroups < 11, "many file groups: " + manyFileGroups);
    assertEquals(fewFileGroups, HoodieRecordIndexEntrySizeEstimator.evaluateValueBytes(fileSlices(1, 4), RECORD_INDEX_FIELD_FILEID_ENCODING_UUID));
  }

  @Test
  void valueBytesAreZeroWithoutFileSlices() {
    assertEquals(0, HoodieRecordIndexEntrySizeEstimator.evaluateValueBytes(Collections.emptyList(), RECORD_INDEX_FIELD_FILEID_ENCODING_UUID));
  }

  @Test
  void tableWithoutMetaFieldsFallsBackToTheFixedEntrySize() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    assertEquals(RECORD_INDEX_AVERAGE_RECORD_SIZE, HoodieRecordIndexEntrySizeEstimator.estimateEntrySize(
        engineContext, metaClient(false), fileSlices(1, 4), RECORD_INDEX_FIELD_FILEID_ENCODING_UUID, 1.3));
    verifyNoInteractions(engineContext);
  }

  @Test
  void tableWithoutParquetBaseFilesFallsBackToTheFixedEntrySize() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    List<FileSliceAndPartition> logOnly = Collections.singletonList(
        FileSliceAndPartition.of("p1", new FileSlice("p1", INSTANT, UUID.randomUUID() + "-0")));
    assertEquals(RECORD_INDEX_AVERAGE_RECORD_SIZE, HoodieRecordIndexEntrySizeEstimator.estimateEntrySize(
        engineContext, metaClient(true), logOnly, RECORD_INDEX_FIELD_FILEID_ENCODING_UUID, 1.3));
    verifyNoInteractions(engineContext);
  }

  private static HoodieColumnRangeMetadata<Comparable> keyColumnStats(String key, long valueCount, long uncompressedSize) {
    Comparable bound = key;
    return HoodieColumnRangeMetadata.create("f.parquet", HoodieRecord.RECORD_KEY_METADATA_FIELD, bound, bound, 0, valueCount,
        uncompressedSize / 2, uncompressedSize, ValueMetadata.V1EmptyMetadata.get());
  }

  /**
   * Base file slices spread over partitions whose paths differ only in their last digits, as many as given per partition.
   */
  private static List<FileSliceAndPartition> fileSlices(int partitions, int fileGroupsPerPartition) {
    List<FileSliceAndPartition> fileSlices = new ArrayList<>();
    for (int p = 0; p < partitions; p++) {
      String partition = String.format("2026-11-19-b%04d", p);
      for (int f = 0; f < fileGroupsPerPartition; f++) {
        String fileId = new UUID(p, f) + "-0";
        FileSlice fileSlice = new FileSlice(partition, INSTANT, fileId);
        fileSlice.setBaseFile(new HoodieBaseFile("/t/" + partition + "/" + fileId + "_0-1-1_" + INSTANT + ".parquet"));
        fileSlices.add(FileSliceAndPartition.of(partition, fileSlice));
      }
    }
    return fileSlices;
  }

  private static HoodieTableMetaClient metaClient(boolean populateMetaFields) {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.populateMetaFields()).thenReturn(populateMetaFields);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    return metaClient;
  }
}

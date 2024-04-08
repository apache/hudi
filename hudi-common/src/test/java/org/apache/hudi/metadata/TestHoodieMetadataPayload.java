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

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.CollectionUtils.createImmutableMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieMetadataPayload}.
 */
public class TestHoodieMetadataPayload extends HoodieCommonTestHarness {
  public static final String PARTITION_NAME = "2022/10/01";

  @Test
  public void testFileSystemMetadataPayloadMerging() {
    Map<String, Long> firstCommitAddedFiles = createImmutableMap(
        Pair.of("file1.parquet", 1000L),
        Pair.of("file2.parquet", 2000L),
        Pair.of("file3.parquet", 3000L)
    );

    HoodieRecord<HoodieMetadataPayload> firstPartitionFilesRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, firstCommitAddedFiles, Collections.emptyList());

    Map<String, Long> secondCommitAddedFiles = createImmutableMap(
        // NOTE: This is an append
        Pair.of("file3.parquet", 3333L),
        Pair.of("file4.parquet", 4000L),
        Pair.of("file5.parquet", 5000L)
    );

    List<String> secondCommitDeletedFiles = Collections.singletonList("file1.parquet");

    HoodieRecord<HoodieMetadataPayload> secondPartitionFilesRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, secondCommitAddedFiles, secondCommitDeletedFiles);

    HoodieMetadataPayload combinedPartitionFilesRecordPayload =
        secondPartitionFilesRecord.getData().preCombine(firstPartitionFilesRecord.getData());

    HoodieMetadataPayload expectedCombinedPartitionedFilesRecordPayload =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L),
                Pair.of("file3.parquet", 3333L),
                Pair.of("file4.parquet", 4000L),
                Pair.of("file5.parquet", 5000L)
            ),
            Collections.emptyList()
        ).getData();

    assertEquals(expectedCombinedPartitionedFilesRecordPayload, combinedPartitionFilesRecordPayload);
  }

  @Test
  public void testFileSystemMetadataPayloadMergingWithDeletions() {
    Map<String, Long> addedFileMap = createImmutableMap(
        Pair.of("file1.parquet", 1000L),
        Pair.of("file2.parquet", 2000L),
        Pair.of("file3.parquet", 3000L),
        Pair.of("file4.parquet", 4000L)
    );
    HoodieRecord<HoodieMetadataPayload> additionRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, addedFileMap, Collections.emptyList());

    List<String> deletedFileList1 = new ArrayList<>();
    deletedFileList1.add("file1.parquet");
    deletedFileList1.add("file3.parquet");
    HoodieRecord<HoodieMetadataPayload> deletionRecord1 =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, Collections.emptyMap(), deletedFileList1);

    List<String> deletedFileList2 = new ArrayList<>();
    deletedFileList2.add("file1.parquet");
    deletedFileList2.add("file4.parquet");
    HoodieRecord<HoodieMetadataPayload> deletionRecord2 =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, Collections.emptyMap(), deletedFileList2);

    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L),
                Pair.of("file4.parquet", 4000L)
            ),
            Collections.emptyList()
        ).getData(),
        deletionRecord1.getData().preCombine(additionRecord.getData())
    );

    List<String> expectedDeleteFileList = new ArrayList<>();
    expectedDeleteFileList.add("file1.parquet");
    expectedDeleteFileList.add("file3.parquet");
    expectedDeleteFileList.add("file4.parquet");
    
    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            Collections.emptyMap(),
            expectedDeleteFileList
        ).getData(),
        deletionRecord2.getData().preCombine(deletionRecord1.getData())
    );

    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L)
            ),
            Collections.emptyList()
        ).getData(),
        deletionRecord2.getData().preCombine(deletionRecord1.getData()).preCombine(additionRecord.getData())
    );

    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L)
            ),
            Collections.singletonList("file1.parquet")
        ).getData(),
        deletionRecord2.getData().preCombine(deletionRecord1.getData().preCombine(additionRecord.getData()))
    );
  }

  @Test
  public void testColumnStatsPayloadMerging() throws IOException {
    String fileName = "file.parquet";
    String targetColName = "c1";

    HoodieColumnRangeMetadata<Comparable> c1Metadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 100, 1000, 5, 1000, 123456, 123456);

    HoodieRecord<HoodieMetadataPayload> columnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(c1Metadata), false)
            .findFirst().get();

    ////////////////////////////////////////////////////////////////////////
    // Case 1: Combining proper (non-deleted) records
    ////////////////////////////////////////////////////////////////////////

    // NOTE: Column Stats record will only be merged in case existing file will be modified,
    //       which could only happen on storages schemes supporting appends
    HoodieColumnRangeMetadata<Comparable> c1AppendedBlockMetadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 0, 500, 0, 100, 12345, 12345);

    HoodieRecord<HoodieMetadataPayload> updatedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(c1AppendedBlockMetadata), false)
            .findFirst().get();

    HoodieMetadataPayload combinedMetadataPayload =
        columnStatsRecord.getData().preCombine(updatedColumnStatsRecord.getData());

    HoodieColumnRangeMetadata<Comparable> expectedColumnRangeMetadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 0, 1000, 5, 1100, 135801, 135801);

    HoodieRecord<HoodieMetadataPayload> expectedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(expectedColumnRangeMetadata), false)
            .findFirst().get();

    // Assert combined payload
    assertEquals(combinedMetadataPayload, expectedColumnStatsRecord.getData());

    Option<IndexedRecord> alternativelyCombinedMetadataPayloadAvro =
        columnStatsRecord.getData().combineAndGetUpdateValue(updatedColumnStatsRecord.getData().getInsertValue(null).get(), null);

    // Assert that using legacy API yields the same value
    assertEquals(combinedMetadataPayload.getInsertValue(null), alternativelyCombinedMetadataPayloadAvro);

    ////////////////////////////////////////////////////////////////////////
    // Case 2: Combining w/ deleted records
    ////////////////////////////////////////////////////////////////////////

    HoodieColumnRangeMetadata<Comparable> c1StubbedMetadata =
        HoodieColumnRangeMetadata.<Comparable>stub(fileName, targetColName);

    HoodieRecord<HoodieMetadataPayload> deletedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(c1StubbedMetadata), true)
            .findFirst().get();

    // NOTE: In this case, deleted (or tombstone) record will be therefore deleting
    //       previous state of the record
    HoodieMetadataPayload deletedCombinedMetadataPayload =
        deletedColumnStatsRecord.getData().preCombine(columnStatsRecord.getData());

    assertEquals(deletedColumnStatsRecord.getData(), deletedCombinedMetadataPayload);

    // NOTE: In this case, proper incoming record will be overwriting previously deleted
    //       record
    HoodieMetadataPayload overwrittenCombinedMetadataPayload =
        columnStatsRecord.getData().preCombine(deletedColumnStatsRecord.getData());

    assertEquals(columnStatsRecord.getData(), overwrittenCombinedMetadataPayload);
  }
}

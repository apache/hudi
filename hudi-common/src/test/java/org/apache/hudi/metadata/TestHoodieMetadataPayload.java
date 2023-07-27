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

import org.apache.hudi.avro.model.HoodieRecordIndexInfo;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.CollectionUtils.createImmutableMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link HoodieMetadataPayload}.
 */
public class TestHoodieMetadataPayload extends HoodieCommonTestHarness {

  // NOTE: This is the schema of the HoodieRecordIndexInfo class without the 'rowId' field
  private static final String OLD_RECORD_INDEX_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"HoodieRecordIndexInfo\",\"namespace\":\"org.apache.hudi.avro.model\",\"fields\":["
      + "{\"name\":\"partition\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Partition which contains the record\",\"avro.java.string\":\"String\"},"
      + "{\"name\":\"fileIdHighBits\",\"type\":\"long\",\"doc\":\"fileId which contains the record (high 64 bits)\"},"
      + "{\"name\":\"fileIdLowBits\",\"type\":\"long\",\"doc\":\"fileId which contains the record (low 64 bits)\"},"
      + "{\"name\":\"fileIndex\",\"type\":\"int\",\"doc\":\"index of the file\"},"
      + "{\"name\":\"instantTime\",\"type\":\"long\",\"doc\":\"Epoch time in millisecond at which record was added\"}]}";

  @Test
  public void testFileSystemMetadataPayloadMerging() {
    String partitionName = "2022/10/01";

    Map<String, Long> firstCommitAddedFiles = createImmutableMap(
        Pair.of("file1.parquet", 1000L),
        Pair.of("file2.parquet", 2000L),
        Pair.of("file3.parquet", 3000L)
    );

    HoodieRecord<HoodieMetadataPayload> firstPartitionFilesRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(partitionName, Option.of(firstCommitAddedFiles), Option.empty());

    Map<String, Long> secondCommitAddedFiles = createImmutableMap(
        // NOTE: This is an append
        Pair.of("file3.parquet", 3333L),
        Pair.of("file4.parquet", 4000L),
        Pair.of("file5.parquet", 5000L)
    );

    List<String> secondCommitDeletedFiles = Collections.singletonList("file1.parquet");

    HoodieRecord<HoodieMetadataPayload> secondPartitionFilesRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(partitionName, Option.of(secondCommitAddedFiles), Option.of(secondCommitDeletedFiles));

    HoodieMetadataPayload combinedPartitionFilesRecordPayload =
        secondPartitionFilesRecord.getData().preCombine(firstPartitionFilesRecord.getData());

    HoodieMetadataPayload expectedCombinedPartitionedFilesRecordPayload =
        HoodieMetadataPayload.createPartitionFilesRecord(partitionName,
            Option.of(
                createImmutableMap(
                    Pair.of("file2.parquet", 2000L),
                    Pair.of("file3.parquet", 3333L),
                    Pair.of("file4.parquet", 4000L),
                    Pair.of("file5.parquet", 5000L)
                )
            ),
            Option.empty()
        ).getData();

    assertEquals(expectedCombinedPartitionedFilesRecordPayload, combinedPartitionFilesRecordPayload);
  }

  @Test
  public void testColumnStatsPayloadMerging() throws IOException {
    String partitionPath = "2022/10/01";
    String fileName = "file.parquet";
    String targetColName = "c1";

    HoodieColumnRangeMetadata<Comparable> c1Metadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 100, 1000, 5, 1000, 123456, 123456);

    HoodieRecord<HoodieMetadataPayload> columnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(partitionPath, Collections.singletonList(c1Metadata), false)
            .findFirst().get();

    ////////////////////////////////////////////////////////////////////////
    // Case 1: Combining proper (non-deleted) records
    ////////////////////////////////////////////////////////////////////////

    // NOTE: Column Stats record will only be merged in case existing file will be modified,
    //       which could only happen on storages schemes supporting appends
    HoodieColumnRangeMetadata<Comparable> c1AppendedBlockMetadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 0, 500, 0, 100, 12345, 12345);

    HoodieRecord<HoodieMetadataPayload> updatedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(partitionPath, Collections.singletonList(c1AppendedBlockMetadata), false)
            .findFirst().get();

    HoodieMetadataPayload combinedMetadataPayload =
        columnStatsRecord.getData().preCombine(updatedColumnStatsRecord.getData());

    HoodieColumnRangeMetadata<Comparable> expectedColumnRangeMetadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 0, 1000, 5, 1100, 135801, 135801);

    HoodieRecord<HoodieMetadataPayload> expectedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(partitionPath, Collections.singletonList(expectedColumnRangeMetadata), false)
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
        HoodieMetadataPayload.createColumnStatsRecords(partitionPath, Collections.singletonList(c1StubbedMetadata), true)
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

  @Test
  public void testRecordIndexPayloadSchemaEvolution() throws IOException {
    Schema oldSchema = new Schema.Parser().parse(OLD_RECORD_INDEX_SCHEMA_STRING);
    GenericRecord oldRecord = new GenericData.Record(oldSchema);
    oldRecord.put("partition", "partition");
    oldRecord.put("fileIdHighBits", 1L);
    oldRecord.put("fileIdLowBits", 1L);
    oldRecord.put("fileIndex", 1);
    oldRecord.put("instantTime", 1L);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(oldSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    // Serialize record with old schema
    writer.write(oldRecord, encoder);
    encoder.flush();
    out.close();

    byte[] serializedBytes = out.toByteArray();

    // Deserialize record with new schema
    Schema newSchema = HoodieRecordIndexInfo.SCHEMA$;
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(oldSchema, newSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
    GenericRecord newRecord = reader.read(null, decoder);

    // Assert that the rowId field in the new record is null
    assertNull(newRecord.get("rowId"));
  }
}

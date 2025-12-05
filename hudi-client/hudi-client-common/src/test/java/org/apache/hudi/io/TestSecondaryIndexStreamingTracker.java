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

package org.apache.hudi.io;

import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSecondaryIndexStreamingTracker {

  private static final String SIMPLE_SCHEMA_STR = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"TestRecord\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"id\", \"type\": \"string\"},\n"
      + "    {\"name\": \"name\", \"type\": \"string\"},\n"
      + "    {\"name\": \"fare\", \"type\": \"double\"},\n"
      + "    {\"name\": \"timestamp\", \"type\": \"long\"}\n"
      + "  ]\n"
      + "}";

  private static final String NULLABLE_SCHEMA_STR = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"TestRecord\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"id\", \"type\": \"string\"},\n"
      + "    {\"name\": \"name\", \"type\": \"string\"},\n"
      + "    {\"name\": \"fare\", \"type\": [\"null\", \"double\"], \"default\": null},\n"
      + "    {\"name\": \"timestamp\", \"type\": \"long\"}\n"
      + "  ]\n"
      + "}";

  private HoodieSchema schema;
  private HoodieSchema nullableSchema;
  private HoodieWriteConfig config;
  private WriteStatus writeStatus;
  private HoodieIndexDefinition fareIndexDef;
  
  @BeforeEach
  void setUp() {
    schema = HoodieSchema.parse(SIMPLE_SCHEMA_STR);
    nullableSchema = HoodieSchema.parse(NULLABLE_SCHEMA_STR);
    Properties props = new Properties();
    config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/test")
        .withProps(props)
        .build();
    writeStatus = new WriteStatus(true, 0.0d);
    fareIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
  }

  /**
   * Given: A new record with secondary index field value (including null)
   * When: trackSecondaryIndexStats processes the insert operation
   * Then: A single secondary index entry should be added with isDeleted=false
   */
  @ParameterizedTest
  @MethodSource("insertTestCases")
  void testTrackSecondaryIndexStats_Insert(String testName, Object fareValue, 
      String expectedSecondaryKeyValue, HoodieSchema testSchema) {
    String recordKey = "test-record-key-" + testName;
    String partitionPath = "2023/01/01";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    GenericRecord avroRecord = createRecord(testSchema, recordKey, partitionPath, 
        "Test User", fareValue, System.currentTimeMillis());
    HoodieRecord<HoodieAvroPayload> newRecord = createHoodieRecord(hoodieKey, avroRecord);
    
    // Track insert operation
    trackSecondaryIndexStats(hoodieKey, newRecord, null, false, testSchema);
    
    // Verify single insert entry
    verifySecondaryIndexStats(writeStatus, (stats, indexName) -> {
      assertEquals(1, stats.size());
      assertEquals(recordKey, stats.get(0).getRecordKey());
      assertEquals(expectedSecondaryKeyValue, stats.get(0).getSecondaryKeyValue());
      assertFalse(stats.get(0).isDeleted());
    });
  }

  private static Stream<Arguments> insertTestCases() {
    return Stream.of(
        Arguments.of("insert-normal", 100.5, "100.5", HoodieSchema.parse(SIMPLE_SCHEMA_STR)),
        Arguments.of("insert-null", null, null, HoodieSchema.parse(NULLABLE_SCHEMA_STR))
    );
  }

  /**
   * Given: An old record and a new record with potentially different secondary key values
   * When: trackSecondaryIndexStats processes the update operation
   * Then: If values differ, two entries are created (delete old, insert new); if same, no entries
   */
  @ParameterizedTest
  @MethodSource("updateTestCases")
  void testTrackSecondaryIndexStats_Update(String testName, Object oldFareValue, 
      Object newFareValue, String expectedDeleteValue, String expectedInsertValue, 
      boolean expectUpdate) {
    String recordKey = "test-record-key-" + testName;
    String partitionPath = "2023/01/02";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    GenericRecord oldAvroRecord = createRecord(nullableSchema, recordKey, partitionPath, 
        "Old User", oldFareValue, System.currentTimeMillis() - 1000);
    HoodieRecord<HoodieAvroPayload> oldRecord = createHoodieRecord(hoodieKey, oldAvroRecord);
    
    GenericRecord newAvroRecord = createRecord(nullableSchema, recordKey, partitionPath, 
        "New User", newFareValue, System.currentTimeMillis());
    HoodieRecord<HoodieAvroPayload> newRecord = createHoodieRecord(hoodieKey, newAvroRecord);
    
    // Track update operation
    trackSecondaryIndexStats(hoodieKey, newRecord, oldRecord, false, nullableSchema);
    
    if (!expectUpdate) {
      // No update expected (same value)
      assertEquals(0, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    } else {
      // Verify update entries (delete old + insert new)
      verifyUpdateEntries(writeStatus, recordKey, expectedDeleteValue, expectedInsertValue);
    }
  }

  private static Stream<Arguments> updateTestCases() {
    return Stream.of(
        Arguments.of("update-same-value", 100.5, 100.5, null, null, false),
        Arguments.of("update-different-value", 100.5, 200.75, "100.5", "200.75", true),
        Arguments.of("update-to-null", 100.5, null, "100.5", null, true),
        Arguments.of("update-from-null", null, 200.75, null, "200.75", true)
    );
  }

  /**
   * Given: An old record with secondary index field value (including null)
   * When: trackSecondaryIndexStats processes the delete operation
   * Then: A single secondary index entry should be added with isDeleted=true
   */
  @ParameterizedTest
  @MethodSource("deleteTestCases")
  void testTrackSecondaryIndexStats_Delete(String testName, Object fareValue, 
      String expectedSecondaryKeyValue) {
    String recordKey = "test-record-key-" + testName;
    String partitionPath = "2023/01/03";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    GenericRecord oldAvroRecord = createRecord(nullableSchema, recordKey, partitionPath, 
        "User to delete", fareValue, System.currentTimeMillis());
    HoodieRecord<HoodieAvroPayload> oldRecord = createHoodieRecord(hoodieKey, oldAvroRecord);
    
    // Track delete operation
    trackSecondaryIndexStats(hoodieKey, new HoodieEmptyRecord<>(null, HoodieRecord.HoodieRecordType.AVRO), oldRecord, true, nullableSchema);
    
    // Verify single delete entry
    verifySecondaryIndexStats(writeStatus, (stats, indexName) -> {
      assertEquals(1, stats.size());
      assertEquals(recordKey, stats.get(0).getRecordKey());
      assertEquals(expectedSecondaryKeyValue, stats.get(0).getSecondaryKeyValue());
      assertTrue(stats.get(0).isDeleted());
    });
  }

  private static Stream<Arguments> deleteTestCases() {
    return Stream.of(
        Arguments.of("delete-normal", 100.5, "100.5"),
        Arguments.of("delete-null", null, null)
    );
  }

  /**
   * Given: Multiple secondary index definitions on different fields (fare and name)
   * When: trackSecondaryIndexStats processes a record
   * Then: Each index should generate its own stats entry independently
   */
  @Test
  void testTrackSecondaryIndexStats_MultipleSecondaryIndexes() {
    String recordKey = "test-record-key-006";
    String partitionPath = "2023/01/06";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    GenericRecord avroRecord = createRecord(schema, recordKey, partitionPath, 
        "Multi Index User", 100.5, System.currentTimeMillis());
    HoodieRecord<HoodieAvroPayload> newRecord = createHoodieRecord(hoodieKey, avroRecord);
    
    // Create multiple index definitions
    HoodieIndexDefinition nameIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_name_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("name"))
        .build();
    
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        newRecord,
        null,
        false,
        writeStatus,
        schema,
        () -> schema,
        Arrays.asList(fareIndexDef, nameIndexDef),
        Option.empty(),
        config
    );
    
    // Should have entries for both indexes
    assertEquals(2, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    
    // Verify fare index
    List<SecondaryIndexStats> fareStats = writeStatus.getIndexStats().getSecondaryIndexStats()
        .get(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index");
    assertEquals(1, fareStats.size());
    assertEquals("100.5", fareStats.get(0).getSecondaryKeyValue());
    
    // Verify name index
    List<SecondaryIndexStats> nameStats = writeStatus.getIndexStats().getSecondaryIndexStats()
        .get(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_name_index");
    assertEquals(1, nameStats.size());
    assertEquals("Multi Index User", nameStats.get(0).getSecondaryKeyValue());
  }

  /**
   * Given: Various edge cases including empty index definitions and null records
   * When: trackSecondaryIndexStats is called with these edge cases
   * Then: Should handle gracefully - empty indexes produce no updates, null records produce no updates
   */
  @Test
  void testTrackSecondaryIndexStats_EdgeCases() {
    HoodieKey hoodieKey = new HoodieKey("test-key", "test-partition");
    GenericRecord avroRecord = createRecord(schema, "test-key", "test-partition", 
        "Test User", 100.0, System.currentTimeMillis());
    HoodieRecord<HoodieAvroPayload> newRecord = createHoodieRecord(hoodieKey, avroRecord);
    
    // Test with empty secondary index definitions
    trackSecondaryIndexStats(hoodieKey, newRecord, null, false, schema, Collections.emptyList());
    assertEquals(0, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    
    // Test with null hoodie key and empty records
    WriteStatus newWriteStatus = new WriteStatus(true, 0.0d);
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        null,
        new HoodieEmptyRecord<>(null, HoodieRecord.HoodieRecordType.AVRO),
        null,
        true,
        newWriteStatus,
        schema,
        () -> schema,
        Collections.singletonList(fareIndexDef),
        Option.empty(),
        config
    );
    assertEquals(0, newWriteStatus.getIndexStats().getSecondaryIndexStats().size());
  }

  /**
   * Given: Various scenarios where record key needs to be extracted from different sources
   * When: HoodieKey is null and record key must be extracted from old record or new record
   * Then: Record key should be successfully extracted in priority order: hoodieKey -> oldRecord -> newRecord
   */
  @Test
  void testTrackSecondaryIndexStats_RecordKeyExtraction() {
    // Test extraction from old record when hoodie key is null
    WriteStatus case1WriteStatus = new WriteStatus(true, 0.0d);
    GenericRecord oldRecordData = createRecord(schema, "old-key", "old-partition", 
        "Old User", 75.0, System.currentTimeMillis());
    HoodieRecord<HoodieAvroPayload> oldRecord = createHoodieRecord(new HoodieKey("old-key", "old-partition"), oldRecordData);
    
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        null,
        new HoodieEmptyRecord<>(null, HoodieRecord.HoodieRecordType.AVRO),
        oldRecord,
        true,
        case1WriteStatus,
        schema,
        () -> schema,
        Collections.singletonList(fareIndexDef),
        Option.empty(),
        config
    );
    
    verifySecondaryIndexStats(case1WriteStatus, (stats, indexName) -> {
      assertEquals(1, stats.size());
      assertEquals("old-key", stats.get(0).getRecordKey());
      assertEquals("75.0", stats.get(0).getSecondaryKeyValue());
      assertTrue(stats.get(0).isDeleted());
    });
    
    // Test extraction from new record when hoodie key and old record are null
    WriteStatus case2WriteStatus = new WriteStatus(true, 0.0d);
    GenericRecord newRecordData = createRecord(schema, "new-key", "new-partition", 
        "New User", 85.0, System.currentTimeMillis());
    HoodieRecord<HoodieAvroPayload> newRecord = createHoodieRecord(new HoodieKey("new-key", "new-partition"), newRecordData);
    
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        null,
        newRecord,
        null,
        false,
        case2WriteStatus,
        schema,
        () -> schema,
        Collections.singletonList(fareIndexDef),
        Option.empty(),
        config
    );
    
    verifySecondaryIndexStats(case2WriteStatus, (stats, indexName) -> {
      assertEquals(1, stats.size());
      assertEquals("new-key", stats.get(0).getRecordKey());
      assertEquals("85.0", stats.get(0).getSecondaryKeyValue());
      assertFalse(stats.get(0).isDeleted());
    });
  }

  // Helper Methods

  private GenericRecord createRecord(HoodieSchema recordSchema, String recordKey, String partitionPath,
      String name, Object fare, long timestamp) {
    GenericRecord avroRecord = new GenericData.Record(recordSchema.toAvroSchema());
    avroRecord.put("_hoodie_record_key", recordKey);
    avroRecord.put("_hoodie_partition_path", partitionPath);
    avroRecord.put("_hoodie_commit_time", "20231201120000");
    avroRecord.put("_hoodie_commit_seqno", "001");
    avroRecord.put("_hoodie_file_name", "test-file.parquet");
    avroRecord.put("id", recordKey);
    avroRecord.put("name", name);
    avroRecord.put("fare", fare);
    avroRecord.put("timestamp", timestamp);
    return avroRecord;
  }

  private HoodieRecord<HoodieAvroPayload> createHoodieRecord(HoodieKey key, GenericRecord avroRecord) {
    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(avroRecord));
    return new HoodieAvroRecord<>(key, payload);
  }

  private void trackSecondaryIndexStats(HoodieKey hoodieKey, HoodieRecord<HoodieAvroPayload> newRecord,
      HoodieRecord<HoodieAvroPayload> oldRecord, boolean isDelete, HoodieSchema recordSchema) {
    trackSecondaryIndexStats(hoodieKey, newRecord, oldRecord, isDelete, recordSchema, 
        Collections.singletonList(fareIndexDef));
  }

  private void trackSecondaryIndexStats(HoodieKey hoodieKey, HoodieRecord<HoodieAvroPayload> newRecord,
      HoodieRecord<HoodieAvroPayload> oldRecord, boolean isDelete, HoodieSchema recordSchema,
      List<HoodieIndexDefinition> indexDefs) {
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        newRecord,
        oldRecord,
        isDelete,
        writeStatus,
        recordSchema,
        () -> recordSchema,
        indexDefs,
        Option.empty(),
        config
    );
  }

  private void verifySecondaryIndexStats(WriteStatus status, 
      BiConsumer<List<SecondaryIndexStats>, String> verification) {
    assertFalse(status.getIndexStats().getSecondaryIndexStats().isEmpty());
    status.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> 
        verification.accept(statsList, indexName));
  }

  private void verifyUpdateEntries(WriteStatus status, String recordKey, 
      String expectedDeleteValue, String expectedInsertValue) {
    verifySecondaryIndexStats(status, (stats, indexName) -> {
      assertEquals(2, stats.size());
      
      boolean foundDelete = false;
      boolean foundInsert = false;
      for (SecondaryIndexStats stat : stats) {
        assertEquals(recordKey, stat.getRecordKey());
        if (stat.isDeleted()) {
          assertEquals(expectedDeleteValue, stat.getSecondaryKeyValue());
          foundDelete = true;
        } else {
          assertEquals(expectedInsertValue, stat.getSecondaryKeyValue());
          foundInsert = true;
        }
      }
      assertTrue(foundDelete && foundInsert, "Should have both delete and insert entries");
    });
  }
}
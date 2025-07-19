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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSecondaryIndexStreamingTracker {

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

  private Schema schema;
  private HoodieWriteConfig config;
  private WriteStatus writeStatus;
  
  @BeforeEach
  public void setUp() {
    schema = new Schema.Parser().parse(SIMPLE_SCHEMA_STR);
    Properties props = new Properties();
    config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/test")
        .withProps(props)
        .build();
    writeStatus = new WriteStatus(true, 0.0d);
  }

  /**
   * Given: A new record with no corresponding old record
   * When: trackSecondaryIndexStats is called with only a new record
   * Then: A single secondary index entry should be added with isDeleted=false
   */
  @Test
  public void testTrackSecondaryIndexStats_InsertNewRecord() {
    // Create a HoodieKey
    String recordKey = "test-record-key-001";
    String partitionPath = "2023/01/01";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    // Create a new record with a secondary index field value
    GenericRecord avroRecord = new GenericData.Record(schema);
    
    // Set Hoodie metadata fields
    avroRecord.put("_hoodie_record_key", recordKey);
    avroRecord.put("_hoodie_partition_path", partitionPath);
    avroRecord.put("_hoodie_commit_time", "20231201120000");
    avroRecord.put("_hoodie_commit_seqno", "001");
    avroRecord.put("_hoodie_file_name", "test-file.parquet");
    
    // Set data fields
    avroRecord.put("id", recordKey);
    avroRecord.put("name", "Test User");
    avroRecord.put("fare", 100.5); // This will be our secondary index field
    avroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(avroRecord));
    HoodieRecord newRecord = new HoodieAvroRecord<>(hoodieKey, payload);
    
    // Create secondary index definition for the "fare" field
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    // Test case: Insert a new record (no old record exists)
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.of(newRecord),
        null, // No old record
        false, // Not a delete
        writeStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(), // No key generator needed for this test
        config
    );
    
    // Verify that a secondary index entry was added (not deleted)
    assertFalse(writeStatus.getIndexStats().getSecondaryIndexStats().isEmpty());
    
    // Check that the entry is an insert (isDeleted = false)
    writeStatus.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> {
      assertEquals(1, statsList.size());
      statsList.forEach(stats -> {
        assertEquals(recordKey, stats.getRecordKey());
        assertEquals("100.5", stats.getSecondaryKeyValue()); // The fare value
        assertFalse(stats.isDeleted()); // isDeleted should be false for insert
      });
    });
  }

  /**
   * Given: An old record and a new record with the same secondary key value
   * When: trackSecondaryIndexStats compares the records
   * Then: No secondary index updates should be recorded (shouldUpdate=false optimization)
   */
  @Test
  public void testTrackSecondaryIndexStats_UpdateWithSameSecondaryKey() {
    // Create a HoodieKey
    String recordKey = "test-record-key-002";
    String partitionPath = "2023/01/02";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    // Create old record
    GenericRecord oldAvroRecord = new GenericData.Record(schema);
    oldAvroRecord.put("_hoodie_record_key", recordKey);
    oldAvroRecord.put("_hoodie_partition_path", partitionPath);
    oldAvroRecord.put("_hoodie_commit_time", "20231201120000");
    oldAvroRecord.put("_hoodie_commit_seqno", "001");
    oldAvroRecord.put("_hoodie_file_name", "test-file.parquet");
    oldAvroRecord.put("id", recordKey);
    oldAvroRecord.put("name", "Old User");
    oldAvroRecord.put("fare", 100.5); // Same fare value
    oldAvroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload oldPayload = new HoodieAvroPayload(Option.of(oldAvroRecord));
    HoodieRecord oldRecord = new HoodieAvroRecord<>(hoodieKey, oldPayload);
    
    // Create new record with same fare value
    GenericRecord newAvroRecord = new GenericData.Record(schema);
    newAvroRecord.put("_hoodie_record_key", recordKey);
    newAvroRecord.put("_hoodie_partition_path", partitionPath);
    newAvroRecord.put("_hoodie_commit_time", "20231201130000");
    newAvroRecord.put("_hoodie_commit_seqno", "002");
    newAvroRecord.put("_hoodie_file_name", "test-file.parquet");
    newAvroRecord.put("id", recordKey);
    newAvroRecord.put("name", "Updated User");
    newAvroRecord.put("fare", 100.5); // Same fare value
    newAvroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload newPayload = new HoodieAvroPayload(Option.of(newAvroRecord));
    HoodieRecord newRecord = new HoodieAvroRecord<>(hoodieKey, newPayload);
    
    // Create secondary index definition
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    // Test case: Update with same secondary key (shouldUpdate = false)
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.of(newRecord),
        oldRecord,
        false,
        writeStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(),
        config
    );
    
    // No secondary index updates should be recorded
    assertEquals(0, writeStatus.getIndexStats().getSecondaryIndexStats().size());
  }

  /**
   * Given: An old record and a new record with different secondary key values
   * When: trackSecondaryIndexStats processes the update
   * Then: Two entries should be recorded - delete old value (isDeleted=true) and insert new value (isDeleted=false)
   */
  @Test
  public void testTrackSecondaryIndexStats_UpdateWithDifferentSecondaryKey() {
    // Create a HoodieKey
    String recordKey = "test-record-key-003";
    String partitionPath = "2023/01/03";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    // Create old record
    GenericRecord oldAvroRecord = new GenericData.Record(schema);
    oldAvroRecord.put("_hoodie_record_key", recordKey);
    oldAvroRecord.put("_hoodie_partition_path", partitionPath);
    oldAvroRecord.put("_hoodie_commit_time", "20231201120000");
    oldAvroRecord.put("_hoodie_commit_seqno", "001");
    oldAvroRecord.put("_hoodie_file_name", "test-file.parquet");
    oldAvroRecord.put("id", recordKey);
    oldAvroRecord.put("name", "Old User");
    oldAvroRecord.put("fare", 100.5);
    oldAvroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload oldPayload = new HoodieAvroPayload(Option.of(oldAvroRecord));
    HoodieRecord oldRecord = new HoodieAvroRecord<>(hoodieKey, oldPayload);
    
    // Create new record with different fare value
    GenericRecord newAvroRecord = new GenericData.Record(schema);
    newAvroRecord.put("_hoodie_record_key", recordKey);
    newAvroRecord.put("_hoodie_partition_path", partitionPath);
    newAvroRecord.put("_hoodie_commit_time", "20231201130000");
    newAvroRecord.put("_hoodie_commit_seqno", "002");
    newAvroRecord.put("_hoodie_file_name", "test-file.parquet");
    newAvroRecord.put("id", recordKey);
    newAvroRecord.put("name", "Updated User");
    newAvroRecord.put("fare", 200.75); // Different fare value
    newAvroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload newPayload = new HoodieAvroPayload(Option.of(newAvroRecord));
    HoodieRecord newRecord = new HoodieAvroRecord<>(hoodieKey, newPayload);
    
    // Create secondary index definition
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    // Test case: Update with different secondary key
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.of(newRecord),
        oldRecord,
        false,
        writeStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(),
        config
    );
    
    // Should have 2 entries: delete old and insert new
    assertFalse(writeStatus.getIndexStats().getSecondaryIndexStats().isEmpty());
    writeStatus.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> {
      assertEquals(2, statsList.size());
      
      // First entry should be delete of old value
      assertEquals(recordKey, statsList.get(0).getRecordKey());
      assertEquals("100.5", statsList.get(0).getSecondaryKeyValue());
      assertTrue(statsList.get(0).isDeleted());
      
      // Second entry should be insert of new value
      assertEquals(recordKey, statsList.get(1).getRecordKey());
      assertEquals("200.75", statsList.get(1).getSecondaryKeyValue());
      assertFalse(statsList.get(1).isDeleted());
    });
  }

  /**
   * Given: An old record exists but no new record (deletion scenario)
   * When: trackSecondaryIndexStats is called with isDelete=true
   * Then: A single secondary index entry should be added with isDeleted=true
   */
  @Test
  public void testTrackSecondaryIndexStats_DeleteRecord() {
    // Create a HoodieKey
    String recordKey = "test-record-key-004";
    String partitionPath = "2023/01/04";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    // Create old record
    GenericRecord oldAvroRecord = new GenericData.Record(schema);
    oldAvroRecord.put("_hoodie_record_key", recordKey);
    oldAvroRecord.put("_hoodie_partition_path", partitionPath);
    oldAvroRecord.put("_hoodie_commit_time", "20231201120000");
    oldAvroRecord.put("_hoodie_commit_seqno", "001");
    oldAvroRecord.put("_hoodie_file_name", "test-file.parquet");
    oldAvroRecord.put("id", recordKey);
    oldAvroRecord.put("name", "User to Delete");
    oldAvroRecord.put("fare", 150.0);
    oldAvroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload oldPayload = new HoodieAvroPayload(Option.of(oldAvroRecord));
    HoodieRecord oldRecord = new HoodieAvroRecord<>(hoodieKey, oldPayload);
    
    // Create secondary index definition
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    // Test case: Delete record
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.empty(), // No new record
        oldRecord,
        true, // isDelete = true
        writeStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(),
        config
    );
    
    // Should have 1 delete entry
    assertFalse(writeStatus.getIndexStats().getSecondaryIndexStats().isEmpty());
    writeStatus.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> {
      assertEquals(1, statsList.size());
      assertEquals(recordKey, statsList.get(0).getRecordKey());
      assertEquals("150.0", statsList.get(0).getSecondaryKeyValue());
      assertTrue(statsList.get(0).isDeleted());
    });
  }

  /**
   * Given: Both old record and combined record exist but isDelete=true
   * When: trackSecondaryIndexStats processes with the condition combinedRecordOpt.isPresent() && !isDelete
   * Then: hasNewValue should be false (branch coverage) and only delete entry should be recorded
   */
  @Test
  public void testTrackSecondaryIndexStats_DeleteRecordWithCombinedRecord() {
    // Test the branch where combinedRecordOpt.isPresent() but isDelete = true
    String recordKey = "test-record-key-004b";
    String partitionPath = "2023/01/04";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    // Create old record
    GenericRecord oldAvroRecord = new GenericData.Record(schema);
    oldAvroRecord.put("_hoodie_record_key", recordKey);
    oldAvroRecord.put("_hoodie_partition_path", partitionPath);
    oldAvroRecord.put("_hoodie_commit_time", "20231201120000");
    oldAvroRecord.put("_hoodie_commit_seqno", "001");
    oldAvroRecord.put("_hoodie_file_name", "test-file.parquet");
    oldAvroRecord.put("id", recordKey);
    oldAvroRecord.put("name", "User to Delete");
    oldAvroRecord.put("fare", 150.0);
    oldAvroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload oldPayload = new HoodieAvroPayload(Option.of(oldAvroRecord));
    HoodieRecord oldRecord = new HoodieAvroRecord<>(hoodieKey, oldPayload);
    
    // Create a combined record (present but marked for deletion)
    GenericRecord combinedAvroRecord = new GenericData.Record(schema);
    combinedAvroRecord.put("_hoodie_record_key", recordKey);
    combinedAvroRecord.put("_hoodie_partition_path", partitionPath);
    combinedAvroRecord.put("_hoodie_commit_time", "20231201130000");
    combinedAvroRecord.put("_hoodie_commit_seqno", "002");
    combinedAvroRecord.put("_hoodie_file_name", "test-file.parquet");
    combinedAvroRecord.put("id", recordKey);
    combinedAvroRecord.put("name", "Deleted User");
    combinedAvroRecord.put("fare", 150.0);
    combinedAvroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload combinedPayload = new HoodieAvroPayload(Option.of(combinedAvroRecord));
    HoodieRecord combinedRecord = new HoodieAvroRecord<>(hoodieKey, combinedPayload);
    
    // Create secondary index definition
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    // Test case: Delete record with combined record present (isDelete = true)
    // This tests the branch where combinedRecordOpt.isPresent() && !isDelete evaluates to false
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.of(combinedRecord), // Combined record is present
        oldRecord,
        true, // isDelete = true, so hasNewValue will be false
        writeStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(),
        config
    );
    
    // Should have 1 delete entry (hasNewValue = false because isDelete = true)
    assertFalse(writeStatus.getIndexStats().getSecondaryIndexStats().isEmpty());
    writeStatus.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> {
      assertEquals(1, statsList.size());
      assertEquals(recordKey, statsList.get(0).getRecordKey());
      assertEquals("150.0", statsList.get(0).getSecondaryKeyValue());
      assertTrue(statsList.get(0).isDeleted());
    });
  }

  /**
   * Given: A record with null secondary key value
   * When: trackSecondaryIndexStats processes the record
   * Then: The null value should be handled correctly and stored as null in secondary index stats
   */
  @Test
  public void testTrackSecondaryIndexStats_NullSecondaryKeyValues() {
    // Test with null secondary key in new record
    String recordKey = "test-record-key-005";
    String partitionPath = "2023/01/05";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    // Create schema with nullable fare
    String nullableSchema = "{\n"
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
    Schema nullableSchemaObj = new Schema.Parser().parse(nullableSchema);
    
    // Create record with null fare
    GenericRecord avroRecord = new GenericData.Record(nullableSchemaObj);
    avroRecord.put("_hoodie_record_key", recordKey);
    avroRecord.put("_hoodie_partition_path", partitionPath);
    avroRecord.put("_hoodie_commit_time", "20231201120000");
    avroRecord.put("_hoodie_commit_seqno", "001");
    avroRecord.put("_hoodie_file_name", "test-file.parquet");
    avroRecord.put("id", recordKey);
    avroRecord.put("name", "User with null fare");
    avroRecord.put("fare", null); // Null secondary key
    avroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(avroRecord));
    HoodieRecord newRecord = new HoodieAvroRecord<>(hoodieKey, payload);
    
    // Create secondary index definition
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    // Test case: Insert with null secondary key
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.of(newRecord),
        null,
        false,
        writeStatus,
        nullableSchemaObj,
        () -> nullableSchemaObj,
        Arrays.asList(indexDef),
        Option.empty(),
        config
    );
    
    // Should have 1 entry with null secondary key
    assertFalse(writeStatus.getIndexStats().getSecondaryIndexStats().isEmpty());
    writeStatus.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> {
      assertEquals(1, statsList.size());
      assertEquals(recordKey, statsList.get(0).getRecordKey());
      assertEquals(null, statsList.get(0).getSecondaryKeyValue()); // Null is valid
      assertFalse(statsList.get(0).isDeleted());
    });
  }

  /**
   * Given: Multiple secondary index definitions on different fields (fare and name)
   * When: trackSecondaryIndexStats processes a record
   * Then: Each index should generate its own stats entry independently
   */
  @Test
  public void testTrackSecondaryIndexStats_MultipleSecondaryIndexes() {
    // Create a HoodieKey
    String recordKey = "test-record-key-006";
    String partitionPath = "2023/01/06";
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    
    // Create record
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("_hoodie_record_key", recordKey);
    avroRecord.put("_hoodie_partition_path", partitionPath);
    avroRecord.put("_hoodie_commit_time", "20231201120000");
    avroRecord.put("_hoodie_commit_seqno", "001");
    avroRecord.put("_hoodie_file_name", "test-file.parquet");
    avroRecord.put("id", recordKey);
    avroRecord.put("name", "Multi Index User");
    avroRecord.put("fare", 100.5);
    avroRecord.put("timestamp", System.currentTimeMillis());
    
    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(avroRecord));
    HoodieRecord newRecord = new HoodieAvroRecord<>(hoodieKey, payload);
    
    // Create multiple secondary index definitions
    HoodieIndexDefinition fareIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
        
    HoodieIndexDefinition nameIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_name_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("name"))
        .build();
    
    // Test case: Multiple secondary indexes
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.of(newRecord),
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
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats()
        .get(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index").size());
    assertEquals("100.5", writeStatus.getIndexStats().getSecondaryIndexStats()
        .get(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index").get(0).getSecondaryKeyValue());
    
    // Verify name index
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats()
        .get(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_name_index").size());
    assertEquals("Multi Index User", writeStatus.getIndexStats().getSecondaryIndexStats()
        .get(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_name_index").get(0).getSecondaryKeyValue());
  }

  /**
   * Given: Various edge cases including empty index definitions and null records
   * When: trackSecondaryIndexStats is called with these edge cases
   * Then: Should handle gracefully - empty indexes produce no updates, null records with proper fix produce no updates
   */
  @Test
  public void testTrackSecondaryIndexStats_EdgeCases() {
    // Test with empty secondary index definitions list
    HoodieKey hoodieKey = new HoodieKey("test-key", "test-partition");
    GenericRecord avroRecord = createTestRecord("test-key", "test-partition");
    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(avroRecord));
    HoodieRecord newRecord = new HoodieAvroRecord<>(hoodieKey, payload);
    
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        hoodieKey,
        Option.of(newRecord),
        null,
        false,
        writeStatus,
        schema,
        () -> schema,
        Collections.emptyList(), // Empty index definitions
        Option.empty(),
        config
    );
    
    // No secondary index updates should be recorded
    assertEquals(0, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    
    // Test with null hoodie key but valid records (record key extracted from records)
    WriteStatus newWriteStatus = new WriteStatus(true, 0.0d);
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
        
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        null, // null hoodie key
        Option.empty(), // empty new record
        null, // null old record
        false,
        newWriteStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(),
        config
    );
    
    // No updates should be recorded (both old and new are null)
    assertEquals(0, newWriteStatus.getIndexStats().getSecondaryIndexStats().size());
  }

  /**
   * Given: Various scenarios where record key needs to be extracted from different sources
   * When: HoodieKey is null and record key must be extracted from old record or new record
   * Then: Record key should be successfully extracted in priority order: hoodieKey -> oldRecord -> newRecord
   */
  @Test
  public void testTrackSecondaryIndexStats_RecordKeyExtraction() {
    // Test record key extraction from different sources
    HoodieIndexDefinition indexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_fare_index")
        .withIndexType("SECONDARY_INDEX")
        .withSourceFields(Collections.singletonList("fare"))
        .build();
    
    // Case 1: Extract from old record when hoodie key and new record are null
    WriteStatus case1WriteStatus = new WriteStatus(true, 0.0d);
    GenericRecord oldRecordData = createTestRecord("old-key", "old-partition");
    oldRecordData.put("fare", 75.0);
    HoodieAvroPayload oldPayload = new HoodieAvroPayload(Option.of(oldRecordData));
    HoodieRecord oldRecord = new HoodieAvroRecord<>(
        new HoodieKey("old-key", "old-partition"), oldPayload);
    
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        null, // null hoodie key
        Option.empty(), // no new record
        oldRecord, // old record present
        true, // isDelete = true
        case1WriteStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(), // no key generator
        config
    );
    
    // Should extract key from old record
    assertEquals(1, case1WriteStatus.getIndexStats().getSecondaryIndexStats().size());
    case1WriteStatus.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> {
      assertEquals("old-key", statsList.get(0).getRecordKey());
      assertEquals("75.0", statsList.get(0).getSecondaryKeyValue());
      assertTrue(statsList.get(0).isDeleted());
    });
    
    // Case 2: Extract from new record when hoodie key is null but new record exists
    WriteStatus case2WriteStatus = new WriteStatus(true, 0.0d);
    GenericRecord newRecordData = createTestRecord("new-key", "new-partition");
    newRecordData.put("fare", 85.0);
    HoodieAvroPayload newPayload = new HoodieAvroPayload(Option.of(newRecordData));
    HoodieRecord newRecord = new HoodieAvroRecord<>(
        new HoodieKey("new-key", "new-partition"), newPayload);
    
    SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
        null, // null hoodie key
        Option.of(newRecord), // new record present
        null, // no old record
        false, // isDelete = false
        case2WriteStatus,
        schema,
        () -> schema,
        Arrays.asList(indexDef),
        Option.empty(),
        config
    );
    
    // Should extract key from new record
    assertEquals(1, case2WriteStatus.getIndexStats().getSecondaryIndexStats().size());
    case2WriteStatus.getIndexStats().getSecondaryIndexStats().forEach((indexName, statsList) -> {
      assertEquals("new-key", statsList.get(0).getRecordKey());
      assertEquals("85.0", statsList.get(0).getSecondaryKeyValue());
      assertFalse(statsList.get(0).isDeleted());
    });
  }
  
  private GenericRecord createTestRecord(String recordKey, String partitionPath) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("_hoodie_record_key", recordKey);
    avroRecord.put("_hoodie_partition_path", partitionPath);
    avroRecord.put("_hoodie_commit_time", "20231201120000");
    avroRecord.put("_hoodie_commit_seqno", "001");
    avroRecord.put("_hoodie_file_name", "test-file.parquet");
    avroRecord.put("id", recordKey);
    avroRecord.put("name", "Test User");
    avroRecord.put("fare", 100.0);
    avroRecord.put("timestamp", System.currentTimeMillis());
    return avroRecord;
  }
}
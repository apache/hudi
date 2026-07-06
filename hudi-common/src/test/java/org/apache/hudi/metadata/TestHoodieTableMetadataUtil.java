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

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getIndexVersionOption;
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.constructSecondaryIndexKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieTableMetadataUtil {

  @Test
  void testGetRecordKeyToFileGroupIndexFunction() {
    int numFileGroups = 10;
    String recordKey = "recordKey$";
    String secondaryKey = "secondaryKey$";
    // Raw key used for read path
    SecondaryIndexPrefixRawKey rawKey1 = new SecondaryIndexPrefixRawKey(secondaryKey);
    // Composite key used for write path
    String compositeKey = constructSecondaryIndexKey(secondaryKey, recordKey);

    SerializableBiFunction<String, Integer, Integer> hashOnSecKeyOnly =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(true);
    SerializableBiFunction<String, Integer, Integer> hashOnFullKey =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(false);

    // On write path we use hashOnSecKeyOnly
    int result1 = hashOnSecKeyOnly.apply(compositeKey, numFileGroups);
    // On read path, we use hashOnFullKey
    int result2 = hashOnFullKey.apply(rawKey1.encode(), numFileGroups);

    // Both should hash the secondary key portion so read and write paths are consistent.
    assertEquals(result1, result2);
  }

  @Test
  void testGetIndexVersionNoMetadata() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());

    Option<HoodieIndexVersion> result = getIndexVersionOption(PARTITION_NAME_COLUMN_STATS, metaClient);
    assertFalse(result.isPresent());
  }

  @Test
  void testGetIndexVersionNoPartitions() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.emptyMap()); // empty map

    Option<HoodieIndexVersion> result = getIndexVersionOption(PARTITION_NAME_COLUMN_STATS, metaClient);
    assertFalse(result.isPresent());
  }

  @Test
  void testGetColStatsVersionPartitionStatsExists() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexVersion version = HoodieIndexVersion.V1;
    HoodieIndexDefinition def = mock(HoodieIndexDefinition.class);
    when(def.getVersion()).thenReturn(version);
    Map<String, HoodieIndexDefinition> indexDefs = Collections.singletonMap(PARTITION_NAME_PARTITION_STATS, def);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(indexDefs);
    Option<HoodieIndexVersion> result = getIndexVersionOption(PARTITION_NAME_COLUMN_STATS, metaClient);
    assertTrue(result.isPresent());
    assertEquals(version, result.get());
  }

  @Test
  void testGetPartitionStatsVersionColStatsExists() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexVersion version = HoodieIndexVersion.V1;
    HoodieIndexDefinition def = mock(HoodieIndexDefinition.class);
    when(def.getVersion()).thenReturn(version);
    Map<String, HoodieIndexDefinition> indexDefs = Collections.singletonMap(PARTITION_NAME_COLUMN_STATS, def);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(indexDefs);

    Option<HoodieIndexVersion> result = getIndexVersionOption(PARTITION_NAME_PARTITION_STATS, metaClient);
    assertTrue(result.isPresent());
    assertEquals(version, result.get());
  }

  @Test
  void testGetColAndPartitionStatsIndexBothExist() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexVersion version = HoodieIndexVersion.V1;
    HoodieIndexDefinition defColStats = mock(HoodieIndexDefinition.class);
    when(defColStats.getVersion()).thenReturn(version);
    // NOTE: this is intentionally set to a different version than col stats
    // but should never happen outside of this test.
    HoodieIndexVersion otherVersion = HoodieIndexVersion.V2;
    HoodieIndexDefinition defPartStats = mock(HoodieIndexDefinition.class);
    when(defPartStats.getVersion()).thenReturn(otherVersion);
    Map<String, HoodieIndexDefinition> indexDefs = new HashMap<>(2);
    indexDefs.put(PARTITION_NAME_COLUMN_STATS, defColStats);
    indexDefs.put(PARTITION_NAME_PARTITION_STATS, defPartStats);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(indexDefs);

    Option<HoodieIndexVersion> result = getIndexVersionOption(PARTITION_NAME_COLUMN_STATS, metaClient);
    assertTrue(result.isPresent());
    assertEquals(version, result.get());

    result = getIndexVersionOption(PARTITION_NAME_PARTITION_STATS, metaClient);
    assertTrue(result.isPresent());
    assertEquals(otherVersion, result.get());
  }

  @Test
  void testGetArbitraryIndexVersion() {
    String indexName = "asdf";
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexVersion version = HoodieIndexVersion.V1;
    HoodieIndexDefinition def = mock(HoodieIndexDefinition.class);
    when(def.getVersion()).thenReturn(version);
    Map<String, HoodieIndexDefinition> indexDefs = Collections.singletonMap(indexName, def);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(indexDefs);

    Option<HoodieIndexVersion> result = getIndexVersionOption(indexName, metaClient);
    assertTrue(result.isPresent());
    assertEquals(version, result.get());
  }

  @Test
  void testFiltersOutTimestampMillisColumns() {
    HoodieSchema tableSchema = HoodieSchema.createRecord(
        "record",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("created_at", HoodieSchema.createTimestampMillis()),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT))
        )
    );

    // Case 1: Verify timestamp-millis field is excluded
    List<String> inputCols = Arrays.asList("name", "created_at", "age");
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withVersion(HoodieIndexVersion.V1)
        .withSourceFields(inputCols)
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .build();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getTableInitialVersion()).thenReturn(HoodieTableVersion.NINE);
    List<String> result = HoodieTableMetadataUtil.getValidIndexedColumns(indexDefinition, tableSchema, tableConfig);
    assertEquals(Arrays.asList("name", "age"), result);
    assertFalse(result.contains("created_at"), "Timestamp-millis field should be excluded");

    // Case 2: Verify all fields are included for V2
    indexDefinition = HoodieIndexDefinition.newBuilder()
        .withVersion(HoodieIndexVersion.V2)
        .withSourceFields(inputCols)
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .build();
    result = HoodieTableMetadataUtil.getValidIndexedColumns(indexDefinition, tableSchema, tableConfig);
    assertEquals(Arrays.asList("name", "created_at", "age"), result);

    // Case 3: Verify timestamp-millis field is excluded for initial table version < 9.
    HoodieTableConfig newTableConfig = mock(HoodieTableConfig.class);
    when(newTableConfig.getTableInitialVersion()).thenReturn(HoodieTableVersion.SIX);
    result = HoodieTableMetadataUtil.getValidIndexedColumns(indexDefinition, tableSchema, newTableConfig);
    assertEquals(Arrays.asList("name", "age"), result);

    // Case 4: Non-timestamp columns should remain unchanged
    inputCols = Arrays.asList("name", "age");
    indexDefinition = HoodieIndexDefinition.newBuilder()
        .withVersion(HoodieIndexVersion.V1)
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(inputCols)
        .build();
    result = HoodieTableMetadataUtil.getValidIndexedColumns(indexDefinition, tableSchema, tableConfig);
    assertEquals(inputCols, result, "Non-timestamp columns should remain unchanged");

    // Case 5: Empty input should return empty output
    indexDefinition = HoodieIndexDefinition.newBuilder()
        .withVersion(HoodieIndexVersion.V1)
        .withSourceFields(Collections.emptyList())
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .build();
    result = HoodieTableMetadataUtil.getValidIndexedColumns(indexDefinition, tableSchema, tableConfig);
    assertTrue(result.isEmpty(), "Expected empty output for empty input");
  }

  @Test
  void testFilterNestedLogicalTimestampColumn() {
    // Create ProfileRecord (innermost nested record)
    HoodieSchema profileRecordSchema = HoodieSchema.createRecord(
        "ProfileRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("ts_millis", HoodieSchema.createTimestampMillis()),
            HoodieSchemaField.of("ts_micros", HoodieSchema.createTimestampMicros()),
            HoodieSchemaField.of("display_name", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Create UserRecord (middle level nested record)
    HoodieSchema userRecordSchema = HoodieSchema.createRecord(
        "UserRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("profile", profileRecordSchema),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT))
        )
    );

    // Create RootRecord (top level)
    HoodieSchema nestedSchema = HoodieSchema.createRecord(
        "RootRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("user", userRecordSchema),
            HoodieSchemaField.of("event_id", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    List<String> inputCols = Arrays.asList(
        "event_id",
        "user.profile.ts_millis",
        "user.profile.ts_micros",
        "user.profile.display_name",
        "user.age"
    );

    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withVersion(HoodieIndexVersion.V1)
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(inputCols)
        .build();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getTableInitialVersion()).thenReturn(HoodieTableVersion.NINE);
    List<String> result = HoodieTableMetadataUtil.getValidIndexedColumns(indexDefinition, nestedSchema, tableConfig);

    // should filter out only the timestamp millis field
    assertEquals(
        Arrays.asList("event_id", "user.profile.ts_micros", "user.profile.display_name", "user.age"),
        result,
        "Nested timestamp-millis field should be filtered out"
    );
  }

  @Test
  void testIsTimestampMillisField() {
    // Test timestamp-millis
    HoodieSchema timestampMillisSchema = HoodieSchema.createTimestampMillis();
    assertTrue(HoodieTableMetadataUtil.isTimestampMillisField(timestampMillisSchema),
        "Should return true for timestamp-millis");

    // Test nullable timestamp-millis
    HoodieSchema nullableTimestampMillisSchema = HoodieSchema.createNullable(HoodieSchema.createTimestampMillis());
    assertTrue(HoodieTableMetadataUtil.isTimestampMillisField(nullableTimestampMillisSchema),
        "Should return true for nullable timestamp-millis");

    // Test timestamp-micros (should return false)
    HoodieSchema timestampMicrosSchema = HoodieSchema.createTimestampMicros();
    assertFalse(HoodieTableMetadataUtil.isTimestampMillisField(timestampMicrosSchema),
        "Should return false for timestamp-micros");

    // Test regular long (should return false)
    HoodieSchema longSchema = HoodieSchema.create(HoodieSchemaType.LONG);
    assertFalse(HoodieTableMetadataUtil.isTimestampMillisField(longSchema),
        "Should return false for regular long");

    // Test string (should return false)
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertFalse(HoodieTableMetadataUtil.isTimestampMillisField(stringSchema),
        "Should return false for string");
  }

  @Test
  void testVectorColumnsAreNotSupportedForV2ColumnStats() {
    HoodieSchema vectorSchema = HoodieSchema.createNullable(HoodieSchema.createVector(128));
    HoodieSchema stringSchema = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING));

    assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(vectorSchema, Option.empty(), HoodieIndexVersion.V2));
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(stringSchema, Option.empty(), HoodieIndexVersion.V2));
  }

  @Test
  void testVariantBlobVectorColumnsAreNotSupportedForV1ColumnStats() {
    HoodieSchema variantSchema = HoodieSchema.createNullable(HoodieSchema.createVariant());
    HoodieSchema blobSchema = HoodieSchema.createNullable(HoodieSchema.createBlob());
    HoodieSchema vectorSchema = HoodieSchema.createNullable(HoodieSchema.createVector(128));
    HoodieSchema stringSchema = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING));

    for (HoodieRecordType recordType : new HoodieRecordType[] {HoodieRecordType.AVRO, HoodieRecordType.SPARK}) {
      Option<HoodieRecordType> rt = Option.of(recordType);
      assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(variantSchema, rt, HoodieIndexVersion.V1),
          "VARIANT must be excluded from V1 column stats for record type " + recordType);
      assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(blobSchema, rt, HoodieIndexVersion.V1),
          "BLOB must be excluded from V1 column stats for record type " + recordType);
      assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(vectorSchema, rt, HoodieIndexVersion.V1),
          "VECTOR must be excluded from V1 column stats for record type " + recordType);
      assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(stringSchema, rt, HoodieIndexVersion.V1),
          "STRING should remain supported for record type " + recordType);
    }
  }

  @Test
  void testCreateRecordIndexUpdateMillisOverloadMatchesStringOverload() {
    String instantTime = "20260610153045678";
    long instantTimeMillis = HoodieMetadataPayload.parseRecordIndexInstantTime(instantTime);

    // uuid-encoded fileId (encoding 0)
    HoodieRecord<HoodieMetadataPayload> fromString = HoodieMetadataPayload.createRecordIndexUpdate(
        "rk1", "p1", "49b8b3c8-9e5d-4731-9d51-a2d8e9b5c7f3-0", instantTime, 0);
    HoodieRecord<HoodieMetadataPayload> fromMillis = HoodieMetadataPayload.createRecordIndexUpdate(
        "rk1", "p1", "49b8b3c8-9e5d-4731-9d51-a2d8e9b5c7f3-0", instantTimeMillis, 0);
    assertEquals(fromString.getKey(), fromMillis.getKey());
    assertEquals(fromString.getData(), fromMillis.getData());

    // raw fileId (encoding 1)
    fromString = HoodieMetadataPayload.createRecordIndexUpdate(
        "rk1", "p1", "some-raw-file-id", instantTime, 1);
    fromMillis = HoodieMetadataPayload.createRecordIndexUpdate(
        "rk1", "p1", "some-raw-file-id", instantTimeMillis, 1);
    assertEquals(fromString.getKey(), fromMillis.getKey());
    assertEquals(fromString.getData(), fromMillis.getData());
  }

  @Test
  void testRecordIndexPayloadRoundTripsThroughAvro() throws Exception {
    // both fileId encodings populate the numeric RLI fields; they must survive the avro read path
    // (constructMetadataPayload now reads the long/int fields directly instead of via toString+parse)
    assertRecordIndexRoundTrips("49b8b3c8-9e5d-4731-9d51-a2d8e9b5c7f3-0", 0);
    assertRecordIndexRoundTrips("some-raw-file-id", 1);
  }

  private static void assertRecordIndexRoundTrips(String fileId, int fileIdEncoding) throws Exception {
    HoodieRecord<HoodieMetadataPayload> written =
        HoodieMetadataPayload.createRecordIndexUpdate("rk1", "p1", fileId, "20260610153045678", fileIdEncoding);
    // serialize to avro bytes and back so the read path sees a GenericRecord with boxed Long/Integer fields
    byte[] bytes = HoodieAvroUtils.avroToBytes(written.getData().getInsertValue(null).get());
    GenericRecord deserialized = HoodieAvroUtils.bytesToAvro(bytes, HoodieMetadataRecord.getClassSchema());
    HoodieMetadataPayload readBack = new HoodieMetadataPayload(Option.of(deserialized));
    assertEquals(written.getData().recordIndexMetadata, readBack.recordIndexMetadata,
        "RLI metadata must survive the avro read path for fileId encoding " + fileIdEncoding);
  }

  @Test
  void testGetLocationFromRecordIndexInfoFormatsInstantConsistently() {
    long instantMillis1 = HoodieMetadataPayload.parseRecordIndexInstantTime("20260610153045678");
    long instantMillis2 = HoodieMetadataPayload.parseRecordIndexInstantTime("20260610163045678");
    String expected1 = HoodieInstantTimeGenerator.formatDate(new Date(instantMillis1));
    String expected2 = HoodieInstantTimeGenerator.formatDate(new Date(instantMillis2));
    // repeated and alternating instants must format consistently
    for (long instantMillis : new long[] {instantMillis1, instantMillis1, instantMillis2, instantMillis1}) {
      HoodieRecordGlobalLocation location = HoodieTableMetadataUtil.getLocationFromRecordIndexInfo(
          "p1", 1, -1L, -1L, -1, "some-raw-file-id", instantMillis);
      assertEquals(instantMillis == instantMillis1 ? expected1 : expected2, location.getInstantTime());
      assertEquals("p1", location.getPartitionPath());
      assertEquals("some-raw-file-id", location.getFileId());
    }

    // formatDate follows the JVM default time zone, so the decoded location must track a zone
    // change; the two switches differ in offset, so at least one changes the formatted string
    TimeZone originalTimeZone = TimeZone.getDefault();
    try {
      for (String zoneId : new String[] {"UTC", "Asia/Kolkata"}) {
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
        String expectedInZone = HoodieInstantTimeGenerator.formatDate(new Date(instantMillis1));
        HoodieRecordGlobalLocation location = HoodieTableMetadataUtil.getLocationFromRecordIndexInfo(
            "p1", 1, -1L, -1L, -1, "some-raw-file-id", instantMillis1);
        assertEquals(expectedInZone, location.getInstantTime());
      }
    } finally {
      TimeZone.setDefault(originalTimeZone);
    }
  }
}

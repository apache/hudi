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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getIndexVersionOption;
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.constructSecondaryIndexKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
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

  @Test
  void testColumnStatsValueValidation() {
    assertFalse(HoodieTableMetadataUtil.getColumnStatsValueAsString(null).isPresent());
    assertThrows(HoodieNotSupportedException.class,
        () -> HoodieTableMetadataUtil.getColumnStatsValueAsString(new Object()));
  }

  @Test
  void testWritePartitionPathsIncludeNonPartitionedTableIdentifier() {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addWriteStat("", new HoodieWriteStat());
    commitMetadata.addWriteStat("year=2026", new HoodieWriteStat());

    assertEquals(
        new java.util.HashSet<>(Arrays.asList("", "year=2026")),
        HoodieTableMetadataUtil.getWritePartitionPaths(Collections.singletonList(commitMetadata)));
  }

  @Test
  void testDecimalUpcastValidation() {
    assertThrows(AvroTypeException.class,
        () -> HoodieTableMetadataUtil.tryUpcastDecimal(
            new BigDecimal("1.23"), LogicalTypes.decimal(5, 1)));
    assertThrows(AvroTypeException.class,
        () -> HoodieTableMetadataUtil.tryUpcastDecimal(
            new BigDecimal("123"), LogicalTypes.decimal(3, 1)));
    assertThrows(AvroTypeException.class,
        () -> HoodieTableMetadataUtil.tryUpcastDecimal(
            new BigDecimal("1234"), LogicalTypes.decimal(3, 0)));
  }

  @Test
  void testComparableCoercion() {
    assertNull(HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.INT), null));
    assertEquals(1, HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.INT), true));
    assertEquals(0L, HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.LONG), false));
    assertEquals(1.5f, HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.FLOAT), 1.5d));
    assertEquals(2.5d, HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.DOUBLE), 2.5f));
    assertEquals(1.0f, HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.FLOAT), true));
    assertEquals(0.0d, HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.DOUBLE), false));
    assertNull(HoodieTableMetadataUtil.coerceToComparable(
        HoodieSchema.create(HoodieSchemaType.NULL), "ignored"));
  }

  @Test
  void testFileGroupCountBoundsAndInflightWriteStatusTracking() {
    assertEquals(10, HoodieTableMetadataUtil.estimateFileGroupCount(
        MetadataPartitionType.RECORD_INDEX, () -> 10_000L, 1, 1, 10, 1.0f, 100));
    assertEquals(5, HoodieTableMetadataUtil.estimateFileGroupCount(
        MetadataPartitionType.RECORD_INDEX, () -> 500L, 1, 2, 10, 1.0f, 100));

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)).thenReturn(false);
    when(tableConfig.getMetadataPartitionsInflight())
        .thenReturn(Collections.singleton(MetadataPartitionType.RECORD_INDEX.getPartitionPath()));

    assertTrue(HoodieTableMetadataUtil.getMetadataPartitionsNeedingWriteStatusTracking(
        HoodieMetadataConfig.newBuilder().enable(false).build(), metaClient));
  }

  @Test
  @SuppressWarnings("deprecation")
  void testGenerateKeyPrefixesMatchesRawKeyEncoding() {
    List<String> columns = Arrays.asList("c1", "c2");
    assertEquals(
        HoodieTableMetadataUtil.generateColumnStatsKeys(columns, "partition").stream()
            .map(ColumnStatsIndexPrefixRawKey::encode)
            .collect(java.util.stream.Collectors.toList()),
        HoodieTableMetadataUtil.generateKeyPrefixes(columns, "partition"));
  }

  @Test
  void testCollectColumnRangeMetadata() {
    HoodieSchema recordSchema = mock(HoodieSchema.class);
    StorageConfiguration<?> storageConfig = mock(StorageConfiguration.class);
    when(storageConfig.getString(
        org.apache.hudi.common.config.HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(),
        org.apache.hudi.common.config.HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()))
        .thenReturn("UTC");

    HoodieRecord<?> record = mock(HoodieRecord.class);
    when(record.getRecordType()).thenReturn(HoodieRecordType.FLINK);
    when(record.getColumnValueAsJava(
        org.mockito.ArgumentMatchers.eq(recordSchema),
        org.mockito.ArgumentMatchers.eq("id"),
        org.mockito.ArgumentMatchers.any()))
        .thenReturn(7);

    HoodieSchemaField idField = HoodieSchemaField.of(
        "id", HoodieSchema.create(HoodieSchemaType.INT), null, null);
    HoodieSchemaField unsupportedField = HoodieSchemaField.of(
        "attributes",
        HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING)),
        null,
        null);
    Map<String, HoodieColumnRangeMetadata<Comparable>> stats =
        HoodieTableMetadataUtil.collectColumnRangeMetadata(
            Collections.<HoodieRecord>singletonList(record).iterator(),
            Arrays.asList(Pair.of("id", idField), Pair.of("attributes", unsupportedField)),
            "file.parquet",
            recordSchema,
            storageConfig,
            HoodieIndexVersion.V1);

    assertEquals(7, stats.get("id").getMinValue());
    assertEquals(7, stats.get("id").getMaxValue());
    assertNull(stats.get("attributes").getMinValue());
  }

  @Test
  void testBloomAndColumnStatsConversionFast() {
    HoodieLocalEngineContext engineContext =
        new HoodieLocalEngineContext(mock(StorageConfiguration.class));
    Map<String, List<String>> deletedFiles = new HashMap<>();
    deletedFiles.put("p1", Arrays.asList("file.log.1", "file_1-0-1_001.parquet"));

    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToBloomFilterRecords(
        engineContext,
        deletedFiles,
        Collections.emptyMap(),
        "001",
        mock(HoodieTableMetaClient.class),
        2,
        "SIMPLE");
    assertEquals(1, records.collectAsList().size());

    // release-1.2.1 takes an extra HoodieMetadataConfig after the meta client; master dropped
    // that parameter, so pass a default config here.
    assertTrue(HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
        engineContext,
        Collections.emptyMap(),
        Collections.emptyMap(),
        mock(HoodieTableMetaClient.class),
        HoodieMetadataConfig.newBuilder().build(),
        1,
        1024,
        Collections.singletonList("id")).collectAsList().isEmpty());
  }

  @Test
  void testFilesPartitionAvailability() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions())
        .thenReturn(Collections.singleton(HoodieTableMetadataUtil.PARTITION_NAME_FILES));

    assertTrue(HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient));
  }

  @Test
  void testMetadataTableDeletionOutcomes() throws Exception {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getStorage()).thenReturn(storage);

    when(storage.exists(any(StoragePath.class))).thenReturn(false);
    assertNull(HoodieTableMetadataUtil.deleteMetadataTable(metaClient, null, false));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenThrow(new FileNotFoundException("missing"));
    assertNull(HoodieTableMetadataUtil.deleteMetadataTable(metaClient, null, false));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenThrow(new IOException("check failed"));
    assertThrows(HoodieMetadataException.class,
        () -> HoodieTableMetadataUtil.deleteMetadataTable(metaClient, null, false));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenReturn(true);
    when(storage.rename(any(StoragePath.class), any(StoragePath.class))).thenReturn(true);
    assertTrue(HoodieTableMetadataUtil.deleteMetadataTable(metaClient, null, true)
        .contains(".metadata_"));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenReturn(true);
    when(storage.rename(any(StoragePath.class), any(StoragePath.class)))
        .thenThrow(new IOException("rename failed"));
    assertNull(HoodieTableMetadataUtil.deleteMetadataTable(metaClient, null, true));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenReturn(true);
    org.mockito.Mockito.doThrow(new IOException("delete failed"))
        .when(storage).deleteDirectory(any(StoragePath.class));
    assertThrows(HoodieMetadataException.class,
        () -> HoodieTableMetadataUtil.deleteMetadataTable(metaClient, null, false));
  }

  @Test
  void testMetadataPartitionDeletionOutcomes() throws Exception {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getStorage()).thenReturn(storage);
    String partition = MetadataPartitionType.COLUMN_STATS.getPartitionPath();

    when(storage.exists(any(StoragePath.class))).thenReturn(false);
    assertNull(HoodieTableMetadataUtil.deleteMetadataTablePartition(
        metaClient, null, MetadataPartitionType.FILES.getPartitionPath(), false));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenThrow(new FileNotFoundException("missing"));
    assertNull(HoodieTableMetadataUtil.deleteMetadataTablePartition(
        metaClient, null, partition, false));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenThrow(new IOException("check failed"));
    assertThrows(HoodieMetadataException.class,
        () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(
            metaClient, null, partition, false));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenReturn(true);
    when(storage.rename(any(StoragePath.class), any(StoragePath.class))).thenReturn(true);
    assertTrue(HoodieTableMetadataUtil.deleteMetadataTablePartition(
        metaClient, null, partition, true).contains(".metadata_"));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenReturn(true);
    when(storage.rename(any(StoragePath.class), any(StoragePath.class)))
        .thenThrow(new IOException("rename failed"));
    assertNull(HoodieTableMetadataUtil.deleteMetadataTablePartition(
        metaClient, null, partition, true));

    reset(storage);
    when(storage.exists(any(StoragePath.class))).thenReturn(true);
    org.mockito.Mockito.doThrow(new IOException("delete failed"))
        .when(storage).deleteDirectory(any(StoragePath.class));
    assertThrows(HoodieMetadataException.class,
        () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(
            metaClient, null, partition, false));
  }

  @Test
  void testRecordKeyReadSchemaFailureIsWrapped() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
    when(metaClient.getStorageConf()).thenReturn(mock(StorageConfiguration.class));

    assertThrows(org.apache.hudi.exception.HoodieException.class,
        () -> HoodieTableMetadataUtil.readRecordKeysFromFileSlices(
            engineContext,
            Collections.singletonList(Pair.of("partition", mock(FileSlice.class))),
            1,
            "test",
            metaClient,
            false));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPartitionStatsConversionFailureIsWrapped() {
    HoodiePairData<String, List<HoodieColumnRangeMetadata<Comparable>>> pairData =
        mock(HoodiePairData.class);
    when(pairData.flatMapValues(any())).thenThrow(new RuntimeException("conversion failed"));

    assertThrows(org.apache.hudi.exception.HoodieException.class,
        () -> HoodieTableMetadataUtil.convertMetadataToPartitionStatsRecords(
            pairData,
            mock(HoodieTableMetaClient.class),
            Collections.emptyMap(),
            HoodieIndexVersion.V1));
  }

  @Test
  void testMergeColumnStatsTombstoneWins() {
    HoodieMetadataColumnStats previous = HoodieMetadataColumnStats.newBuilder()
        .setColumnName("column")
        .setIsDeleted(false)
        .build();
    HoodieMetadataColumnStats tombstone = HoodieMetadataColumnStats.newBuilder()
        .setColumnName("column")
        .setIsDeleted(true)
        .build();

    assertEquals(tombstone, HoodieTableMetadataUtil.mergeColumnStatsRecords(previous, tombstone));
    assertEquals(previous, HoodieTableMetadataUtil.mergeColumnStatsRecords(tombstone, previous));
  }

  @Test
  void testFileSliceAndSchemaResolutionEdge() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(timeline.countInstants()).thenReturn(1);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
    assertThrows(org.apache.hudi.exception.HoodieException.class,
        () -> HoodieTableMetadataUtil.tryResolveSchemaForTable(metaClient));

    HoodieTableFileSystemView fileSystemView = mock(HoodieTableFileSystemView.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterCompletedInstants()).thenReturn(activeTimeline);
    when(activeTimeline.lastInstant()).thenReturn(Option.empty());
    assertTrue(HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(
        metaClient, fileSystemView, "files").isEmpty());
  }

  @Test
  void testEmptyLogInputsAvoidReaderConstruction() {
    Pair<java.util.Set<String>, java.util.Set<String>> changes =
        HoodieTableMetadataUtil.getRevivedAndDeletedKeysFromMergedLogs(
            mock(HoodieTableMetaClient.class),
            "001",
            Collections.singletonList("previous.log"),
            Option.empty(),
            Collections.singletonList("current.log"),
            "partition",
            mock(org.apache.hudi.common.engine.HoodieReaderContext.class));

    assertTrue(changes.getLeft().isEmpty());
    assertTrue(changes.getRight().isEmpty());
  }

  @Test
  void testRollbackPlanFallbackAndReadFailure() throws Exception {
    Method method = HoodieTableMetadataUtil.class.getDeclaredMethod(
        "getRollbackedCommits",
        HoodieInstant.class,
        HoodieActiveTimeline.class,
        InstantGenerator.class);
    method.setAccessible(true);

    HoodieInstant completed = mock(HoodieInstant.class);
    HoodieInstant requested = mock(HoodieInstant.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    InstantGenerator instantGenerator = mock(InstantGenerator.class);
    HoodieRollbackPlan rollbackPlan = mock(HoodieRollbackPlan.class);
    HoodieInstantInfo instantInfo = mock(HoodieInstantInfo.class);
    when(completed.getAction()).thenReturn(HoodieTimeline.ROLLBACK_ACTION);
    when(completed.requestedTime()).thenReturn("002");
    when(timeline.readRollbackMetadata(completed)).thenThrow(new IOException("empty rollback"));
    when(instantGenerator.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, "002"))
        .thenReturn(requested);
    when(timeline.readRollbackPlan(requested)).thenReturn(rollbackPlan);
    when(rollbackPlan.getInstantToRollback()).thenReturn(instantInfo);
    when(instantInfo.getCommitTime()).thenReturn("001");
    assertEquals(Collections.singletonList("001"), method.invoke(
        null, completed, timeline, instantGenerator));

    when(completed.getAction()).thenReturn(HoodieTimeline.RESTORE_ACTION);
    when(timeline.readRestoreMetadata(completed)).thenThrow(new IOException("broken restore"));
    InvocationTargetException exception = assertThrows(
        InvocationTargetException.class,
        () -> method.invoke(null, completed, timeline, instantGenerator));
    assertTrue(exception.getCause() instanceof HoodieMetadataException);
  }

  @Test
  void testMetadataPartitionExistenceFailureIsWrapped() throws Exception {
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    org.mockito.Mockito.doReturn(storageConfiguration).when(context).getStorageConf();
    when(storage.exists(any(StoragePath.class))).thenThrow(new IOException("failed"));

    try (MockedStatic<HoodieStorageUtils> storageUtils = mockStatic(HoodieStorageUtils.class)) {
      storageUtils.when(() -> HoodieStorageUtils.getStorage(any(String.class), any()))
          .thenReturn(storage);
      assertThrows(org.apache.hudi.exception.HoodieIOException.class,
          () -> HoodieTableMetadataUtil.metadataPartitionExists(
              "/table", context, MetadataPartitionType.FILES.getPartitionPath()));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testDeletedFileStatsCreateStubs() throws Exception {
    Method method = HoodieTableMetadataUtil.class.getDeclaredMethod(
        "getFileStatsRangeMetadata",
        String.class,
        String.class,
        HoodieTableMetaClient.class,
        List.class,
        boolean.class,
        int.class,
        HoodieIndexVersion.class);
    method.setAccessible(true);

    List<HoodieColumnRangeMetadata<Comparable>> stats =
        (List<HoodieColumnRangeMetadata<Comparable>>) method.invoke(
            null,
            "partition",
            "file.parquet",
            mock(HoodieTableMetaClient.class),
            Arrays.asList("c1", "c2"),
            true,
            1024,
            HoodieIndexVersion.V1);
    assertEquals(2, stats.size());
  }

  @Test
  void testCommitPartitionExtraction() throws Exception {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addWriteStat("", new HoodieWriteStat());
    commitMetadata.addWriteStat("partition", new HoodieWriteStat());
    Method method = HoodieTableMetadataUtil.class.getDeclaredMethod(
        "getPartitionsAdded", HoodieCommitMetadata.class);
    method.setAccessible(true);

    assertEquals(
        new java.util.HashSet<>(Arrays.asList(HoodieTableMetadata.NON_PARTITIONED_NAME, "partition")),
        new java.util.HashSet<>((List<String>) method.invoke(null, commitMetadata)));
  }

  @Test
  void testInflightFileSliceViewIsClosedWhenCreatedInternally() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableFileSystemView fileSystemView = mock(HoodieTableFileSystemView.class);
    when(fileSystemView.getLatestFileSlicesIncludingInflight("files"))
        .thenReturn(Stream.empty());

    try (MockedStatic<HoodieTableMetadataUtil> util =
             mockStatic(HoodieTableMetadataUtil.class, org.mockito.Answers.CALLS_REAL_METHODS)) {
      util.when(() -> HoodieTableMetadataUtil.getFileSystemViewForMetadataTable(metaClient))
          .thenReturn(fileSystemView);
      assertTrue(HoodieTableMetadataUtil.getPartitionLatestFileSlicesIncludingInflight(
          metaClient, Option.empty(), "files").isEmpty());
    }

    verify(fileSystemView).close();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void testLogOnlyFileSliceRecordKeys() throws Exception {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieSchema schema = mock(HoodieSchema.class);
    FileSlice fileSlice = mock(FileSlice.class);
    List<Pair<String, FileSlice>> slices =
        Collections.singletonList(Pair.of("partition", fileSlice));
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    when(storageConfiguration.getEnum(
        any(String.class), any(ExternalSpillableMap.DiskMapType.class)))
        .thenAnswer(invocation -> invocation.getArgument(1));
    org.mockito.Mockito.doReturn(storageConfiguration).when(metaClient).getStorageConf();
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(timeline.lastInstant()).thenReturn(Option.empty());
    when(fileSlice.getBaseFile()).thenReturn(Option.empty());
    when(fileSlice.getLogFiles()).thenReturn(Stream.empty());
    when(fileSlice.getPartitionPath()).thenReturn("partition");
    when(fileSlice.getFileId()).thenReturn("file-id");
    when(fileSlice.getBaseInstantTime()).thenReturn("20240101000000000");
    org.mockito.Mockito.doReturn(HoodieListData.eager(slices))
        .when(engineContext).parallelize(anyList(), anyInt());

    ReaderContextFactory readerContextFactory = mock(ReaderContextFactory.class);
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    org.mockito.Mockito.doReturn(readerContextFactory)
        .when(engineContext).getReaderContextFactory(metaClient);
    when(readerContextFactory.getContext()).thenReturn(readerContext);

    HoodieFileGroupReader.HoodieFileGroupReaderBuilder builder =
        mock(HoodieFileGroupReader.HoodieFileGroupReaderBuilder.class);
    HoodieFileGroupReader fileGroupReader = mock(HoodieFileGroupReader.class);
    when(builder.withReaderContext(any())).thenReturn(builder);
    when(builder.withHoodieTableMetaClient(any())).thenReturn(builder);
    when(builder.withBaseFileOption(any())).thenReturn(builder);
    when(builder.withLogFiles(any())).thenReturn(builder);
    when(builder.withPartitionPath(any())).thenReturn(builder);
    when(builder.withDataSchema(any())).thenReturn(builder);
    when(builder.withRequestedSchema(any())).thenReturn(builder);
    when(builder.withLatestCommitTime(any())).thenReturn(builder);
    when(builder.withProps(any())).thenReturn(builder);
    when(builder.build()).thenReturn(fileGroupReader);
    when(fileGroupReader.getClosableKeyIterator())
        .thenReturn(ClosableIterator.wrap(Collections.emptyIterator()));

    try (MockedConstruction<TableSchemaResolver> ignored =
             mockConstruction(TableSchemaResolver.class,
                 (resolver, context) -> when(resolver.getTableSchema()).thenReturn(schema));
         MockedStatic<HoodieFileGroupReader> readerStatic =
             mockStatic(HoodieFileGroupReader.class)) {
      readerStatic.when(HoodieFileGroupReader::builder).thenReturn(builder);
      assertTrue(HoodieTableMetadataUtil.readRecordKeysFromFileSlices(
          engineContext, slices, 1, "test", metaClient, false).collectAsList().isEmpty());
    }
  }
}

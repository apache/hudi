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

import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}

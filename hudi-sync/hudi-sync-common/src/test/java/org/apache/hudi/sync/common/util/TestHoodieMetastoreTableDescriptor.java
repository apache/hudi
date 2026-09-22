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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieMetastoreTableDescriptor {

  private static final String BASE_PATH = "file:///tmp/trips";

  private static HoodieSchema schema() {
    HoodieSchema record = HoodieSchema.createRecord("trips", "hoodie.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("fare", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
        HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING))));
    return HoodieSchemaUtils.addMetadataFields(record);
  }

  private static List<String> names(List<HoodieSchemaField> fields) {
    return fields.stream().map(HoodieSchemaField::name).collect(Collectors.toList());
  }

  @Test
  void copyOnWriteUsesTheNonRealtimeInputFormat() {
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);

    assertEquals(HoodieMetastoreTableDescriptor.PARQUET_INPUT_FORMAT_CLASS, descriptor.getInputFormatClassName());
    assertEquals(HoodieMetastoreTableDescriptor.PARQUET_OUTPUT_FORMAT_CLASS, descriptor.getOutputFormatClassName());
    assertEquals(HoodieMetastoreTableDescriptor.PARQUET_SERDE_CLASS, descriptor.getSerdeClassName());
  }

  @Test
  void mergeOnReadSnapshotViewUsesTheRealtimeInputFormat() {
    // The snapshot view of a MOR table is the real-time one, and forSnapshotView has to derive that
    // from the table type. Getting it backwards does not fail: the table registers with the
    // Copy-on-Write input format and every reader then silently ignores its log files.
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.MERGE_ON_READ, BASE_PATH, true);

    assertEquals(HoodieMetastoreTableDescriptor.PARQUET_REALTIME_INPUT_FORMAT_CLASS, descriptor.getInputFormatClassName());
  }

  @Test
  void copyOnWriteCannotUseTheRealtimeInputFormat() {
    assertThrows(IllegalArgumentException.class, () -> HoodieMetastoreTableDescriptor.forView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH,
        HoodieMetastoreTableDescriptor.ViewOptions.builder()
            .setExternal(true)
            .setUseRealtimeInputFormat(true)
            .build()));
  }

  @Test
  void theReadOptimizedViewRecordsItselfAsSuch() {
    // The _ro registration hive-sync performs for MOR: non-realtime input format, but flagged as the
    // read-optimized view. Trino does not create these; the general form still has to express it.
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forView(
        schema(), Collections.emptyList(), HoodieTableType.MERGE_ON_READ, BASE_PATH,
        HoodieMetastoreTableDescriptor.ViewOptions.builder()
            .setExternal(true)
            .setReadAsOptimized(true)
            .build());

    assertEquals(HoodieMetastoreTableDescriptor.PARQUET_INPUT_FORMAT_CLASS, descriptor.getInputFormatClassName());
    assertEquals("true", descriptor.getSerdeParameters().get("hoodie.query.as.ro.table"));
  }

  @Test
  void metaFieldsKeepTheirLeadingPositionsAmongTheDataColumns() {
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);

    List<String> dataColumns = names(descriptor.getDataFields());
    assertEquals("_hoodie_commit_time", dataColumns.get(0));
    assertTrue(dataColumns.indexOf("_hoodie_record_key") < dataColumns.indexOf("id"),
        "meta fields must precede the declared columns: " + dataColumns);
    assertTrue(descriptor.getPartitionFields().isEmpty());
  }

  @Test
  void partitionColumnsAreSplitOutAndKeepTheGivenOrder() {
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Arrays.asList("city", "id"), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);

    // Partition-path order, not schema order: the schema has id before city.
    assertEquals(Arrays.asList("city", "id"), names(descriptor.getPartitionFields()));
    assertFalse(names(descriptor.getDataFields()).contains("city"));
    assertFalse(names(descriptor.getDataFields()).contains("id"));
    assertTrue(names(descriptor.getDataFields()).contains("fare"));
  }

  @Test
  void partitionColumnMissingFromTheSchemaIsTypedAsString() {
    // Matches HiveSchemaUtil#getPartitionKeyType. Tables built with some key generators do not carry
    // their partition fields in the schema, and register_table has to cope with those.
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.singletonList("datestr"), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);

    assertEquals(Collections.singletonList("datestr"), names(descriptor.getPartitionFields()));
    assertEquals(HoodieSchemaType.STRING, descriptor.getPartitionFields().get(0).schema().getType());
    // Nothing was removed from the data columns, since the name was not among them.
    assertTrue(names(descriptor.getDataFields()).containsAll(Arrays.asList("id", "fare", "city")));
  }

  @Test
  void repeatedPartitionColumnIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Arrays.asList("city", "city"), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true));
  }

  @Test
  void serializationFormatGoesInTheSerdeParametersNotTheTableParameters() {
    // HMSDDLExecutor puts it in the serde properties. A table that carries it as a table parameter
    // instead differs from every hive-synced table, without failing visibly.
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);

    assertEquals("1", descriptor.getSerdeParameters().get(HoodieMetastoreTableDescriptor.SERIALIZATION_FORMAT_PARAMETER));
    assertFalse(descriptor.getTableParameters().containsKey(HoodieMetastoreTableDescriptor.SERIALIZATION_FORMAT_PARAMETER));
  }

  @Test
  void serdeParametersCarryThePathAndReadOptimizedFlag() {
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.MERGE_ON_READ, BASE_PATH, true);

    assertEquals(BASE_PATH, descriptor.getSerdeParameters().get("path"));
    // The snapshot view is not the read-optimized one.
    assertEquals("false", descriptor.getSerdeParameters().get("hoodie.query.as.ro.table"));
  }

  @Test
  void sparkDatasourcePropertiesArePresentSoSparkSqlRecognisesTheTable() {
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.singletonList("city"), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);

    // Spark SQL dispatches on the provider; without it the table is not a Hudi datasource table.
    assertEquals("hudi", descriptor.getTableParameters().get("spark.sql.sources.provider"));
    assertEquals("1", descriptor.getTableParameters().get("spark.sql.sources.schema.numPartCols"));
    assertEquals("city", descriptor.getTableParameters().get("spark.sql.sources.schema.partCol.0"));

    String reassembled = reassembleSparkSchema(descriptor);
    assertTrue(reassembled.contains("_hoodie_commit_time"), reassembled);
    assertTrue(reassembled.contains("\"id\""), reassembled);
    assertTrue(reassembled.contains("\"city\""), reassembled);
  }

  @Test
  void anEmptySparkVersionOmitsTheCreateVersionProperty() {
    // hoodie.meta_sync.spark.version defaults to empty, so a hive-synced table has no
    // spark.sql.create.version either. Trino is not Spark and has no version to claim.
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);

    assertFalse(descriptor.getTableParameters().containsKey("spark.sql.create.version"));
  }

  @Test
  void externalIsRecordedOnlyWhenTheTableIsExternal() {
    HoodieMetastoreTableDescriptor external = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH, true);
    assertTrue(external.isExternal());
    assertEquals("TRUE", external.getTableParameters().get(HoodieMetastoreTableDescriptor.EXTERNAL_PARAMETER));

    HoodieMetastoreTableDescriptor managed = HoodieMetastoreTableDescriptor.forSnapshotView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH, false);
    assertFalse(managed.isExternal());
    assertFalse(managed.getTableParameters().containsKey(HoodieMetastoreTableDescriptor.EXTERNAL_PARAMETER));
  }

  @Test
  void extraTableParametersAreAppliedLast() {
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH,
        HoodieMetastoreTableDescriptor.ViewOptions.builder()
            .setExternal(true)
            .setExtraTableParameters(Collections.singletonMap("spark.sql.sources.provider", "overridden"))
            .build());

    assertEquals("overridden", descriptor.getTableParameters().get("spark.sql.sources.provider"));
  }

  @Test
  void theSchemaIsSplitIntoPartsOfTheGivenThreshold() {
    HoodieMetastoreTableDescriptor descriptor = HoodieMetastoreTableDescriptor.forView(
        schema(), Collections.emptyList(), HoodieTableType.COPY_ON_WRITE, BASE_PATH,
        HoodieMetastoreTableDescriptor.ViewOptions.builder()
            .setExternal(true)
            .setSchemaStringLengthThreshold(16)
            .build());

    int numParts = Integer.parseInt(descriptor.getTableParameters().get("spark.sql.sources.schema.numParts"));
    assertTrue(numParts > 1, "a 16-character threshold should split the schema");
    // Every declared part is present, so a reader concatenating 0..numParts-1 loses nothing.
    for (int i = 0; i < numParts; i++) {
      assertTrue(descriptor.getTableParameters().containsKey("spark.sql.sources.schema.part." + i),
          "missing part " + i);
    }
  }

  private static String reassembleSparkSchema(HoodieMetastoreTableDescriptor descriptor) {
    int numParts = Integer.parseInt(descriptor.getTableParameters().get("spark.sql.sources.schema.numParts"));
    StringBuilder schema = new StringBuilder();
    for (int i = 0; i < numParts; i++) {
      schema.append(descriptor.getTableParameters().get("spark.sql.sources.schema.part." + i));
    }
    return schema.toString();
  }
}

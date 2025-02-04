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

package org.apache.hudi.common.table;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getMetaClientBuilder;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TableSchemaResolver}.
 */
public class TestTableSchemaResolver2 extends HoodieCommonTestHarness {

  HoodieTestTable testTable;

  Schema originalSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

  private final Map<String, String> extraMetadataWithSchema = Collections.singletonMap(
      HoodieCommitMetadata.SCHEMA_KEY, originalSchema.toString());

  /**
   * Pads a string number with leading zeros until it reaches the specified length.
   *
   * @param number The string number to pad
   * @param length The desired total length after padding
   * @return The padded string number
   * @throws IllegalArgumentException if the input number is longer than the desired length
   */
  public static String padWithLeadingZeros(String number, int length) {
    if (number == null) {
      throw new IllegalArgumentException("Input number cannot be null");
    }
    if (number.length() > length) {
      throw new IllegalArgumentException("Input number length " + number.length()
          + " is greater than desired length " + length);
    }
    return String.format("%0" + length + "d", Long.parseLong(number));
  }

  @BeforeEach
  public void setUp() throws Exception {
    if (basePath == null) {
      initPath();
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  private static Stream<Arguments> testGetTableSchemaFromLatestCommitMetadataTestDimension() {
    return Stream.of(
      // enableMetadata, tableType, isCommit
      Arguments.of(true, HoodieTableType.COPY_ON_WRITE, "commitOrDeltaCommit"),
      Arguments.of(true, HoodieTableType.COPY_ON_WRITE, "replacementCommit"),
      Arguments.of(true, HoodieTableType.MERGE_ON_READ, "commitOrDeltaCommit"),
      Arguments.of(true, HoodieTableType.MERGE_ON_READ, "replacementCommit"),
      Arguments.of(false, HoodieTableType.COPY_ON_WRITE, "commitOrDeltaCommit"),
      Arguments.of(false, HoodieTableType.COPY_ON_WRITE, "replacementCommit"),
      Arguments.of(false, HoodieTableType.MERGE_ON_READ, "commitOrDeltaCommit"),
      Arguments.of(false, HoodieTableType.MERGE_ON_READ, "replacementCommit")
    );
  }

  // Covers all valid commit instants.
  @ParameterizedTest
  @MethodSource("testGetTableSchemaFromLatestCommitMetadataTestDimension")
  void testGetTableSchemaFromLatestCommitMetadata(boolean enableMetadata, HoodieTableType tableType, String type) throws Exception {
    initMetaClient(enableMetadata, tableType);
    testTable = HoodieTestTable.of(metaClient);

    String commitTime1 = "001";
    if (type.equals("commitOrDeltaCommit")) {
      // Case 1: Regular commit
      if (tableType == HoodieTableType.COPY_ON_WRITE) {
        testTable.addCommit(commitTime1, Option.of(buildMetadata(
            Collections.emptyList(),
            Collections.emptyMap(),
            Option.empty(),
            WriteOperationType.UNKNOWN,
            originalSchema.toString(),
            COMMIT_ACTION)));
      } else {
        testTable.addDeltaCommit(commitTime1, buildMetadata(
            Collections.emptyList(),
            Collections.emptyMap(),
            Option.empty(),
            WriteOperationType.UNKNOWN,
            originalSchema.toString(),
            DELTA_COMMIT_ACTION));
      }
    } else if (type.equals("replacementCommit")) {
      // Case 2: Replacement commit
      HoodieClusteringGroup group = new HoodieClusteringGroup();
      HoodieClusteringPlan plan = new HoodieClusteringPlan(
          Collections.singletonList(group),
          HoodieClusteringStrategy.newBuilder().build(),
          Collections.emptyMap(),
          1,
          false,
          null);
      HoodieRequestedReplaceMetadata requestedMetadata = new HoodieRequestedReplaceMetadata(
          WriteOperationType.UNKNOWN.name(),
          plan,
          Collections.emptyMap(),
          1);
      testTable.addReplaceCommit(commitTime1,
          Option.of(requestedMetadata),
          Option.empty(),
          new HoodieReplaceCommitMetadata(buildMetadata(
              Collections.emptyList(),
              Collections.emptyMap(),
              Option.empty(),
              WriteOperationType.UNKNOWN,
              originalSchema.toString(),
              REPLACE_COMMIT_ACTION)));
    }

    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(false);
    assertTrue(schemaOption.isPresent());
    assertEquals(originalSchema, schemaOption.get());
  }

  private static Stream<Arguments> commonTableConfigTestDimension() {
    return Stream.of(
      // version 6 or 8, tableType
      Arguments.of(true, HoodieTableType.COPY_ON_WRITE),
      Arguments.of(true, HoodieTableType.MERGE_ON_READ),
      Arguments.of(false, HoodieTableType.COPY_ON_WRITE),
      Arguments.of(false, HoodieTableType.MERGE_ON_READ)
    );
  }

  @ParameterizedTest
  @MethodSource("commonTableConfigTestDimension")
  void testGetTableAvroSchemaInternalNoSchemaFoundEmptyTimeline(boolean enableMetadata, HoodieTableType tableType) throws IOException {
    // Don't set any schema in commit metadata or table config
    initMetaClient(enableMetadata, tableType);
    testTable = HoodieTestTable.of(metaClient);
    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(true);
    assertFalse(schemaOption.isPresent());
  }

  // Covers all instants that will be ignored when resolving table schema.
  @ParameterizedTest
  @MethodSource("commonTableConfigTestDimension")
  void testGetTableAvroSchemaInternalNoSchemaFoundDisqualifiedInstant(boolean enableMetadata, HoodieTableType tableType) throws Exception {
    // Don't set any schema in commit metadata or table config
    initMetaClient(enableMetadata, tableType);
    testTable = HoodieTestTable.of(metaClient);
    int startCommitTime = 1;

    createExhaustiveDisqualifiedInstants(startCommitTime, tableType);
    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(true);
    assertFalse(schemaOption.isPresent());
  }

  private int createExhaustiveDisqualifiedInstants(int startCommitTime, HoodieTableType tableType) throws Exception {
    if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
      testTable.addCompaction(padWithLeadingZeros(Integer.toString(startCommitTime), 4),
          buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.COMPACT, originalSchema.toString(), COMPACTION_ACTION));
    }
    startCommitTime += 1;
    // Clean
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""),
        "", "", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
    HoodieCleanMetadata cleanMeta = new HoodieCleanMetadata("", 0L, 0, "20", "",
        Collections.emptyMap(), metaClient.getTableConfig().getTableVersion().versionCode(), Collections.emptyMap(), extraMetadataWithSchema);
    testTable.addClean(padWithLeadingZeros(Integer.toString(startCommitTime), 4), cleanerPlan, cleanMeta);
    startCommitTime += 1;

    // Clustering commit
    HoodieClusteringGroup group = new HoodieClusteringGroup();
    HoodieClusteringPlan plan = new HoodieClusteringPlan(Collections.singletonList(group),
        HoodieClusteringStrategy.newBuilder().build(), Collections.emptyMap(), 1, false, null);
    HoodieRequestedReplaceMetadata requestedMetadata = new HoodieRequestedReplaceMetadata(WriteOperationType.CLUSTER.name(), plan, Collections.emptyMap(), 1);
    testTable.addReplaceCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4), Option.of(requestedMetadata), Option.empty(),
        new HoodieReplaceCommitMetadata(buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UNKNOWN, originalSchema.toString(), CLUSTERING_ACTION)));
    startCommitTime += 1;

    // Inflight commits
    testTable.addInflightCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4));
    startCommitTime += 1;

    testTable.addInflightDeltaCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4));
    startCommitTime += 1;

    // Commits without schema in it.
    testTable.addCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        "",
        COMMIT_ACTION)));
    startCommitTime += 1;

    // Commits without schema in it.
    testTable.addDeltaCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4), buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        "",
        DELTA_COMMIT_ACTION));
    startCommitTime += 1;

    // Savepoint
    HoodieSavepointMetadata savepointMetadata = new HoodieSavepointMetadata();
    savepointMetadata.setSavepointedAt(12345L);
    savepointMetadata.setSavepointedBy("12345");
    savepointMetadata.setComments("12345");
    savepointMetadata.setPartitionMetadata(Collections.emptyMap());
    testTable.addSavepointCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4), savepointMetadata);
    startCommitTime += 1;

    // Rollback
    testTable.addInflightRollback(padWithLeadingZeros(Integer.toString(startCommitTime), 4));
    testTable.addRollback(padWithLeadingZeros(Integer.toString(startCommitTime), 4), new HoodieRollbackMetadata(), new HoodieRollbackPlan());

    return startCommitTime;
  }

  private static Stream<Arguments> testGetTableAvroSchemaInternalFromTableConfigParams() {
    return Stream.of(
      Arguments.of(true, true),   // includeMetadata=true, expectMetadataFields=true
      Arguments.of(false, false)  // includeMetadata=false, expectMetadataFields=false
    );
  }

  @ParameterizedTest
  @MethodSource("testGetTableAvroSchemaInternalFromTableConfigParams")
  void testGetTableAvroSchemaInternalFromTableConfig(boolean includeMetadata, boolean expectMetadataFields) throws IOException {
    metaClient = getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), null)
        .setTableVersion(HoodieTableVersion.SIX)
        .setTableCreateSchema(new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).toString())
        .initTable(getDefaultStorageConf(), basePath);
    testTable = HoodieTestTable.of(metaClient);

    // Set schema in table config
    metaClient.getTableConfig().setValue(HoodieTableConfig.CREATE_SCHEMA, originalSchema.toString());
    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);

    Option<Schema> schema = resolver.getTableAvroSchemaIfPresent(includeMetadata);
    assertTrue(schema.isPresent());
    
    if (expectMetadataFields) {
      assertNotNull(schema.get().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD));
    } else {
      assertNull(schema.get().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD));
    }
  }

  @Test
  public void testGetTableAvroSchemaInternalWithPartitionFields() throws IOException {
    initMetaClient(false, HoodieTableType.COPY_ON_WRITE);
    testTable = HoodieTestTable.of(metaClient);
    // Setup table config with partition fields
    String[] partitionFields = new String[] {"partition_path"};
    metaClient.getTableConfig().setValue(PARTITION_FIELDS, String.join(",", partitionFields));
    metaClient.getTableConfig().setValue(HoodieTableConfig.DROP_PARTITION_COLUMNS, "true");
    metaClient.getTableConfig().setValue(HoodieTableConfig.CREATE_SCHEMA, originalSchema.toString());
    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(true);

    assertTrue(schemaOption.isPresent());
    Schema resultSchema = schemaOption.get();
    assertTrue(resultSchema.getFields().stream()
        .anyMatch(f -> f.name().equals("partition_path")));
  }

  @ParameterizedTest
  @MethodSource("commonTableConfigTestDimension")
  void testGetTableAvroSchemaInternalWithSpecificInstant(boolean preTableVersion8, HoodieTableType tableType) throws Exception {
    initMetaClient(preTableVersion8, tableType);
    testTable = HoodieTestTable.of(metaClient);

    Schema schema1 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Schema schema2 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_SCHEMA);

    // Create two commits with different schemas
    int startCommitTime = 1;
    // First commit with schema1
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      testTable.addCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4),
          Option.of(buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UNKNOWN, schema1.toString(), COMMIT_ACTION)));
    } else {
      testTable.addDeltaCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4), buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
          WriteOperationType.UNKNOWN, schema1.toString(), DELTA_COMMIT_ACTION));
    }
    startCommitTime += 1;

    // Second commit with schema2
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      testTable.addCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4), Option.of(buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
          WriteOperationType.UNKNOWN, schema2.toString(), COMMIT_ACTION)));
    } else {
      testTable.addDeltaCommit(padWithLeadingZeros(Integer.toString(startCommitTime), 4), buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
          WriteOperationType.UNKNOWN, schema2.toString(), DELTA_COMMIT_ACTION));
    }
    startCommitTime += 1;

    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);

    // Test getting schema from first instant
    Option<Schema> schema1Option = resolver.getTableAvroSchemaIfPresent(false, Option.of(Integer.toString(1)));
    assertTrue(schema1Option.isPresent());
    assertEquals(schema1.toString(), schema1Option.get().toString());

    // Test getting schema from second instant
    Option<Schema> schema2Option = resolver.getTableAvroSchemaIfPresent(false, Option.of(Integer.toString(2)));
    assertTrue(schema2Option.isPresent());
    assertEquals(schema2.toString(), schema2Option.get().toString());

    // Now follow with more disqualified instants and try to get table schema with their request time, we should back track to instant 2.
    int endCommitTime = createExhaustiveDisqualifiedInstants(startCommitTime, tableType);
    metaClient.reloadActiveTimeline();

    for (int i = startCommitTime + 1; i <= endCommitTime + 1; i++) {
      schema2Option = resolver.getTableAvroSchemaIfPresent(false, Option.of(Integer.toString(i)));
      assertTrue(schema2Option.isPresent());
      assertEquals(schema2.toString(), schema2Option.get().toString());
    }
  }
}

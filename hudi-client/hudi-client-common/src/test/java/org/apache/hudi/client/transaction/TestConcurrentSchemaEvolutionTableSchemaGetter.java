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

package org.apache.hudi.client.transaction;

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
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.FileCreateUtils.createDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedDeltaCommit;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link ConcurrentSchemaEvolutionTableSchemaGetter}.
 */
public class TestConcurrentSchemaEvolutionTableSchemaGetter extends HoodieCommonTestHarness {

  public static final int REQUEST_TIME_LENGTH = 4;
  HoodieTestTable testTable;

  private static final String SCHEMA_WITHOUT_METADATA_STR = "{\n"
      + "  \"namespace\": \"example.avro\",\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"User\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"timestamp\", \"type\": \"long\"},\n"
      + "    {\"name\": \"_row_key\", \"type\": \"string\"},\n"
      + "    {\"name\": \"rider\", \"type\": \"string\"},\n"
      + "    {\"name\": \"driver\", \"type\": \"string\"}\n"
      + "  ]\n"
      + "}";

  private static final String SCHEMA_WITHOUT_METADATA_STR2 = "{\n"
      + "  \"namespace\": \"example.avro\",\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"User\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"timestamp\", \"type\": \"long\"},\n"
      + "    {\"name\": \"_row_key\", \"type\": \"string\"},\n"
      + "    {\"name\": \"rider\", \"type\": \"string\"},\n"
      + "    {\"name\": \"rider2\", \"type\": \"string\"},\n"
      + "    {\"name\": \"driver\", \"type\": \"string\"}\n"
      + "  ]\n"
      + "}";

  private static final String SCHEMA_WITH_PARTITION_COLUMN_STR = "{\n"
      + "  \"namespace\": \"example.avro\",\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"User\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"timestamp\", \"type\": \"long\"},\n"
      + "    {\"name\": \"_row_key\", \"type\": \"string\"},\n"
      + "    {\"name\": \"rider\", \"type\": \"string\"},\n"
      + "    {\"name\": \"driver\", \"type\": \"string\"},\n"
      + "    {\"name\":\"partitionColumn\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}"
      + "  ]\n"
      + "}";
  private static Schema SCHEMA_WITHOUT_METADATA2 = new Schema.Parser().parse(SCHEMA_WITHOUT_METADATA_STR2);
  private static Schema SCHEMA_WITHOUT_METADATA = new Schema.Parser().parse(SCHEMA_WITHOUT_METADATA_STR);
  private static Schema SCHEMA_WITH_METADATA = addMetadataFields(SCHEMA_WITHOUT_METADATA, false);
  private static Schema SCHEMA_WITH_PARTITION_COLUMN = new Schema.Parser().parse(SCHEMA_WITH_PARTITION_COLUMN_STR);

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

    String requestTime1 = "0010";
    if (type.equals("commitOrDeltaCommit")) {
      // Case 1: Regular commit
      addCommitOrDeltaCommitWithSchema(tableType, requestTime1, SCHEMA_WITH_METADATA.toString());
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
      testTable.addReplaceCommit(requestTime1,
          Option.of(incTimestampStrByOne(requestTime1)),
          Option.of(requestedMetadata),
          Option.empty(),
          (HoodieReplaceCommitMetadata)(buildMetadata(
              Collections.emptyList(),
              Collections.emptyMap(),
              Option.empty(),
              WriteOperationType.UNKNOWN,
              SCHEMA_WITH_METADATA.toString(),
              REPLACE_COMMIT_ACTION)));
    }

    metaClient.reloadActiveTimeline();

    ConcurrentSchemaEvolutionTableSchemaGetter resolver = new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(false, Option.empty());
    assertTrue(schemaOption.isPresent());
    assertEquals(SCHEMA_WITHOUT_METADATA, schemaOption.get());
  }

  @Test
  void testGetTableSchemaFromLatestCommitMetadata() throws Exception {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    initMetaClient(false, HoodieTableType.MERGE_ON_READ);
    testTable = HoodieTestTable.of(metaClient);

    String requestTime1 = "0010";
    String requestTime2 = "0020";
    String commitTime1 = "0040";
    String commitTime2 = "0030";
    // 001 |------------------| s1
    // 002    |------| s2
    // We should use completion time based ordering and get s2.
    createRequestedDeltaCommit(metaClient, requestTime1);
    createInflightDeltaCommit(metaClient, requestTime1);

    testTable.addDeltaCommit(requestTime2, Option.of(commitTime2), buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        SCHEMA_WITHOUT_METADATA2.toString(),
        DELTA_COMMIT_ACTION));

    createDeltaCommit(metaClient, metaClient.getCommitMetadataSerDe(), commitTime1, Option.of(commitTime1), buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        SCHEMA_WITH_METADATA.toString(),
        DELTA_COMMIT_ACTION));

    metaClient.reloadActiveTimeline();

    ConcurrentSchemaEvolutionTableSchemaGetter resolver = new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(false, Option.empty());
    assertTrue(schemaOption.isPresent());
    assertEquals(SCHEMA_WITHOUT_METADATA, schemaOption.get());
  }

  private void addCommitOrDeltaCommitWithSchema(HoodieTableType tableType, String requestTime1, String schemaStr) throws Exception {
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      testTable.addCommit(requestTime1, Option.of(incTimestampStrByOne(requestTime1)), Option.of(buildMetadata(
          Collections.emptyList(),
          Collections.emptyMap(),
          Option.empty(),
          WriteOperationType.UNKNOWN,
          schemaStr,
          COMMIT_ACTION)));
    } else {
      testTable.addDeltaCommit(requestTime1, Option.of(incTimestampStrByOne(requestTime1)), buildMetadata(
          Collections.emptyList(),
          Collections.emptyMap(),
          Option.empty(),
          WriteOperationType.UNKNOWN,
          schemaStr,
          DELTA_COMMIT_ACTION));
    }
  }

  private static Stream<Arguments> commonTableConfigTestDimension() {
    return Stream.of(
      Arguments.of(HoodieTableType.COPY_ON_WRITE),
      Arguments.of(HoodieTableType.MERGE_ON_READ)
    );
  }

  @ParameterizedTest
  @MethodSource("commonTableConfigTestDimension")
  void testGetTableAvroSchemaInternalNoSchemaFoundEmptyTimeline(HoodieTableType tableType) throws IOException {
    // Don't set any schema in commit metadata or table config
    initMetaClient(false, tableType);
    testTable = HoodieTestTable.of(metaClient);
    metaClient.reloadActiveTimeline();

    ConcurrentSchemaEvolutionTableSchemaGetter resolver = new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(true, Option.empty());
    assertFalse(schemaOption.isPresent());
  }

  // Test fall back behavior of table schema resolver - if there is no schema extracted from the schema evolution timeline, we will
  // try commit timeline and parse any usable writer schema. As long as there is anything usable in the schema evolution timeline,
  // we will only use that and ignore the other instants.
  @ParameterizedTest
  @MethodSource("commonTableConfigTestDimension")
  void testGetTableAvroSchemaInternalNoSchemaFoundDisqualifiedInstant(HoodieTableType tableType) throws Exception {
    // Don't set any schema in commit metadata or table config
    initMetaClient(false, tableType);
    testTable = HoodieTestTable.of(metaClient);
    int startCommitTime = 10;

    // Create instants that won't show up in the schema evolution timeline.
    createExhaustiveDisqualifiedInstants(startCommitTime, tableType);
    metaClient.reloadActiveTimeline();

    ConcurrentSchemaEvolutionTableSchemaGetter resolver = new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(true, Option.empty());
    assertTrue(schemaOption.isEmpty());
  }

  private int createExhaustiveDisqualifiedInstants(int startCommitTime, HoodieTableType tableType) throws Exception {
    if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
      String requestTime = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
      testTable.addCompaction(requestTime, Option.of(incTimestampStrByOne(requestTime)),
          buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.COMPACT, SCHEMA_WITH_METADATA.toString(), COMPACTION_ACTION));
    }
    startCommitTime += 10;
    // Clean
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""),
        "", "", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
    HoodieCleanMetadata cleanMeta = new HoodieCleanMetadata("", 0L, 0, "20", "",
        Collections.emptyMap(), metaClient.getTableConfig().getTableVersion().versionCode(), Collections.emptyMap(), Collections.singletonMap(
            HoodieCommitMetadata.SCHEMA_KEY, SCHEMA_WITH_METADATA.toString()));
    String cleanTimestamp = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
    testTable.addClean(cleanTimestamp, Option.of(incTimestampStrByOne(cleanTimestamp)), cleanerPlan, cleanMeta);
    startCommitTime += 10;

    // Clustering commit
    HoodieClusteringGroup group = new HoodieClusteringGroup();
    HoodieClusteringPlan plan = new HoodieClusteringPlan(Collections.singletonList(group),
        HoodieClusteringStrategy.newBuilder().build(), Collections.emptyMap(), 1, false, null);
    HoodieRequestedReplaceMetadata requestedMetadata = new HoodieRequestedReplaceMetadata(WriteOperationType.CLUSTER.name(), plan, Collections.emptyMap(), 1);
    String replaceInstantTime = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
    testTable.addReplaceCommit(replaceInstantTime, Option.of(incTimestampStrByOne(replaceInstantTime)), Option.of(requestedMetadata), Option.empty(),
            (HoodieReplaceCommitMetadata)buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UNKNOWN,
                SCHEMA_WITH_METADATA.toString(), CLUSTERING_ACTION));
    startCommitTime += 10;

    // Inflight commits
    testTable.addInflightCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH));
    startCommitTime += 10;

    testTable.addInflightDeltaCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH));
    startCommitTime += 10;

    // Commits without schema in it.
    String commitTimestamp = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
    testTable.addCommit(commitTimestamp, Option.of(incTimestampStrByOne(commitTimestamp)), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        "",
        COMMIT_ACTION)));
    startCommitTime += 10;

    // Commits without schema in it.
    String deltaCommitTimestamp = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
    testTable.addDeltaCommit(deltaCommitTimestamp, Option.of(deltaCommitTimestamp), buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        "",
        DELTA_COMMIT_ACTION));
    startCommitTime += 10;

    // Savepoint
    HoodieSavepointMetadata savepointMetadata = new HoodieSavepointMetadata();
    savepointMetadata.setSavepointedAt(12345L);
    savepointMetadata.setSavepointedBy("12345");
    savepointMetadata.setComments("12345");
    savepointMetadata.setPartitionMetadata(Collections.emptyMap());
    String savepointTimestamp = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
    testTable.addSavepointCommit(savepointTimestamp, Option.of(incTimestampStrByOne(savepointTimestamp)), savepointMetadata);
    startCommitTime += 10;

    // Rollback
    testTable.addInflightRollback(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH));
    testTable.addRollback(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH), new HoodieRollbackMetadata(), new HoodieRollbackPlan());

    return startCommitTime;
  }

  private static Stream<Arguments> schemaTestParams() {
    return Stream.of(
        Arguments.of(SCHEMA_WITH_METADATA, false, SCHEMA_WITHOUT_METADATA),    // Schema with metadata fields, don't include metadata
        Arguments.of(SCHEMA_WITHOUT_METADATA, true, SCHEMA_WITH_METADATA)      // Schema without metadata fields, include metadata
    );
  }

  @ParameterizedTest
  @MethodSource("schemaTestParams")
  void testGetTableAvroSchema(Schema inputSchema, boolean includeMetadataFields, Schema expectedSchema) throws Exception {
    metaClient = HoodieTestUtils.getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(),"")
        .initTable(getDefaultStorageConf(), basePath);
    testTable = HoodieTestTable.of(metaClient);

    testTable.addCommit("0010", Option.of(incTimestampStrByOne("0010")), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        inputSchema.toString(),
        COMMIT_ACTION)));

    assertEquals(expectedSchema, new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient).getTableAvroSchemaIfPresent(includeMetadataFields, Option.empty()).get());
    HoodieInstant instant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, "0010", "0011");
    assertEquals(expectedSchema, new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient).getTableAvroSchemaIfPresent(
        includeMetadataFields, Option.of(instant)).get());
  }

  private static Stream<Arguments> partitionColumnSchemaTestParams() {
    return Stream.of(
        Arguments.of(false, SCHEMA_WITHOUT_METADATA),    // Schema with metadata fields, don't include metadata
        Arguments.of(true, SCHEMA_WITH_PARTITION_COLUMN)      // Schema without metadata fields, include metadata
    );
  }

  @ParameterizedTest
  @MethodSource("partitionColumnSchemaTestParams")
  void testGetTableAvroSchemaAppendPartitionColumn(boolean shouldIncludePartitionColumns, Schema expectedSchema) throws Exception {
    metaClient = HoodieTestUtils.getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(),"")
        .setPartitionFields("partitionColumn")
        .setShouldDropPartitionColumns(shouldIncludePartitionColumns)
        .initTable(getDefaultStorageConf(), basePath);
    testTable = HoodieTestTable.of(metaClient);

    testTable.addCommit("0010", Option.of("0011"), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        SCHEMA_WITHOUT_METADATA.toString(),
        COMMIT_ACTION)));

    assertEquals(expectedSchema, new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient).getTableAvroSchemaIfPresent(false, Option.empty()).get());
    assertEquals(expectedSchema, new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient).getTableAvroSchemaIfPresent(
        false, Option.of(metaClient.getInstantGenerator().createNewInstant(
            HoodieInstant.State.COMPLETED, COMMIT_ACTION, "0010", "0011"))).get());
  }

  private static Stream<Arguments> createSchemaTestParam() {
    return Stream.of(
        Arguments.of(false, SCHEMA_WITHOUT_METADATA),    // Schema with metadata fields, don't include metadata
        Arguments.of(true, SCHEMA_WITH_METADATA)      // Schema without metadata fields, include metadata
    );
  }

  @ParameterizedTest
  @MethodSource("createSchemaTestParam")
  void testGetTableCreateAvroSchema(boolean includeMetadataFields, Schema expectedSchema) throws Exception {
    metaClient = HoodieTestUtils.getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(),"")
        .initTable(getDefaultStorageConf(), basePath);
    testTable = HoodieTestTable.of(metaClient);

    assertEquals(expectedSchema, new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient).getTableAvroSchemaIfPresent(includeMetadataFields, Option.empty()).get());
    // getTableAvroSchemaFromLatestCommit only cares about active timeline, since it is empty, no schema is returned.
    assertEquals(expectedSchema, new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient).getTableAvroSchemaIfPresent(
        includeMetadataFields, Option.of(metaClient.getInstantGenerator().createNewInstant(
            HoodieInstant.State.COMPLETED, COMMIT_ACTION, "0010", "0011"))).get());
  }

  @Test
  public void testGetTableAvroSchemaInternalWithPartitionFields() throws IOException {
    initMetaClient(false, HoodieTableType.COPY_ON_WRITE);
    testTable = HoodieTestTable.of(metaClient);
    // Setup table config with partition fields
    String[] partitionFields = new String[] {"partition_path"};
    metaClient.getTableConfig().setValue(PARTITION_FIELDS, String.join(",", partitionFields));
    metaClient.getTableConfig().setValue(HoodieTableConfig.DROP_PARTITION_COLUMNS, "true");
    metaClient.reloadActiveTimeline();

    ConcurrentSchemaEvolutionTableSchemaGetter resolver = new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaIfPresent(true, Option.empty());

    assertTrue(schemaOption.isPresent());
    Schema resultSchema = schemaOption.get();
    assertTrue(resultSchema.getFields().stream()
        .anyMatch(f -> f.name().equals("partition_path")));
  }

  @ParameterizedTest
  @MethodSource("commonTableConfigTestDimension")
  void testGetTableAvroSchemaInternalWithSpecificInstant(HoodieTableType tableType) throws Exception {
    initMetaClient(false, tableType);
    testTable = HoodieTestTable.of(metaClient);

    Schema schema1 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Schema schema2 = new Schema.Parser().parse(TRIP_SCHEMA);

    // Create two commits with different schemas
    int startCommitTime = 10;
    // First commit with schema1
    String instantTime = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      testTable.addCommit(instantTime, Option.of(incTimestampStrByOne(instantTime)),
          Option.of(buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UNKNOWN, schema1.toString(), COMMIT_ACTION)));
    } else {
      testTable.addDeltaCommit(instantTime, Option.of(incTimestampStrByOne(instantTime)), buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
          WriteOperationType.UNKNOWN, schema1.toString(), DELTA_COMMIT_ACTION));
    }
    startCommitTime += 10;

    // Second commit with schema2
    String instantTime2 = padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH);
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      testTable.addCommit(instantTime2, Option.of(incTimestampStrByOne(instantTime2)), Option.of(buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
          WriteOperationType.UNKNOWN, schema2.toString(), COMMIT_ACTION)));
    } else {
      testTable.addDeltaCommit(instantTime2, Option.of(incTimestampStrByOne(instantTime2)), buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(),
          WriteOperationType.UNKNOWN, schema2.toString(), DELTA_COMMIT_ACTION));
    }
    startCommitTime += 10;

    metaClient.reloadActiveTimeline();

    ConcurrentSchemaEvolutionTableSchemaGetter resolver = new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient);

    // Test getting schema from first instant
    String timestamp1 = padWithLeadingZeros(Integer.toString(10), REQUEST_TIME_LENGTH);
    Option<Schema> schema1Option = resolver.getTableAvroSchemaIfPresent(
        false,
        Option.of(metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, timestamp1, incTimestampStrByOne(timestamp1))));
    assertTrue(schema1Option.isPresent());
    assertEquals(schema1.toString(), schema1Option.get().toString());

    // Test getting schema from second instant
    String timestamp2 = padWithLeadingZeros(Integer.toString(20), REQUEST_TIME_LENGTH);
    Option<Schema> schema2Option = resolver.getTableAvroSchemaIfPresent(
        false,
        Option.of(metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, timestamp2, incTimestampStrByOne(timestamp2))));
    assertTrue(schema2Option.isPresent());
    assertEquals(schema2.toString(), schema2Option.get().toString());

    // Now follow with more disqualified instants and try to get table schema with their request time, we should back track to instant 2.
    int endCommitTime = createExhaustiveDisqualifiedInstants(startCommitTime, tableType);
    metaClient.reloadActiveTimeline();

    for (Integer i = startCommitTime + 10; i <= endCommitTime + 10; i += 10) {
      String timestampI = padWithLeadingZeros(Integer.toString(i), REQUEST_TIME_LENGTH);
      schema2Option = resolver.getTableAvroSchemaIfPresent(false,
          Option.of(metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, timestampI, incTimestampStrByOne(timestampI))));
      assertTrue(schema2Option.isPresent(), i::toString);
      assertEquals(schema2.toString(), schema2Option.get().toString());
    }
  }

  @Test
  void testTableAvroSchemaFromTimelineCachingBehavior() throws Exception {
    // Initialize with COW table type
    initMetaClient(false, HoodieTableType.COPY_ON_WRITE);
    testTable = HoodieTestTable.of(metaClient);

    // Create test schema
    Schema schema1 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Schema schema2 = new Schema.Parser().parse(HoodieTestDataGenerator.SHORT_TRIP_SCHEMA);

    // Create a commit with schema1
    String commitTime1 = "0010";
    testTable.addCommit(commitTime1, Option.of(incTimestampStrByOne(commitTime1)), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        schema1.toString(),
        COMMIT_ACTION)));
    // Create a commit with schema1
    String commitTime2 = "0020";
    testTable.addCommit(commitTime2, Option.of(incTimestampStrByOne(commitTime2)), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        schema2.toString(),
        COMMIT_ACTION)));

    metaClient.reloadActiveTimeline();

    // Create spy of TableSchemaGetter to track method calls
    ConcurrentSchemaEvolutionTableSchemaGetter resolver = Mockito.spy(new ConcurrentSchemaEvolutionTableSchemaGetter(metaClient));
    HoodieInstant instant2 = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieInstant instant1 = metaClient.getCommitsTimeline().filterCompletedInstants().nthInstant(0).get();

    // Case 1: First call with empty instant - should fetch from timeline and cache
    Option<Schema> schemaOption1 = resolver.getTableAvroSchemaFromTimelineWithCache(Option.empty());
    assertTrue(schemaOption1.isPresent());
    assertEquals(schema2, schemaOption1.get());

    // Verify getLastCommitMetadataWithValidSchemaFromTimeline was called
    verify(resolver, times(1)).getLastCommitMetadataWithValidSchemaFromTimeline(any(), any());

    // Case 2: Second call with empty instant - should use cache
    Option<Schema> schemaOption2 = resolver.getTableAvroSchemaFromTimelineWithCache(Option.empty());
    assertTrue(schemaOption2.isPresent());
    assertEquals(schema2, schemaOption2.get());

    // Verify no additional calls to timeline
    verify(resolver, times(1)).getLastCommitMetadataWithValidSchemaFromTimeline(any(), any());

    // Case 3: Call with the latest valid instant - there should be a cache hit
    Option<Schema> schemaOption3 = resolver.getTableAvroSchemaFromTimelineWithCache(Option.of(instant2));
    assertTrue(schemaOption3.isPresent());
    assertEquals(schema2, schemaOption3.get());

    // Verify no additional calls to timeline
    verify(resolver, times(1)).getLastCommitMetadataWithValidSchemaFromTimeline(any(), any());

    // Case 4: Second call with some other instant - should use cache
    Option<Schema> schemaOption4 = resolver.getTableAvroSchemaFromTimelineWithCache(Option.of(instant1));
    assertTrue(schemaOption4.isPresent());
    assertEquals(schema1, schemaOption4.get());

    // Verify no additional calls to timeline
    verify(resolver, times(2)).getLastCommitMetadataWithValidSchemaFromTimeline(any(), any());

    // Case 5: Call with future instant - should return the latest schema
    String nonExistentTime = "9999";
    HoodieInstant nonExistentInstant = metaClient.getInstantGenerator().createNewInstant(
        HoodieInstant.State.COMPLETED, COMMIT_ACTION, nonExistentTime, nonExistentTime);
    Option<Schema> schemaOption5 = resolver.getTableAvroSchemaFromTimelineWithCache(Option.of(nonExistentInstant));
    assertEquals(schema2, schemaOption5.get());

    // Verify one more call to timeline for non-existent instant
    verify(resolver, times(3)).getLastCommitMetadataWithValidSchemaFromTimeline(any(), any());

    // Cache contains 3 entries: 001, 002, 999.
    assertEquals(3L, resolver.getTableSchemaCache().size());
  }
}

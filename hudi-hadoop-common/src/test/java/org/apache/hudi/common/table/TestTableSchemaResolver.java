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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TableSchemaResolver}.
 */
public class TestTableSchemaResolver extends HoodieCommonTestHarness {
  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testRecreateSchemaWhenDropPartitionColumns() {
    Schema originSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    // case2
    String[] pts1 = new String[0];
    Schema s2 = TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts1));
    assertEquals(originSchema, s2);

    // case3: partition_path is in originSchema
    String[] pts2 = {"partition_path"};
    Schema s3 = TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts2));
    assertEquals(originSchema, s3);

    // case4: user_partition is not in originSchema
    String[] pts3 = {"user_partition"};
    Schema s4 = TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts3));
    assertNotEquals(originSchema, s4);
    assertTrue(s4.getFields().stream().anyMatch(f -> f.name().equals("user_partition")));
    Schema.Field f = s4.getField("user_partition");
    assertEquals(f.schema(), AvroSchemaUtils.createNullableSchema(Schema.Type.STRING));

    // case5: user_partition is in originSchema, but partition_path is in originSchema
    String[] pts4 = {"user_partition", "partition_path"};
    try {
      TableSchemaResolver.appendPartitionColumns(originSchema, Option.of(pts3));
    } catch (HoodieSchemaException e) {
      assertTrue(e.getMessage().contains("Partial partition fields are still in the schema"));
    }
  }

  @Test
  public void testReadSchemaFromLogFile() throws IOException, URISyntaxException, InterruptedException {
    initPath("read_schema_from_log_file");
    StoragePath partitionPath = new StoragePath(basePath, "partition1");
    Schema expectedSchema = getSimpleSchema();
    StoragePath logFilePath = writeLogFile(partitionPath, expectedSchema);
    assertEquals(expectedSchema, TableSchemaResolver.readSchemaFromLogFile(new HoodieHadoopStorage(
        logFilePath, HoodieTestUtils.getDefaultStorageConfWithDefaults()), logFilePath));
  }

  @Test
  public void testGetTableSchemaFromLatestCommitMetadataV2() throws Exception {
    Schema originalSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    String commitTime1 = "001";
    HoodieInstant instant1 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, commitTime1, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant1);

    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(HoodieCommitMetadata.SCHEMA_KEY, originalSchema.toString());

    activeTimeline.saveAsComplete(instant1, getCommitMetadata(basePath, "partition1", commitTime1, 2, extraMetadata));
    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaForClustering(false);
    assertTrue(schemaOption.isPresent());
    assertEquals(originalSchema, schemaOption.get());
  }

  @Test
  public void testGetTableCreateSchemaWithMetadata() throws IOException {
    Schema originalSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    // Set table create schema in table config
    // Create commit metadata without schema information, which should be ignored.
    Map<String, String> emptyMetadata = new HashMap<>();

    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    String commitTime1 = "001";
    HoodieInstant instant1 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, commitTime1, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant1);
    activeTimeline.saveAsComplete(instant1, getCommitMetadata(basePath, "partition1", commitTime1, 2, emptyMetadata));
    metaClient.reloadActiveTimeline();
    metaClient.getTableConfig().setValue(HoodieTableConfig.CREATE_SCHEMA, originalSchema.toString());

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaForClustering(false);
    assertTrue(schemaOption.isPresent());
    assertEquals(originalSchema, schemaOption.get());
  }

  @Test
  public void testHandlePartitionColumnsIfNeeded() {
    Schema originalSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    String[] partitionFields = new String[] {"partition_path"};
    metaClient.getTableConfig().setValue(PARTITION_FIELDS, String.join(",", partitionFields));
    metaClient.getTableConfig().setValue(HoodieTableConfig.DROP_PARTITION_COLUMNS, "true");
    metaClient.getTableConfig().setValue(HoodieTableConfig.CREATE_SCHEMA, originalSchema.toString());

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaForClustering(false);
    assertTrue(schemaOption.isPresent());

    Schema resultSchema = schemaOption.get();
    assertTrue(resultSchema.getFields().stream()
        .anyMatch(f -> f.name().equals("partition_path")));
  }

  static class SchemaEvolutionTestCase {
    private final String name;
    private final HoodieTableType tableType;
    private final List<HoodieInstant> inputInstants;
    private final List<HoodieInstant> expectedInstants;
    private final Option<HoodieInstant> clusteringInstants;

    public SchemaEvolutionTestCase(
        String name,
        HoodieTableType tableType,
        List<HoodieInstant> inputInstants,
        List<HoodieInstant> expectedInstants) {
      this.name = name;
      this.tableType = tableType;
      this.inputInstants = inputInstants;
      this.expectedInstants = expectedInstants;
      this.clusteringInstants = Option.empty();
    }

    public SchemaEvolutionTestCase(
        String name,
        HoodieTableType tableType,
        List<HoodieInstant> inputInstants,
        List<HoodieInstant> expectedInstants,
        HoodieInstant clusteringInstants
    ) {
      this.name = name;
      this.tableType = tableType;
      this.inputInstants = inputInstants;
      this.expectedInstants = expectedInstants;
      this.clusteringInstants = Option.of(clusteringInstants);
    }

    @Override
    public String toString() {
      return String.format("%s (%s)", name, tableType);
    }
  }

  static Stream<SchemaEvolutionTestCase> schemaEvolutionTestCases() {
    return Stream.of(
        // Empty timeline case
        new SchemaEvolutionTestCase(
            "Empty Timeline",
            HoodieTableType.COPY_ON_WRITE,
            new ArrayList<>(),
            new ArrayList<>()
        ),

        // Empty timeline case
        new SchemaEvolutionTestCase(
            "Empty Timeline",
            HoodieTableType.MERGE_ON_READ,
            new ArrayList<>(),
            new ArrayList<>()
        ),

        // Mixed actions case
        new SchemaEvolutionTestCase(
            "Mixed Actions",
            HoodieTableType.COPY_ON_WRITE,
            Arrays.asList(
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "001", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "002",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "003", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                // Table service actions that should be filtered out
                new HoodieInstant(REQUESTED, CLEAN_ACTION, "004", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(REQUESTED, ROLLBACK_ACTION, "005",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(REQUESTED, CLUSTERING_ACTION, "006",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(INFLIGHT, CLEAN_ACTION, "004", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(INFLIGHT, ROLLBACK_ACTION, "005",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(INFLIGHT, CLUSTERING_ACTION, "006",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, CLEAN_ACTION, "004", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "005",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, CLUSTERING_ACTION, "006",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "010",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR) // clustering commits

            ),
            Arrays.asList(
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "003", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "002",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "001", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)
            ),
            new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "010",
                InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)
        ),

        // Case 3: Comprehensive Mixed Timeline
        new SchemaEvolutionTestCase(
            "Comprehensive Mixed Timeline",
            HoodieTableType.COPY_ON_WRITE,
            Arrays.asList(
                // Valid instants
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "001", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "002",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "003", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),

                // Invalid states
                new HoodieInstant(REQUESTED, COMMIT_ACTION, "004", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(INFLIGHT, COMMIT_ACTION, "005", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                // Invalid actions
                new HoodieInstant(COMPLETED, CLEAN_ACTION, "006", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, CLUSTERING_ACTION, "007",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "010",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR) // clustering commits
            ),
            Arrays.asList(
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "003", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "002",
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                new HoodieInstant(COMPLETED, COMMIT_ACTION, "001", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)
            ),
            new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "010",
                InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)
        )
    );
  }

  private HoodieTableMetaClient prepareMetaClient(List<HoodieInstant> instants, HoodieTableType tableType,
                                                  Option<HoodieInstant> clusteringInstant) throws IOException {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    TimelineLayout timelineLayout = TimelineLayout.TIMELINE_LAYOUT_V2;
    HoodieActiveTimeline mockActiveTimeline = mock(HoodieActiveTimeline.class);

    when(mockMetaClient.getTableType()).thenReturn(tableType);
    when(mockMetaClient.getTimelineLayout()).thenReturn(timelineLayout);
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    when(mockActiveTimeline.getInstantsAsStream()).thenReturn(instants.stream());
    when(mockActiveTimeline.getInstantDetails(any()))
        .thenReturn(Option.of(new byte[0]));
    when(mockMetaClient.getBasePath()).thenReturn(new StoragePath("file://dummy/path"));
    when(mockMetaClient.scanHoodieInstantsFromFileSystem(any(), any(), eq(true)))
        .thenReturn(instants);
    when(mockMetaClient.getMetaPath()).thenReturn(new StoragePath("file://dummy/path/.hoodie"));
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockMetaClient.getTableConfig().getTimelinePath()).thenReturn("timeline");
    when(mockMetaClient.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);

    if (clusteringInstant.isPresent()) {
      // Create clustering plan metadata
      HoodieRequestedReplaceMetadata requestedReplaceMetadata =
          HoodieRequestedReplaceMetadata.newBuilder()
              .setOperationType("CLUSTER")
              .setClusteringPlan(HoodieClusteringPlan.newBuilder()
                  .setVersion(1)
                  .setStrategy(HoodieClusteringStrategy.newBuilder()
                      .setStrategyClassName(
                          "org.apache.hudi.client.clustering.run.strategy.SparkSingleFileSortStrategy")
                      .build())
                  .build())
              .build();
      // Get the corresponding requested instant for the inflight instant
      HoodieInstant requestedInstant = new HoodieInstant(
          HoodieInstant.State.REQUESTED, CLUSTERING_ACTION, clusteringInstant.get().requestedTime(),
          InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
      when(mockActiveTimeline.readRequestedReplaceMetadata(requestedInstant)).thenReturn(requestedReplaceMetadata);
    }
    return mockMetaClient;
  }

  @Disabled("HUDI-8219")
  @ParameterizedTest
  @MethodSource("schemaEvolutionTestCases")
  public void testGetSchemaEvolutionTimelineInReverseOrder(SchemaEvolutionTestCase testCase) throws IOException {
    HoodieTableMetaClient mockMetaClient =
        prepareMetaClient(testCase.inputInstants, testCase.tableType, testCase.clusteringInstants);
    TableSchemaResolver resolver = new TableSchemaResolver(mockMetaClient);
    HoodieTimeline timeline = resolver.getSchemaEvolutionTimelineInReverseOrder();

    verifyTimelineInstants(testCase.expectedInstants, timeline);
  }

  private void verifyTimelineInstants(List<HoodieInstant> expectedInstants, HoodieTimeline timeline) {
    // Verify the timeline contains exactly the expected instants in reverse order
    List<HoodieInstant> actualInstants = timeline.getInstantsAsStream().collect(Collectors.toList());
    assertEquals(expectedInstants.size(), actualInstants.size());

    for (int i = 0; i < expectedInstants.size(); i++) {
      HoodieInstant expected = expectedInstants.get(i);
      HoodieInstant actual = actualInstants.get(i);
      assertEquals(expected.getAction(), actual.getAction());
      assertEquals(expected.getCompletionTime(), actual.getCompletionTime());
      assertEquals(expected.getState(), actual.getState());
    }
  }

  private StoragePath writeLogFile(StoragePath partitionPath, Schema schema) throws IOException, URISyntaxException, InterruptedException {
    HoodieStorage storage = HoodieTestUtils.getStorage(partitionPath);
    HoodieLogFormat.Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").withInstantTime("100").withStorage(storage).build();
    List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, 100);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = getDataBlock(AVRO_DATA_BLOCK, records, header);
    writer.appendBlock(dataBlock);
    writer.close();
    return writer.getLogFile().getPath();
  }

  @Test
  public void testGetTableAvroSchemaInternalFromCommitMetadata() throws Exception {
    Schema originalSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    // Setup commit with schema in metadata
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    String commitTime = "001";
    HoodieInstant instant = new HoodieInstant(INFLIGHT, COMMIT_ACTION, commitTime, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant);

    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(HoodieCommitMetadata.SCHEMA_KEY, originalSchema.toString());

    activeTimeline.saveAsComplete(instant,
        getCommitMetadata(basePath, "partition1", commitTime, 2, extraMetadata));
    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);

    // Test with includeMetadataFields = false
    Option<Schema> schemaWithoutMetadata = resolver.getTableAvroSchemaInternal(false, Option.empty());
    assertTrue(schemaWithoutMetadata.isPresent());
    assertNull(schemaWithoutMetadata.get().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD));
  }

  @Test
  public void testGetTableAvroSchemaInternalFromTableConfig() {
    Schema originalSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    // Set schema in table config
    metaClient.getTableConfig().setValue(HoodieTableConfig.CREATE_SCHEMA, originalSchema.toString());

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);

    // Test with includeMetadataFields = true
    Option<Schema> schemaWithMetadata = resolver.getTableAvroSchemaInternal(true, Option.empty());
    assertTrue(schemaWithMetadata.isPresent());
    assertTrue(schemaWithMetadata.get().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD) != null);

    // Test with includeMetadataFields = false
    Option<Schema> schemaWithoutMetadata = resolver.getTableAvroSchemaInternal(false, Option.empty());
    assertTrue(schemaWithoutMetadata.isPresent());
    assertNull(schemaWithoutMetadata.get().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD));
  }

  @Test
  public void testGetTableAvroSchemaInternalWithPartitionFields() throws Exception {
    Schema originalSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    // Setup table config with partition fields
    String[] partitionFields = new String[] {"partition_path"};
    metaClient.getTableConfig().setValue(PARTITION_FIELDS, String.join(",", partitionFields));
    metaClient.getTableConfig().setValue(HoodieTableConfig.DROP_PARTITION_COLUMNS, "true");
    metaClient.getTableConfig().setValue(HoodieTableConfig.CREATE_SCHEMA, originalSchema.toString());

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaInternal(true, Option.empty());

    assertTrue(schemaOption.isPresent());
    Schema resultSchema = schemaOption.get();
    assertTrue(resultSchema.getFields().stream()
        .anyMatch(f -> f.name().equals("partition_path")));
  }

  @Test
  public void testGetTableAvroSchemaInternalNoSchemaFound() throws Exception {
    // Don't set any schema in commit metadata or table config
    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Option<Schema> schemaOption = resolver.getTableAvroSchemaInternal(true, Option.empty());
    assertFalse(schemaOption.isPresent());
  }

  @Test
  public void testGetTableAvroSchemaInternalWithSpecificInstant() throws Exception {
    Schema schema1 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Schema schema2 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_SCHEMA);

    // Create two commits with different schemas
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    // First commit with schema1
    String commitTime1 = "001";
    HoodieInstant instant1 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, commitTime1, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant1);
    Map<String, String> metadata1 = new HashMap<>();
    metadata1.put(HoodieCommitMetadata.SCHEMA_KEY, schema1.toString());
    activeTimeline.saveAsComplete(instant1,
        getCommitMetadata(basePath, "partition1", commitTime1, 2, metadata1));

    // Second commit with schema2
    String commitTime2 = "002";
    HoodieInstant instant2 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, commitTime2, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant2);
    Map<String, String> metadata2 = new HashMap<>();
    metadata2.put(HoodieCommitMetadata.SCHEMA_KEY, schema2.toString());
    activeTimeline.saveAsComplete(instant2,
        getCommitMetadata(basePath, "partition1", commitTime2, 2, metadata2));

    metaClient.reloadActiveTimeline();

    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);

    // Test getting schema from first instant
    Option<Schema> schema1Option = resolver.getTableAvroSchemaInternal(false, Option.of(
        new HoodieInstant(COMPLETED, COMMIT_ACTION, commitTime1, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)));
    assertTrue(schema1Option.isPresent());
    assertEquals(schema1.toString(), schema1Option.get().toString());

    // Test getting schema from second instant
    Option<Schema> schema2Option = resolver.getTableAvroSchemaInternal(false, Option.of(
        new HoodieInstant(COMPLETED, COMMIT_ACTION, commitTime2, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)));
    assertTrue(schema2Option.isPresent());
    assertEquals(schema2.toString(), schema2Option.get().toString());
  }
}

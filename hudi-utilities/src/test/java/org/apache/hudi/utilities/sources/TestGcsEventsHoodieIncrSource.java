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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSourceHarness.TestSourceProfile;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGcsEventsHoodieIncrSource extends SparkClientFunctionalTestHarness {

  private static final Schema GCS_METADATA_SCHEMA = SchemaTestUtil.getSchemaFromResource(
      TestGcsEventsHoodieIncrSource.class, "/streamer-config/gcs-metadata.avsc", true);
  private static final String IGNORE_FILE_EXTENSION = ".ignore";

  private ObjectMapper mapper = new ObjectMapper();

  @TempDir
  protected java.nio.file.Path tempDir;

  @Mock
  QueryRunner queryRunner;
  @Mock
  QueryInfo queryInfo;
  @Mock
  CloudObjectsSelectorCommon cloudObjectsSelectorCommon;
  @Mock
  HoodieIngestionMetrics metrics;
  @Mock
  SourceProfileSupplier sourceProfileSupplier;

  protected Option<SchemaProvider> schemaProvider;
  private HoodieTableMetaClient metaClient;
  private JavaSparkContext jsc;

  private static final Logger LOG = LoggerFactory.getLogger(TestGcsEventsHoodieIncrSource.class);

  @BeforeEach
  public void setUp() throws IOException {
    metaClient = getHoodieMetaClient(storageConf(), basePath());
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    String schemaFilePath = TestGcsEventsHoodieIncrSource.class.getClassLoader().getResource("schema/sample_gcs_data.avsc").getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    this.schemaProvider = Option.of(new FilebasedSchemaProvider(props, jsc));
    MockitoAnnotations.initMocks(this);
  }

  @Override
  public String basePath() {
    return tempDir.toAbsolutePath().toUri().toString();
  }

  @Test
  public void shouldNotFindNewDataIfCommitTimeOfWriteAndReadAreEqual() throws IOException {
    String commitTimeForWrites = "1";
    String commitTimeForReads = commitTimeForWrites;

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, inserts.getKey());
  }

  @Test
  public void shouldFetchDataIfCommitTimeForReadsLessThanForWrites() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";
    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, "1#path/to/file1.json");
  }

  @Test
  public void testTwoFilesAndContinueInSameCommit() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 250L, "1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file2.json"), 250L, "1#path/to/file3.json");
  }

  @Test
  public void largeBootstrapWithFilters() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    for (int i = 0; i <= 10000; i++) {
      filePathSizeAndCommitTime.add(Triple.of("path/to/file" + i + ".parquet", 100L, "1"));
    }
    filePathSizeAndCommitTime.add(Triple.of("path/to/file10005.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file10006.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file10007.json", 200L, "1"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 250L, "1#path/to/file10006.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file10006.json"), 250L, "1#path/to/file10007.json");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      ".json",
      ".gz"
  })
  public void testTwoFilesAndContinueAcrossCommits(String extension) throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);

    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    // In the case the extension is explicitly set to something other than the file format.
    if (!extension.endsWith("json")) {
      typedProperties.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), extension);
    }

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list.
    // Check with a couple of invalid file extensions to ensure they are filtered out.
    filePathSizeAndCommitTime.add(Triple.of(String.format("path/to/file1%s", extension), 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of(String.format("path/to/file2%s", IGNORE_FILE_EXTENSION), 800L, "1"));
    filePathSizeAndCommitTime.add(Triple.of(String.format("path/to/file3%s", extension), 200L, "1"));
    filePathSizeAndCommitTime.add(Triple.of(String.format("path/to/file2%s", extension), 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of(String.format("path/to/file4%s", extension), 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of(String.format("path/to/file4%s", IGNORE_FILE_EXTENSION), 200L, "2"));
    filePathSizeAndCommitTime.add(Triple.of(String.format("path/to/file5%s", extension), 150L, "2"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);
    List<Long> bytesPerPartition = Arrays.asList(10L, 100L, -1L);
    setMockQueryRunner(inputDs);

    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(100L, 0, bytesPerPartition.get(0)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1"), 100L, "1#path/to/file1" + extension, typedProperties);
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(100L, 0, bytesPerPartition.get(1)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file1" + extension), 100L, "1#path/to/file2" + extension, typedProperties);
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(1000L, 0, bytesPerPartition.get(2)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file2" + extension), 1000L, "2#path/to/file5" + extension, typedProperties);
    // Verify the partitions being passed in getCloudObjectDataDF are correct.
    List<Integer> numPartitions = Arrays.asList(12, 2, 1);
    ArgumentCaptor<Integer> argumentCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(cloudObjectsSelectorCommon, atLeastOnce()).loadAsDataset(any(), any(), any(), eq(schemaProvider), argumentCaptor.capture());
    Assertions.assertEquals(numPartitions, argumentCaptor.getAllValues());
  }

  @ParameterizedTest
  @CsvSource({
      "1,1#path/to/file2.json,3#path/to/file4.json,1#path/to/file1.json,1",
      "2,1#path/to/file2.json,3#path/to/file4.json,1#path/to/file1.json,2",
      "3,3#path/to/file5.json,3#path/to/file5.json,1#path/to/file1.json,3"
  })
  public void testSplitSnapshotLoad(String snapshotCheckPoint, String exptected1, String exptected2, String exptected3, String exptected4) throws IOException {

    writeGcsMetadataRecords("1");
    writeGcsMetadataRecords("2");
    writeGcsMetadataRecords("3");

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 50L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 50L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip1.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip2.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 50L, "3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "3"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs, Option.of(snapshotCheckPoint));
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.ignore.relpath.prefix", "path/to/skip");
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    List<Long> bytesPerPartition = Arrays.asList(10L, 20L, -1L, 1000L * 1000L * 1000L);

    // If the computed number of partitions based on bytes is less than this value, it should use this value for num partitions.
    int sourcePartitions = 2;
    //1. snapshot query, read all records
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50000L, sourcePartitions, bytesPerPartition.get(0)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50000L, exptected1, typedProperties);
    //2. incremental query, as commit is present in timeline
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(10L, sourcePartitions, bytesPerPartition.get(1)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(exptected1), 10L, exptected2, typedProperties);
    //3. snapshot query with source limit less than first commit size
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50L, sourcePartitions, bytesPerPartition.get(2)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50L, exptected3, typedProperties);
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.ignore.relpath.prefix", "path/to");
    //4. As snapshotQuery will return 1 -> same would be return as nextCheckpoint (dataset is empty due to ignore prefix).
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50L, sourcePartitions, bytesPerPartition.get(3)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50L, exptected4, typedProperties);
    // Verify the partitions being passed in getCloudObjectDataDF are correct.
    ArgumentCaptor<Integer> argumentCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> argumentCaptorForMetrics = ArgumentCaptor.forClass(Integer.class);
    verify(cloudObjectsSelectorCommon, atLeastOnce()).loadAsDataset(any(), any(), any(), eq(schemaProvider), argumentCaptor.capture());
    verify(metrics, atLeastOnce()).updateStreamerSourceParallelism(argumentCaptorForMetrics.capture());
    List<Integer> numPartitions;
    if (snapshotCheckPoint.equals("1") || snapshotCheckPoint.equals("2")) {
      numPartitions = Arrays.asList(12, 3, sourcePartitions);
    } else {
      numPartitions = Arrays.asList(23, sourcePartitions);
    }
    Assertions.assertEquals(numPartitions, argumentCaptor.getAllValues());
    Assertions.assertEquals(numPartitions, argumentCaptorForMetrics.getAllValues());
  }

  @Test
  public void testUnsupportedCheckpoint() {
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    GcsEventsHoodieIncrSource incrSource = new GcsEventsHoodieIncrSource(typedProperties, jsc(),
        spark(),
        new CloudDataFetcher(typedProperties, jsc(), spark(), metrics, cloudObjectsSelectorCommon),
        queryRunner,
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));

    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> incrSource.translateCheckpoint(Option.of(new StreamerCheckpointV2("1"))));
    assertEquals("For GcsEventsHoodieIncrSource, only StreamerCheckpointV1, i.e., requested time-based "
            + "checkpoint, is supported. Checkpoint provided is: StreamerCheckpointV2{checkpointKey='1'}",
        exception.getMessage());
  }

  @Test
  public void testCreateSource() throws IOException {
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    Source gcsSource = UtilHelpers.createSource(GcsEventsHoodieIncrSource.class.getName(), typedProperties, jsc(), spark(), metrics,
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));
    assertEquals(Source.SourceType.ROW, gcsSource.getSourceType());
    assertThrows(IOException.class, () -> UtilHelpers.createSource(GcsEventsHoodieIncrSource.class.getName(), new TypedProperties(), jsc(), spark(), metrics,
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier))));
  }

  private void setMockQueryRunner(Dataset<Row> inputDs) {
    setMockQueryRunner(inputDs, Option.empty());
  }

  private void setMockQueryRunner(Dataset<Row> inputDs, Option<String> nextCheckPointOpt) {

    when(queryRunner.run(any(QueryInfo.class), any())).thenAnswer(invocation -> {
      QueryInfo queryInfo = invocation.getArgument(0);
      QueryInfo updatedQueryInfo = nextCheckPointOpt.map(nextCheckPoint ->
              queryInfo.withUpdatedEndInstant(nextCheckPoint))
          .orElse(queryInfo);
      if (updatedQueryInfo.isSnapshot()) {
        return Pair.of(updatedQueryInfo,
            inputDs.filter(String.format("%s >= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                    updatedQueryInfo.getStartInstant()))
                .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                    updatedQueryInfo.getEndInstant())));
      }
      return Pair.of(updatedQueryInfo, inputDs);
    });
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, long sourceLimit, String expectedCheckpoint,
                             TypedProperties typedProperties) {

    GcsEventsHoodieIncrSource incrSource = new GcsEventsHoodieIncrSource(typedProperties, jsc(),
        spark(), new CloudDataFetcher(typedProperties, jsc(), spark(), metrics, cloudObjectsSelectorCommon), queryRunner,
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));

    Pair<Option<Dataset<Row>>, Checkpoint> dataAndCheckpoint = incrSource.fetchNextBatch(
        checkpointToPull.isPresent() ? Option.of(new StreamerCheckpointV1(checkpointToPull.get())) : Option.empty(), sourceLimit);

    Option<Dataset<Row>> datasetOpt = dataAndCheckpoint.getLeft();
    Checkpoint nextCheckPoint = dataAndCheckpoint.getRight();

    Assertions.assertNotNull(nextCheckPoint);
    Assertions.assertEquals(new StreamerCheckpointV1(expectedCheckpoint), nextCheckPoint);
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, long sourceLimit, String expectedCheckpoint) {
    TypedProperties typedProperties = setProps(missingCheckpointStrategy);
    typedProperties.put("hoodie.streamer.source.hoodieincr.file.format", "json");
    readAndAssert(missingCheckpointStrategy, checkpointToPull, sourceLimit, expectedCheckpoint, typedProperties);
  }

  private HoodieRecord getGcsMetadataRecord(String commitTime, String filename, String bucketName, String generation) {
    String partitionPath = bucketName;

    String id = "id:" + bucketName + "/" + filename + "/" + generation;
    String mediaLink = String.format("https://storage.googleapis.com/download/storage/v1/b/%s/o/%s"
        + "?generation=%s&alt=media", bucketName, filename, generation);
    String selfLink = String.format("https://www.googleapis.com/storage/v1/b/%s/o/%s", bucketName, filename);

    GenericRecord rec = new GenericData.Record(GCS_METADATA_SCHEMA);
    rec.put("_row_key", id);
    rec.put("partition_path", bucketName);
    rec.put("timestamp", Long.parseLong(commitTime));

    rec.put("bucket", bucketName);
    rec.put("contentLanguage", "en");
    rec.put("contentType", "application/octet-stream");
    rec.put("crc32c", "oRB3Aw==");
    rec.put("etag", "CP7EwYCu6/kCEAE=");
    rec.put("generation", generation);
    rec.put("id", id);
    rec.put("kind", "storage#object");
    rec.put("md5Hash", "McsS8FkcDSrB3cGfb18ysA==");
    rec.put("mediaLink", mediaLink);
    rec.put("metageneration", "1");
    rec.put("name", filename);
    rec.put("selfLink", selfLink);
    rec.put("size", "370");
    rec.put("storageClass", "STANDARD");
    rec.put("timeCreated", "2022-08-29T05:52:55.869Z");
    rec.put("timeStorageClassUpdated", "2022-08-29T05:52:55.869Z");
    rec.put("updated", "2022-08-29T05:52:55.869Z");

    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(rec));
    return new HoodieAvroRecord(new HoodieKey(id, partitionPath), payload);
  }

  private HoodieWriteConfig getWriteConfig() {
    return getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  private Pair<String, List<HoodieRecord>> writeGcsMetadataRecords(String commitTime) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {

      WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
      List<HoodieRecord> gcsMetadataRecords = Arrays.asList(
          getGcsMetadataRecord(commitTime, "data-file-1.json", "bucket-1", "1"),
          getGcsMetadataRecord(commitTime, "data-file-2.json", "bucket-1", "1"),
          getGcsMetadataRecord(commitTime, "data-file-3.json", "bucket-1", "1"),
          getGcsMetadataRecord(commitTime, "data-file-4.json", "bucket-1", "1")
      );
      List<WriteStatus> statusList = writeClient.upsert(jsc().parallelize(gcsMetadataRecords, 1), commitTime).collect();
      writeClient.commit(commitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);
      return Pair.of(commitTime, gcsMetadataRecords);
    }
  }

  private TypedProperties setProps(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy) {
    TypedProperties properties = new TypedProperties();
    //String schemaFilePath = TestGcsEventsHoodieIncrSource.class.getClassLoader().getResource("schema/sample_gcs_data.avsc").getPath();
    //properties.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    properties.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    properties.setProperty("hoodie.streamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy",
        missingCheckpointStrategy.name());
    properties.setProperty(CloudSourceConfig.DATAFILE_FORMAT.key(), "json");
    return properties;
  }

  private HoodieWriteConfig.Builder getConfigBuilder(String basePath, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(GCS_METADATA_SCHEMA.toString())
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(metaClient.getTableConfig().getTableName());
  }

  private String generateGCSEventMetadata(Long objectSize, String bucketName, String objectKey, String commitTime)
      throws JsonProcessingException {
    Map<String, Object> objectMetadata = new HashMap<>();
    objectMetadata.put("bucket", bucketName);
    objectMetadata.put("name", objectKey);
    objectMetadata.put("size", objectSize);
    objectMetadata.put("_hoodie_commit_time", commitTime);
    return mapper.writeValueAsString(objectMetadata);
  }

  private List<String> getSampleGCSObjectKeys(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    return filePathSizeAndCommitTime.stream().map(f -> {
      try {
        return generateGCSEventMetadata(f.getMiddle(), "bucket-1", f.getLeft(), f.getRight());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private Dataset<Row> generateDataset(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    JavaRDD<String> testRdd = jsc.parallelize(getSampleGCSObjectKeys(filePathSizeAndCommitTime), 2);
    Dataset<Row> inputDs = spark().read().json(testRdd);
    return inputDs;
  }
}

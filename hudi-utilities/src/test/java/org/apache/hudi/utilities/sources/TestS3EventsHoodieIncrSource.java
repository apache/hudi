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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
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
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.sources.helpers.TestCloudObjectsSelectorCommon;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.SourceProfile;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestS3EventsHoodieIncrSource extends SparkClientFunctionalTestHarness {
  private static final Schema S3_METADATA_SCHEMA = SchemaTestUtil.getSchemaFromResource(
      TestS3EventsHoodieIncrSource.class, "/streamer-config/s3-metadata.avsc", true);

  private ObjectMapper mapper = new ObjectMapper();

  private static final String MY_BUCKET = "some-bucket";
  private static final String IGNORE_FILE_EXTENSION = ".ignore";

  private Option<SchemaProvider> schemaProvider;
  @Mock
  QueryRunner mockQueryRunner;
  @Mock
  CloudObjectsSelectorCommon mockCloudObjectsSelectorCommon;
  @Mock
  SourceProfileSupplier sourceProfileSupplier;
  @Mock
  QueryInfo queryInfo;
  @Mock
  HoodieIngestionMetrics metrics;
  private JavaSparkContext jsc;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    metaClient = getHoodieMetaClient(storageConf(), basePath());
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_gcs_data.avsc").getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    this.schemaProvider = Option.of(new FilebasedSchemaProvider(props, jsc));
  }

  private List<String> getSampleS3ObjectKeys(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    return filePathSizeAndCommitTime.stream().map(f -> {
      try {
        return generateS3EventMetadata(f.getMiddle(), MY_BUCKET, f.getLeft(), f.getRight());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private Dataset<Row> generateDataset(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    JavaRDD<String> testRdd = jsc.parallelize(getSampleS3ObjectKeys(filePathSizeAndCommitTime), 2);
    Dataset<Row> inputDs = spark().read().json(testRdd);
    return inputDs;
  }

  /**
   * Generates simple Json structure like below
   * <p>
   * s3 : {
   * object : {
   * size:
   * key:
   * }
   * bucket: {
   * name:
   * }
   */
  private String generateS3EventMetadata(Long objectSize, String bucketName, String objectKey, String commitTime)
      throws JsonProcessingException {
    Map<String, Object> objectMetadata = new HashMap<>();
    objectMetadata.put("size", objectSize);
    objectMetadata.put("key", objectKey);
    Map<String, String> bucketMetadata = new HashMap<>();
    bucketMetadata.put("name", bucketName);
    Map<String, Object> s3Metadata = new HashMap<>();
    s3Metadata.put("object", objectMetadata);
    s3Metadata.put("bucket", bucketMetadata);
    Map<String, Object> eventMetadata = new HashMap<>();
    eventMetadata.put("s3", s3Metadata);
    eventMetadata.put("_hoodie_commit_time", commitTime);
    return mapper.writeValueAsString(eventMetadata);
  }

  private HoodieRecord generateS3EventMetadata(String commitTime, String bucketName, String objectKey, Long objectSize) {
    String partitionPath = bucketName;
    Schema schema = S3_METADATA_SCHEMA;
    GenericRecord rec = new GenericData.Record(schema);
    Schema.Field s3Field = schema.getField("s3");
    Schema s3Schema = s3Field.schema().getTypes().get(1); // Assuming the record schema is the second type
    // Create a generic record for the "s3" field
    GenericRecord s3Record = new GenericData.Record(s3Schema);

    Schema.Field s3BucketField = s3Schema.getField("bucket");
    Schema s3Bucket = s3BucketField.schema().getTypes().get(1); // Assuming the record schema is the second type
    GenericRecord s3BucketRec = new GenericData.Record(s3Bucket);
    s3BucketRec.put("name", bucketName);


    Schema.Field s3ObjectField = s3Schema.getField("object");
    Schema s3Object = s3ObjectField.schema().getTypes().get(1); // Assuming the record schema is the second type
    GenericRecord s3ObjectRec = new GenericData.Record(s3Object);
    s3ObjectRec.put("key", objectKey);
    s3ObjectRec.put("size", objectSize);

    s3Record.put("bucket", s3BucketRec);
    s3Record.put("object", s3ObjectRec);
    rec.put("s3", s3Record);
    rec.put("_hoodie_commit_time", commitTime);

    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(rec));
    return new HoodieAvroRecord(new HoodieKey(objectKey, partitionPath), payload);
  }

  private TypedProperties setProps(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy) {
    Properties properties = new Properties();
    properties.setProperty("hoodie.streamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy",
        missingCheckpointStrategy.name());
    properties.setProperty("hoodie.streamer.source.hoodieincr.file.format", "json");
    return new TypedProperties(properties);
  }

  private HoodieWriteConfig.Builder getConfigBuilder(String basePath, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(S3_METADATA_SCHEMA.toString())
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(metaClient.getTableConfig().getTableName());
  }

  private HoodieWriteConfig getWriteConfig() {
    return getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  private Pair<String, List<HoodieRecord>> writeS3MetadataRecords(String commitTime) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {

      writeClient.startCommitWithTime(commitTime);
      List<HoodieRecord> s3MetadataRecords = Arrays.asList(
          generateS3EventMetadata(commitTime, "bucket-1", "data-file-1.json", 1L)
      );
      JavaRDD<WriteStatus> result = writeClient.upsert(jsc().parallelize(s3MetadataRecords, 1), commitTime);

      List<WriteStatus> statuses = result.collect();
      assertNoWriteErrors(statuses);

      return Pair.of(commitTime, s3MetadataRecords);
    }
  }

  @Test
  public void testEmptyCheckpoint() throws IOException {
    String commitTimeForWrites = "1";
    String commitTimeForReads = commitTimeForWrites;

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForWrites);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 0L, inserts.getKey());
  }

  @Test
  public void testOneFileInCommit() throws IOException {
    String commitTimeForWrites1 = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForReads);
    inserts = writeS3MetadataRecords(commitTimeForWrites1);

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, "1#path/to/file1.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file1.json"), 200L, "1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file2.json"), 200L, "1#path/to/file3.json");
  }

  @Test
  public void testTwoFilesAndContinueInSameCommit() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForReads);
    inserts = writeS3MetadataRecords(commitTimeForWrites);

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 250L, "1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file2.json"), 250L, "1#path/to/file3.json");

  }

  @ParameterizedTest
  @ValueSource(strings = {
      ".json",
      ".gz"
  })
  public void testTwoFilesAndContinueAcrossCommits(String extension) throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForReads);
    inserts = writeS3MetadataRecords(commitTimeForWrites);

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

    setMockQueryRunner(inputDs);
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1"), 100L,
        "1#path/to/file1" + extension, typedProperties);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file1" + extension), 100L,
        "1#path/to/file2" + extension, typedProperties);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file2" + extension), 1000L,
        "2#path/to/file5" + extension, typedProperties);
  }

  @Test
  public void testEmptyDataAfterFilter() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForReads);
    inserts = writeS3MetadataRecords(commitTimeForWrites);


    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip3.json", 200L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip5.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip4.json", 150L, "2"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    typedProperties.setProperty("hoodie.streamer.source.s3incr.ignore.key.prefix", "path/to/skip");

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1"), 1000L, "2", typedProperties);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file3.json"), 1000L, "2", typedProperties);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("2#path/to/skip4.json"), 1000L, "2#path/to/skip4.json", typedProperties);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("2#path/to/skip5.json"), 1000L, "2#path/to/skip5.json", typedProperties);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("2"), 1000L, "2", typedProperties);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFilterAnEntireCommit(boolean useSourceProfile) throws IOException {
    String commitTimeForWrites1 = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForReads);
    inserts = writeS3MetadataRecords(commitTimeForWrites1);


    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip2.json", 200L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip3.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip4.json", 50L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip5.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 150L, "2"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    SourceProfile<Long> sourceProfile = new TestSourceProfile(50L, 10L);
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    if (useSourceProfile) {
      when(sourceProfileSupplier.getSourceProfile()).thenReturn(sourceProfile);
    } else {
      when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    }
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    typedProperties.setProperty("hoodie.streamer.source.s3incr.ignore.key.prefix", "path/to/skip");

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1"), 50L, "2#path/to/file4.json", typedProperties);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFilterAnEntireMiddleCommit(boolean useSourceProfile) throws IOException {
    String commitTimeForWrites1 = "2";
    String commitTimeForWrites2 = "3";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForReads);
    inserts = writeS3MetadataRecords(commitTimeForWrites1);
    inserts = writeS3MetadataRecords(commitTimeForWrites2);


    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip1.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip2.json", 150L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 150L, "3"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    SourceProfile<Long> sourceProfile = new TestSourceProfile(50L, 10L);
    if (useSourceProfile) {
      when(sourceProfileSupplier.getSourceProfile()).thenReturn(sourceProfile);
    } else {
      when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    }

    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    typedProperties.setProperty("hoodie.streamer.source.s3incr.ignore.key.prefix", "path/to/skip");

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file3.json"), 50L, "3#path/to/file4.json", typedProperties);

    schemaProvider = Option.empty();
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("1#path/to/file3.json"), 50L, "3#path/to/file4.json", typedProperties);
  }

  @ParameterizedTest
  @CsvSource({
      "1,1#path/to/file2.json,3#path/to/file4.json,1#path/to/file1.json,1",
      "2,1#path/to/file2.json,3#path/to/file4.json,1#path/to/file1.json,2",
      "3,3#path/to/file5.json,3#path/to/file5.json,1#path/to/file1.json,3"
  })
  public void testSplitSnapshotLoad(String snapshotCheckPoint, String exptected1, String exptected2, String exptected3, String exptected4) throws IOException {

    writeS3MetadataRecords("1");
    writeS3MetadataRecords("2");
    writeS3MetadataRecords("3");

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
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    typedProperties.setProperty("hoodie.streamer.source.s3incr.ignore.key.prefix", "path/to/skip");
    List<Long> bytesPerPartition = Arrays.asList(10L, 20L, -1L, 1000L * 1000L * 1000L);

    //1. snapshot query, read all records
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50000L, bytesPerPartition.get(0)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50000L, exptected1, typedProperties);
    //2. incremental query, as commit is present in timeline
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(10L, bytesPerPartition.get(1)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(exptected1), 10L, exptected2, typedProperties);
    //3. snapshot query with source limit less than first commit size
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50L, bytesPerPartition.get(2)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50L, exptected3, typedProperties);
    typedProperties.setProperty("hoodie.streamer.source.s3incr.ignore.key.prefix", "path/to");
    //4. As snapshotQuery will return 1 -> same would be return as nextCheckpoint (dataset is empty due to ignore prefix).
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50L, bytesPerPartition.get(3)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50L, exptected4, typedProperties);
    // Verify the partitions being passed in getCloudObjectDataDF are correct.
    ArgumentCaptor<Integer> argumentCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> argumentCaptorForMetrics = ArgumentCaptor.forClass(Integer.class);
    verify(mockCloudObjectsSelectorCommon, atLeastOnce()).loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), argumentCaptor.capture());
    verify(metrics, atLeastOnce()).updateStreamerSourceParallelism(argumentCaptorForMetrics.capture());
    List<Integer> numPartitions;
    if (snapshotCheckPoint.equals("1") || snapshotCheckPoint.equals("2")) {
      numPartitions = Arrays.asList(12, 3, 1);
    } else {
      numPartitions = Arrays.asList(23, 1);
    }
    Assertions.assertEquals(numPartitions, argumentCaptor.getAllValues());
    Assertions.assertEquals(numPartitions, argumentCaptorForMetrics.getAllValues());
  }

  @Test
  public void testCreateSource() throws IOException {
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    Source s3Source = UtilHelpers.createSource(S3EventsHoodieIncrSource.class.getName(), typedProperties, jsc(), spark(), metrics,
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));
    assertEquals(Source.SourceType.ROW, s3Source.getSourceType());
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, long sourceLimit, String expectedCheckpoint,
                             TypedProperties typedProperties) {
    S3EventsHoodieIncrSource incrSource = new S3EventsHoodieIncrSource(typedProperties, jsc(),
        spark(), mockQueryRunner, new CloudDataFetcher(typedProperties, jsc(), spark(), metrics, mockCloudObjectsSelectorCommon),
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));

    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = incrSource.fetchNextBatch(checkpointToPull, sourceLimit);

    Option<Dataset<Row>> datasetOpt = dataAndCheckpoint.getLeft();
    String nextCheckPoint = dataAndCheckpoint.getRight();

    Assertions.assertNotNull(nextCheckPoint);
    Assertions.assertEquals(expectedCheckpoint, nextCheckPoint);
  }

  private void setMockQueryRunner(Dataset<Row> inputDs) {
    setMockQueryRunner(inputDs, Option.empty());
  }

  private void setMockQueryRunner(Dataset<Row> inputDs, Option<String> nextCheckPointOpt) {

    when(mockQueryRunner.run(Mockito.any(QueryInfo.class), Mockito.any())).thenAnswer(invocation -> {
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
                             Option<String> checkpointToPull, long sourceLimit, String expectedCheckpoint) {
    TypedProperties typedProperties = setProps(missingCheckpointStrategy);

    readAndAssert(missingCheckpointStrategy, checkpointToPull, sourceLimit, expectedCheckpoint, typedProperties);
  }

  static class TestSourceProfile implements SourceProfile<Long> {

    private final long maxSourceBytes;
    private final long bytesPerPartition;

    public TestSourceProfile(long maxSourceBytes, long bytesPerPartition) {
      this.maxSourceBytes = maxSourceBytes;
      this.bytesPerPartition = bytesPerPartition;
    }

    @Override
    public long getMaxSourceBytes() {
      return maxSourceBytes;
    }

    @Override
    public int getSourcePartitions() {
      throw new UnsupportedOperationException("getSourcePartitions is not required for S3 source profile");
    }

    @Override
    public Long getSourceSpecificContext() {
      return bytesPerPartition;
    }
  }
}
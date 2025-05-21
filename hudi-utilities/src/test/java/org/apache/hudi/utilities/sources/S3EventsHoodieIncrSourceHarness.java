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
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3EventsHoodieIncrSourceHarness extends SparkClientFunctionalTestHarness {
  protected static final Schema S3_METADATA_SCHEMA = SchemaTestUtil.getSchemaFromResource(
      S3EventsHoodieIncrSourceHarness.class, "/streamer-config/s3-metadata.avsc", true);

  protected ObjectMapper mapper = new ObjectMapper();

  protected static final String MY_BUCKET = "some-bucket";
  protected static final String IGNORE_FILE_EXTENSION = ".ignore";

  protected Option<SchemaProvider> schemaProvider;
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
  protected JavaSparkContext jsc;
  protected HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_gcs_data.avsc").getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    this.schemaProvider = Option.of(new FilebasedSchemaProvider(props, jsc));
  }

  protected List<String> getSampleS3ObjectKeys(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    return filePathSizeAndCommitTime.stream().map(f -> {
      try {
        return generateS3EventMetadata(f.getMiddle(), MY_BUCKET, f.getLeft(), f.getRight());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  protected Dataset<Row> generateDataset(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
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
  protected String generateS3EventMetadata(Long objectSize, String bucketName, String objectKey, String commitTime)
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

  protected HoodieRecord generateS3EventMetadata(String commitTime, String bucketName, String objectKey, Long objectSize) {
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

  protected TypedProperties setProps(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy) {
    TypedProperties properties = new TypedProperties();
    properties.setProperty("hoodie.streamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy",
        missingCheckpointStrategy.name());
    properties.setProperty("hoodie.streamer.source.hoodieincr.file.format", "json");
    return properties;
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(String basePath, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(S3_METADATA_SCHEMA.toString())
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(metaClient.getTableConfig().getTableName());
  }

  protected HoodieWriteConfig getWriteConfig() {
    return getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  protected Pair<String, List<HoodieRecord>> writeS3MetadataRecords(String commitTime) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {

      WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
      List<HoodieRecord> s3MetadataRecords = Arrays.asList(
          generateS3EventMetadata(commitTime, "bucket-1", "data-file-1.json", 1L)
      );
      JavaRDD<WriteStatus> result = writeClient.upsert(jsc().parallelize(s3MetadataRecords, 1), commitTime);

      List<WriteStatus> statuses = result.collect();
      assertNoWriteErrors(statuses);

      return Pair.of(commitTime, s3MetadataRecords);
    }
  }

  protected void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, long sourceLimit, String expectedCheckpoint,
                             TypedProperties typedProperties) {
    S3EventsHoodieIncrSource incrSource = new S3EventsHoodieIncrSource(typedProperties, jsc(),
        spark(), mockQueryRunner, new CloudDataFetcher(typedProperties, jsc(), spark(), metrics, mockCloudObjectsSelectorCommon),
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));

    Pair<Option<Dataset<Row>>, Checkpoint> dataAndCheckpoint = incrSource.fetchNextBatch(
        checkpointToPull.isPresent() ? Option.of(new StreamerCheckpointV1(checkpointToPull.get())) : Option.empty(), sourceLimit);

    Option<Dataset<Row>> datasetOpt = dataAndCheckpoint.getLeft();
    Checkpoint nextCheckPoint = dataAndCheckpoint.getRight();

    Assertions.assertNotNull(nextCheckPoint);
    Assertions.assertEquals(new StreamerCheckpointV1(expectedCheckpoint), nextCheckPoint);
  }

  protected void setMockQueryRunner(Dataset<Row> inputDs) {
    setMockQueryRunner(inputDs, Option.empty());
  }

  protected void setMockQueryRunner(Dataset<Row> inputDs, Option<String> nextCheckPointOpt) {

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

  protected void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, long sourceLimit, String expectedCheckpoint) {
    TypedProperties typedProperties = setProps(missingCheckpointStrategy);

    readAndAssert(missingCheckpointStrategy, checkpointToPull, sourceLimit, expectedCheckpoint, typedProperties);
  }

  static class TestSourceProfile implements SourceProfile<Long> {
    private final long maxSourceBytes;
    private final int sourcePartitions;
    private final long bytesPerPartition;

    public TestSourceProfile(long maxSourceBytes, int sourcePartitions, long bytesPerPartition) {
      this.maxSourceBytes = maxSourceBytes;
      this.sourcePartitions = sourcePartitions;
      this.bytesPerPartition = bytesPerPartition;
    }

    @Override
    public long getMaxSourceBytes() {
      return maxSourceBytes;
    }

    @Override
    public int getSourcePartitions() {
      return sourcePartitions;
    }

    @Override
    public Long getSourceSpecificContext() {
      return bytesPerPartition;
    }
  }

}
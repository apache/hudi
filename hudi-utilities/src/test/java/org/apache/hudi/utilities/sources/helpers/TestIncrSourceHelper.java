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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.sources.TestS3EventsHoodieIncrSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestIncrSourceHelper extends SparkClientFunctionalTestHarness {

  private ObjectMapper mapper = new ObjectMapper();
  private JavaSparkContext jsc;
  private HoodieTableMetaClient metaClient;

  private static final Schema S3_METADATA_SCHEMA = SchemaTestUtil.getSchemaFromResource(
      TestS3EventsHoodieIncrSource.class, "/streamer-config/s3-metadata.avsc", true);

  @BeforeEach
  public void setUp() throws IOException {
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    metaClient = getHoodieMetaClient(storageConf(), basePath());
  }

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

  private List<String> getSampleS3ObjectKeys(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    return filePathSizeAndCommitTime.stream().map(f -> {
      try {
        return generateS3EventMetadata(f.getMiddle(), "bucket-1", f.getLeft(), f.getRight());
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

  @Test
  void testEmptySource() {
    StructType schema = new StructType();
    Dataset<Row> emptyDataset = spark().createDataFrame(new ArrayList<Row>(), schema);
    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit1", "commit1",
        "commit2", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        emptyDataset, 50L, queryInfo, new CloudObjectIncrCheckpoint(null, null));
    assertEquals("commit2", result.getKey().toString());
    assertTrue(!result.getRight().isPresent());
  }

  @Test
  void testSingleObjectExceedingSourceLimit() {
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "commit2"));
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit1", "commit1",
        "commit2", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 50L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    Row row = result.getRight().get().select("cumulativeSize").collectAsList().get((int) result.getRight().get().count() - 1);
    assertEquals("commit1#path/to/file1.json", result.getKey().toString());
    List<Row> rows = result.getRight().get().collectAsList();
    assertEquals(1, rows.size());
    assertEquals("[[commit1,[[bucket-1],[path/to/file1.json,100]],100]]", rows.toString());
    assertEquals(100L, row.get(0));
  }

  @Test
  void testMultipleObjectExceedingSourceLimit() {
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file7.json", 100L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file7.json", 250L, "commit3"));
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit1", "commit1",
        "commit2", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 350L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    Row row = result.getRight().get().select("cumulativeSize").collectAsList().get((int) result.getRight().get().count() - 1);
    assertEquals("commit1#path/to/file2.json", result.getKey().toString());
    List<Row> rows = result.getRight().get().collectAsList();
    assertEquals(2, rows.size());
    assertEquals("[[commit1,[[bucket-1],[path/to/file1.json,100]],100], [commit1,[[bucket-1],[path/to/file2.json,150]],250]]", rows.toString());
    assertEquals(250L, row.get(0));

    result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 550L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    row = result.getRight().get().select("cumulativeSize").collectAsList().get((int) result.getRight().get().count() - 1);
    assertEquals("commit2#path/to/file4.json", result.getKey().toString());
    rows = result.getRight().get().collectAsList();
    assertEquals(4, rows.size());
    assertEquals("[[commit1,[[bucket-1],[path/to/file1.json,100]],100], [commit1,[[bucket-1],[path/to/file2.json,150]],250],"
            + " [commit1,[[bucket-1],[path/to/file3.json,200]],450], [commit2,[[bucket-1],[path/to/file4.json,50]],500]]",
        rows.toString());
    assertEquals(500L, row.get(0));
  }

  @Test
  void testCatchAllObjects() {
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file8.json", 100L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file6.json", 250L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file7.json", 50L, "commit3"));
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit1", "commit1",
        "commit2", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 1500L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    Row row = result.getRight().get().select("cumulativeSize").collectAsList().get((int) result.getRight().get().count() - 1);
    assertEquals("commit3#path/to/file8.json", result.getKey().toString());
    List<Row> rows = result.getRight().get().collectAsList();
    assertEquals(8, rows.size());
    assertEquals(1050L, row.get(0));
  }

  @Test
  void testFileOrderingAcrossCommits() {
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file8.json", 100L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file6.json", 250L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file7.json", 50L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file0.json", 100L, "commit4"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 50L, "commit4"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 50L, "commit4"));
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit3", "commit3",
        "commit4", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 50L, queryInfo, new CloudObjectIncrCheckpoint("commit3", "path/to/file8.json"));
    Row row = result.getRight().get().select("cumulativeSize").collectAsList().get((int) result.getRight().get().count() - 1);
    assertEquals("commit4#path/to/file0.json", result.getKey().toString());
    List<Row> rows = result.getRight().get().collectAsList();
    assertEquals(1, rows.size());
    assertEquals(100L, row.get(0));

    result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 350L, queryInfo, new CloudObjectIncrCheckpoint("commit3", "path/to/file8.json"));
    row = result.getRight().get().select("cumulativeSize").collectAsList().get((int) result.getRight().get().count() - 1);
    assertEquals("commit4#path/to/file2.json", result.getKey().toString());
    rows = result.getRight().get().collectAsList();
    assertEquals(3, rows.size());
    assertEquals(200L, row.get(0));
  }

  @Test
  void testLastObjectInCommit() {
    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file8.json", 100L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file6.json", 250L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file7.json", 50L, "commit3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file8.json", 50L, "commit3"));
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    // Test case 1 when queryInfo.endInstant() is equal to lastCheckpointCommit
    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit1", "commit1",
        "commit3", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 1500L, queryInfo, new CloudObjectIncrCheckpoint("commit3", "path/to/file8.json"));
    assertEquals("commit3#path/to/file8.json", result.getKey().toString());
    assertTrue(!result.getRight().isPresent());
    // Test case 2 when queryInfo.endInstant() is greater than lastCheckpointCommit
    queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit1", "commit1",
        "commit4", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 1500L, queryInfo, new CloudObjectIncrCheckpoint("commit3","path/to/file8.json"));
    assertEquals("commit4", result.getKey().toString());
    assertTrue(!result.getRight().isPresent());
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
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  private Pair<String, List<HoodieRecord>> writeS3MetadataRecords(String commitTime) throws IOException {
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

  // Tests to validate previous, begin and end instances during query generation for
  // different missing checkpoint strategies
  @Test
  void testQueryInfoGeneration() throws IOException {
    String commitTimeForReads = "1";
    String commitTimeForWrites = "2";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForReads);
    inserts = writeS3MetadataRecords(commitTimeForWrites);

    String startInstant = commitTimeForReads;
    String orderColumn = "_hoodie_commit_time";
    String keyColumn = "s3.object.key";
    String limitColumn = "s3.object.size";
    QueryInfo queryInfo = IncrSourceHelper.generateQueryInfo(jsc, basePath(), 5,
        Option.of(new StreamerCheckpointV1(startInstant)), null,
        TimelineUtils.HollowCommitHandling.BLOCK, orderColumn, keyColumn, limitColumn, true, Option.empty());
    assertEquals(String.valueOf(Integer.parseInt(commitTimeForReads) - 1), queryInfo.getPreviousInstant());
    assertEquals(commitTimeForReads, queryInfo.getStartInstant());
    assertEquals(commitTimeForWrites, queryInfo.getEndInstant());

    startInstant = commitTimeForWrites;
    queryInfo = IncrSourceHelper.generateQueryInfo(jsc, basePath(), 5,
        Option.of(new StreamerCheckpointV1(startInstant)), null,
        TimelineUtils.HollowCommitHandling.BLOCK, orderColumn, keyColumn, limitColumn, true, Option.empty());
    assertEquals(commitTimeForReads, queryInfo.getPreviousInstant());
    assertEquals(commitTimeForWrites, queryInfo.getStartInstant());
    assertEquals(commitTimeForWrites, queryInfo.getEndInstant());

  }
}
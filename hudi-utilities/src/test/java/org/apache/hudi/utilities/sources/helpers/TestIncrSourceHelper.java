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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestIncrSourceHelper extends SparkClientFunctionalTestHarness {

  private ObjectMapper mapper = new ObjectMapper();
  private JavaSparkContext jsc;

  @BeforeEach
  public void setUp() throws IOException {
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
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
    Pair<CloudObjectIncrCheckpoint, Dataset<Row>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        emptyDataset, 50L, queryInfo, new CloudObjectIncrCheckpoint(null, null));
    assertEquals("000", result.getKey().toString());
    assertEquals(emptyDataset, result.getRight());
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
    Pair<CloudObjectIncrCheckpoint, Dataset<Row>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 50L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    Row row = result.getRight().select("cumulativeSize").collectAsList().get((int) result.getRight().count() - 1);
    assertEquals("commit1#path/to/file1.json", result.getKey().toString());
    List<Row> rows = result.getRight().collectAsList();
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
    Pair<CloudObjectIncrCheckpoint, Dataset<Row>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 350L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    Row row = result.getRight().select("cumulativeSize").collectAsList().get((int) result.getRight().count() - 1);
    assertEquals("commit1#path/to/file2.json", result.getKey().toString());
    List<Row> rows = result.getRight().collectAsList();
    assertEquals(2, rows.size());
    assertEquals("[[commit1,[[bucket-1],[path/to/file1.json,100]],100], [commit1,[[bucket-1],[path/to/file2.json,150]],250]]", rows.toString());
    assertEquals(250L, row.get(0));

    result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 550L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    row = result.getRight().select("cumulativeSize").collectAsList().get((int) result.getRight().count() - 1);
    assertEquals("commit2#path/to/file4.json", result.getKey().toString());
    rows = result.getRight().collectAsList();
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
    Pair<CloudObjectIncrCheckpoint, Dataset<Row>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 1500L, queryInfo, new CloudObjectIncrCheckpoint("commit1", null));
    Row row = result.getRight().select("cumulativeSize").collectAsList().get((int) result.getRight().count() - 1);
    assertEquals("commit3#path/to/file8.json", result.getKey().toString());
    List<Row> rows = result.getRight().collectAsList();
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
    Pair<CloudObjectIncrCheckpoint, Dataset<Row>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 50L, queryInfo, new CloudObjectIncrCheckpoint("commit3","path/to/file8.json"));
    Row row = result.getRight().select("cumulativeSize").collectAsList().get((int) result.getRight().count() - 1);
    assertEquals("commit4#path/to/file0.json", result.getKey().toString());
    List<Row> rows = result.getRight().collectAsList();
    assertEquals(1, rows.size());
    assertEquals(100L, row.get(0));

    result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 350L, queryInfo, new CloudObjectIncrCheckpoint("commit3","path/to/file8.json"));
    row = result.getRight().select("cumulativeSize").collectAsList().get((int) result.getRight().count() - 1);
    assertEquals("commit4#path/to/file2.json", result.getKey().toString());
    rows = result.getRight().collectAsList();
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
    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_INCREMENTAL_OPT_VAL(), "commit1", "commit1",
        "commit3", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    Pair<CloudObjectIncrCheckpoint, Dataset<Row>> result = IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
        inputDs, 1500L, queryInfo, new CloudObjectIncrCheckpoint("commit3","path/to/file8.json"));
    assertEquals("commit3#path/to/file8.json", result.getKey().toString());
    assertTrue(result.getRight().isEmpty());
  }
}
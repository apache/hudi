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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.SourceProfile;

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
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestS3EventsHoodieIncrSource extends S3EventsHoodieIncrSourceHarness {

  @BeforeEach
  public void setUp() throws IOException {
    super.setUp();
    metaClient = getHoodieMetaClient(storageConf(), basePath());
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
    SourceProfile<Long> sourceProfile = new TestSourceProfile(50L, 0, 10L);
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
    filePathSizeAndCommitTime.add(Triple.of("path/to/file_no_match1.json", 150L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 150L, "3"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs);
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    SourceProfile<Long> sourceProfile = new TestSourceProfile(50L, 0, 10L);
    if (useSourceProfile) {
      when(sourceProfileSupplier.getSourceProfile()).thenReturn(sourceProfile);
    } else {
      when(sourceProfileSupplier.getSourceProfile()).thenReturn(null);
    }

    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    typedProperties.setProperty("hoodie.streamer.source.s3incr.ignore.key.prefix", "path/to/skip");
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.select.relative.path.regex", "path/to/file[0-9]+");

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
    filePathSizeAndCommitTime.add(Triple.of("path/to/file_no_match1.json", 50L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 50L, "1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip1.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/skip2.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file_no_match2.json", 50L, "2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 50L, "3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "3"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file_no_match3.json", 50L, "3"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    setMockQueryRunner(inputDs, Option.of(snapshotCheckPoint));
    when(mockCloudObjectsSelectorCommon.loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), Mockito.anyInt())).thenReturn(Option.empty());
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.ignore.relpath.prefix", "path/to/skip");
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.select.relative.path.regex", "path/to/file[0-9]+");

    List<Long> bytesPerPartition = Arrays.asList(10L, 20L, -1L, 1000L * 1000L * 1000L);

    // If the computed number of partitions based on bytes is less than this value, it should use this value for num partitions.
    int sourcePartitions = 2;
    //1. snapshot query, read all records
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50000L, sourcePartitions, bytesPerPartition.get(0)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50000L, exptected1, typedProperties);
    //2. incremental query, as commit is present in timeline
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.select.relpath.prefix", "path/to/");
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.select.relative.path.regex", "file[0-9]+");
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(10L, sourcePartitions, bytesPerPartition.get(1)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(exptected1), 10L, exptected2, typedProperties);
    //3. snapshot query with source limit less than first commit size
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.select.relpath.prefix", "path/to");
    typedProperties.remove("hoodie.streamer.source.cloud.data.select.relative.path.regex");
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50L, sourcePartitions, bytesPerPartition.get(2)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50L, exptected3, typedProperties);
    typedProperties.setProperty("hoodie.streamer.source.cloud.data.ignore.relpath.prefix", "path/to");
    typedProperties.remove("hoodie.streamer.source.cloud.data.select.relpath.prefix");
    //4. As snapshotQuery will return 1 -> same would be return as nextCheckpoint (dataset is empty due to ignore prefix).
    when(sourceProfileSupplier.getSourceProfile()).thenReturn(new TestSourceProfile(50L, sourcePartitions, bytesPerPartition.get(3)));
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.empty(), 50L, exptected4, typedProperties);
    // Verify the partitions being passed in getCloudObjectDataDF are correct.
    ArgumentCaptor<Integer> argumentCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> argumentCaptorForMetrics = ArgumentCaptor.forClass(Integer.class);
    verify(mockCloudObjectsSelectorCommon, atLeastOnce()).loadAsDataset(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(schemaProvider), argumentCaptor.capture());
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
    S3EventsHoodieIncrSource incrSource = new S3EventsHoodieIncrSource(typedProperties, jsc(),
        spark(), mockQueryRunner,
        new CloudDataFetcher(
            new TypedProperties(), jsc(), spark(), metrics, mockCloudObjectsSelectorCommon),
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));

    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> incrSource.translateCheckpoint(Option.of(new StreamerCheckpointV2("1"))));
    assertEquals("For S3EventsHoodieIncrSource, only StreamerCheckpointV1, i.e., requested time-based "
            + "checkpoint, is supported. Checkpoint provided is: StreamerCheckpointV2{checkpointKey='1'}",
        exception.getMessage());
  }

  @Test
  public void testCreateSource() throws IOException {
    TypedProperties typedProperties = setProps(READ_UPTO_LATEST_COMMIT);
    Source s3Source = UtilHelpers.createSource(S3EventsHoodieIncrSource.class.getName(), typedProperties, jsc(), spark(), metrics,
        new DefaultStreamContext(schemaProvider.orElse(null), Option.of(sourceProfileSupplier)));
    assertEquals(Source.SourceType.ROW, s3Source.getSourceType());
  }
}
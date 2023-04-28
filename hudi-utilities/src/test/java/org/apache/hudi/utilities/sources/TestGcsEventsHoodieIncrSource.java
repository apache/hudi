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
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.gcs.GcsObjectDataFetcher;
import org.apache.hudi.utilities.sources.helpers.gcs.GcsObjectMetadataFetcher;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGcsEventsHoodieIncrSource extends SparkClientFunctionalTestHarness {

  private static final Schema GCS_METADATA_SCHEMA = SchemaTestUtil.getSchemaFromResource(
      TestGcsEventsHoodieIncrSource.class, "/delta-streamer-config/gcs-metadata.avsc", true);

  @TempDir
  protected java.nio.file.Path tempDir;

  @Mock
  GcsObjectMetadataFetcher gcsObjectMetadataFetcher;

  @Mock
  GcsObjectDataFetcher gcsObjectDataFetcher;

  protected FilebasedSchemaProvider schemaProvider;
  private HoodieTableMetaClient metaClient;

  private static final Logger LOG = LoggerFactory.getLogger(TestGcsEventsHoodieIncrSource.class);

  @BeforeEach
  public void setUp() throws IOException {
    metaClient = getHoodieMetaClient(hadoopConf(), basePath());
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

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 0, inserts.getKey());

    verify(gcsObjectMetadataFetcher, times(0)).getGcsObjectMetadata(Mockito.any(), Mockito.any(),
            anyBoolean());
    verify(gcsObjectDataFetcher, times(0)).getCloudObjectDataDF(
            Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void shouldFetchDataIfCommitTimeForReadsLessThanForWrites() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<CloudObjectMetadata> cloudObjectMetadataList = Arrays.asList(
        new CloudObjectMetadata("data-file-1.json", 1),
        new CloudObjectMetadata("data-file-2.json", 1));
    when(gcsObjectMetadataFetcher.getGcsObjectMetadata(Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(cloudObjectMetadataList);

    List<Row> recs = Arrays.asList(
        new GenericRow(new String[] {"1", "Hello 1"}),
        new GenericRow(new String[] {"2", "Hello 2"}),
        new GenericRow(new String[] {"3", "Hello 3"}),
        new GenericRow(new String[] {"4", "Hello 4"})
    );
    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("text", DataTypes.StringType, true)
    });
    Dataset<Row> rows = spark().createDataFrame(recs, schema);

    when(gcsObjectDataFetcher.getCloudObjectDataDF(Mockito.any(), eq(cloudObjectMetadataList), Mockito.any())).thenReturn(Option.of(rows));

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 4, inserts.getKey());

    verify(gcsObjectMetadataFetcher, times(1)).getGcsObjectMetadata(Mockito.any(), Mockito.any(),
            anyBoolean());
    verify(gcsObjectDataFetcher, times(1)).getCloudObjectDataDF(Mockito.any(),
            eq(cloudObjectMetadataList), Mockito.any());
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, int expectedCount, String expectedCheckpoint) {
    TypedProperties typedProperties = setProps(missingCheckpointStrategy);

    GcsEventsHoodieIncrSource incrSource = new GcsEventsHoodieIncrSource(typedProperties, jsc(),
            spark(), schemaProvider, gcsObjectMetadataFetcher, gcsObjectDataFetcher);

    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = incrSource.fetchNextBatch(checkpointToPull, 100);

    Option<Dataset<Row>> datasetOpt = dataAndCheckpoint.getLeft();
    String nextCheckPoint = dataAndCheckpoint.getRight();

    Assertions.assertNotNull(nextCheckPoint);

    if (expectedCount == 0) {
      assertFalse(datasetOpt.isPresent());
    } else {
      assertEquals(datasetOpt.get().count(), expectedCount);
    }

    Assertions.assertEquals(nextCheckPoint, expectedCheckpoint);
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
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                    .withMaxNumDeltaCommitsBeforeCompaction(1).build())
            .build();
  }

  private Pair<String, List<HoodieRecord>> writeGcsMetadataRecords(String commitTime) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);

    writeClient.startCommitWithTime(commitTime);
    List<HoodieRecord> gcsMetadataRecords = Arrays.asList(
            getGcsMetadataRecord(commitTime, "data-file-1.json", "bucket-1", "1"),
            getGcsMetadataRecord(commitTime, "data-file-2.json", "bucket-1", "1"),
            getGcsMetadataRecord(commitTime, "data-file-3.json", "bucket-1", "1"),
            getGcsMetadataRecord(commitTime, "data-file-4.json", "bucket-1", "1")
    );
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc().parallelize(gcsMetadataRecords, 1), commitTime);

    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    return Pair.of(commitTime, gcsMetadataRecords);
  }

  private TypedProperties setProps(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy) {
    Properties properties = new Properties();
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy",
            missingCheckpointStrategy.name());
    properties.setProperty("hoodie.deltastreamer.source.gcsincr.datafile.format", "json");
    return new TypedProperties(properties);
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
}

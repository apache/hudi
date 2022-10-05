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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieLegacyAvroRecord;
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
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.gcs.FileDataFetcher;
import org.apache.hudi.utilities.sources.helpers.gcs.FilePathsFetcher;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

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

  @TempDir
  protected java.nio.file.Path tempDir;

  @Mock
  FilePathsFetcher filePathsFetcher;

  @Mock
  FileDataFetcher fileDataFetcher;

  protected FilebasedSchemaProvider schemaProvider;
  private HoodieTableMetaClient metaClient;

  private static final Logger LOG = LogManager.getLogger(TestGcsEventsHoodieIncrSource.class);

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

    verify(filePathsFetcher, times(0)).getGcsFilePaths(Mockito.any(), Mockito.any(),
            anyBoolean());
    verify(fileDataFetcher, times(0)).fetchFileData(
            Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void shouldFetchDataIfCommitTimeForReadsLessThanForWrites() throws IOException {
    String commitTimeForWrites = "2";
    String commitTimeForReads = "1";

    Pair<String, List<HoodieRecord>> inserts = writeGcsMetadataRecords(commitTimeForWrites);
    List<String> dataFiles = Arrays.asList("data-file-1.json", "data-file-2.json");
    when(filePathsFetcher.getGcsFilePaths(Mockito.any(), Mockito.any(), anyBoolean())).thenReturn(dataFiles);

    List<GcsDataRecord> recs = Arrays.asList(
            new GcsDataRecord("1", "Hello 1"),
            new GcsDataRecord("2", "Hello 2"),
            new GcsDataRecord("3", "Hello 3"),
            new GcsDataRecord("4", "Hello 4")
    );

    Dataset<Row> rows = spark().createDataFrame(recs, GcsDataRecord.class);

    when(fileDataFetcher.fetchFileData(Mockito.any(), eq(dataFiles), Mockito.any())).thenReturn(Option.of(rows));

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 4, inserts.getKey());

    verify(filePathsFetcher, times(1)).getGcsFilePaths(Mockito.any(), Mockito.any(),
            anyBoolean());
    verify(fileDataFetcher, times(1)).fetchFileData(Mockito.any(),
            eq(dataFiles), Mockito.any());
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, int expectedCount, String expectedCheckpoint) {
    TypedProperties typedProperties = setProps(missingCheckpointStrategy);

    GcsEventsHoodieIncrSource incrSource = new GcsEventsHoodieIncrSource(typedProperties, jsc(),
            spark(), schemaProvider, filePathsFetcher, fileDataFetcher);

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
    Schema sourceSchema = new MetadataSchemaProvider().getSourceSchema();
    LOG.info("schema: " + sourceSchema);

    String partitionPath = bucketName;

    String id = "id:" + bucketName + "/" + filename + "/" + generation;
    String mediaLink = String.format("https://storage.googleapis.com/download/storage/v1/b/%s/o/%s"
            + "?generation=%s&alt=media", bucketName, filename, generation);
    String selfLink = String.format("https://www.googleapis.com/storage/v1/b/%s/o/%s", bucketName, filename);

    GenericRecord rec = new GenericData.Record(sourceSchema);
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
    return new HoodieLegacyAvroRecord(new HoodieKey(id, partitionPath), payload);
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
            .withSchema(new MetadataSchemaProvider().getSourceSchema().toString())
            .withParallelism(2, 2)
            .withBulkInsertParallelism(2)
            .withFinalizeWriteParallelism(2).withDeleteParallelism(2)
            .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
            .forTable(metaClient.getTableConfig().getTableName());
  }

  private static class MetadataSchemaProvider extends SchemaProvider {

    private final Schema schema;

    public MetadataSchemaProvider() {
      super(new TypedProperties());
      this.schema = SchemaTestUtil.getSchemaFromResource(
              TestGcsEventsHoodieIncrSource.class,
              "/delta-streamer-config/gcs-metadata.avsc", true);
    }

    @Override
    public Schema getSourceSchema() {
      return schema;
    }
  }

  public static class GcsDataRecord {
    public String id;
    public String text;

    public GcsDataRecord(String id, String text) {
      this.id = id;
      this.text = text;
    }

    public String getId() {
      return id;
    }

    public String getText() {
      return text;
    }
  }

}

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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobParquetConfigInjector;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.ParquetFooters;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The settings the blob job and the embedding job of {@link UnstructuredDataSource} run with: each one a job leaves unset is set
 * when the job starts, the same value set by the job is accepted, a different one is refused naming the source and the job's mode,
 * and neither job's settings reach the other's. Both jobs read objects announced by one S3 events table, which
 * {@link EmbeddingRoundTestBase} sets up; that is why the class sits beside the embedding round tests.
 */
@Tag("functional")
public class TestUnstructuredJobSettings extends EmbeddingRoundTestBase {

  private static final String INJECTOR = HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key();
  private static final String CODEC = HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key();
  private static final String BLOOM = HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key();
  private static final String RECORD_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key();
  private static final String KEY_GENERATOR = HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key();
  private static final String MARKERS = HoodieWriteConfig.MARKERS_TYPE.key();
  private static final String ROLLBACK = HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key();
  /** Above the blob job's inline ceiling, so one object is referenced out of line. */
  private static final long BLOB_INLINE_MAX_BYTES = 64;

  private String blobTable() {
    return table("blob_table");
  }

  /** The table {@code job} writes. */
  private String tableOf(UnstructuredMode job) {
    return job == UnstructuredMode.BLOB ? blobTable() : table();
  }

  /** The record key fields of {@code job}'s table, in order, as one list. */
  private static String keyOf(UnstructuredMode job) {
    return String.join(",", job == UnstructuredMode.BLOB ? BlobTableSchema.KEY_COLUMNS : EmbeddingTableSchema.KEY_COLUMNS);
  }

  /**
   * The properties of {@code job}, with {@code removed} taken out: the blob job's are the harness's job file, the embedding job's a
   * prep_only job over the same events table.
   */
  private TypedProperties job(UnstructuredMode job, String... removed) throws Exception {
    TypedProperties props = job == UnstructuredMode.BLOB ? objects().jobProperties(BLOB_INLINE_MAX_BYTES) : props(table(), EmbeddingTableConfig.MODE_PREP_ONLY);
    for (String key : removed) {
      props.remove(key);
    }
    return props;
  }

  private void runBlobJob(TypedProperties props) throws Exception {
    HoodieStreamer streamer = new HoodieStreamer(BlobJobHarness.streamerConfig(blobTable()), jsc(), Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  /** Runs one round of {@code job} with {@code props}. */
  private void run(UnstructuredMode job, TypedProperties props) throws Exception {
    if (job == UnstructuredMode.BLOB) {
      runBlobJob(props);
    } else {
      Round round = round(props);
      assertTrue(round.getErrors().isEmpty(), round.toString());
    }
  }

  /** Runs {@code job}, which is refused at start, and returns the refusal: the IllegalArgumentException the streamer threw or kept. */
  private String refusedAtStart(UnstructuredMode job, TypedProperties props) {
    Exception failure = assertThrows(Exception.class, () -> run(job, props));
    String refusal = EmbeddingStreamers.causeOf(failure, IllegalArgumentException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
    assertTrue(EmbeddingStreamers.commits(tableOf(job), storage()).isEmpty(), "nothing is written");
    return refusal;
  }

  /** Announces one text object and one object above the blob job's inline ceiling. */
  private void announceObjects() throws Exception {
    announce(text("a", "alpha"), text("b", "a text longer than the blob job's sixty four byte inline ceiling, referenced out of line"));
  }

  private HoodieTableConfig tableConfig(String table) {
    return HoodieTableMetaClient.builder().setBasePath(table).setConf(storage()).build().getTableConfig();
  }

  /**
   * A blob job that sets none of the injector, the record key, the codec or the bloom
   * filter runs: the streamer sets them, so the table stores the blob key list and its files are uncompressed with no bloom filter.
   * The job's own properties are not changed.
   */
  @Test
  void blobJobRunsWithEveryRequiredSettingUnset() throws Exception {
    announceObjects();
    TypedProperties props = job(UnstructuredMode.BLOB, INJECTOR, RECORD_KEY, CODEC, BLOOM);
    TypedProperties before = TypedProperties.copy(props);

    runBlobJob(props);

    assertEquals(1, EmbeddingStreamers.commits(blobTable(), storage()).size());
    assertEquals(keyOf(UnstructuredMode.BLOB), tableConfig(blobTable()).getRecordKeyFieldProp());
    ParquetFooters.assertBaseFiles(jsc().hadoopConfiguration(), blobTable(), CompressionCodecName.UNCOMPRESSED, false);
    assertEquals(before, props, "the job's own properties are not changed");
  }

  /** The same for the embedding job: the markers, the injector and the record key are set; codec and bloom stay Hudi's. */
  @Test
  void embeddingJobRunsWithEveryRequiredSettingUnset() throws Exception {
    announce(text("a", "alpha", "beta"));
    TypedProperties props = job(UnstructuredMode.EMBEDDING, MARKERS, ROLLBACK, INJECTOR, RECORD_KEY);
    TypedProperties before = TypedProperties.copy(props);

    Round round = round(props);

    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(2, rows(table()).size());
    assertEquals(keyOf(UnstructuredMode.EMBEDDING), tableConfig(table()).getRecordKeyFieldProp());
    ParquetFooters.assertBaseFiles(jsc().hadoopConfiguration(), table(), hudiDefaultCodec(), true);
    assertEquals(before, props, "the job's own properties are not changed");
  }

  /**
   * The blob job's codec, bloom filter and injector, set by the streamer, never reach an embedding job that runs after it in the same
   * JVM: the embedding table keeps Hudi's codec and bloom filter.
   */
  @Test
  void blobSettingsNeverReachTheEmbeddingJob() throws Exception {
    announceObjects();
    runBlobJob(job(UnstructuredMode.BLOB, CODEC, BLOOM, INJECTOR));
    run(UnstructuredMode.EMBEDDING, job(UnstructuredMode.EMBEDDING, CODEC, BLOOM));

    ParquetFooters.assertBaseFiles(jsc().hadoopConfiguration(), blobTable(), CompressionCodecName.UNCOMPRESSED, false);
    ParquetFooters.assertBaseFiles(jsc().hadoopConfiguration(), table(), hudiDefaultCodec(), true);
  }

  /** The codec and the bloom filter are tuning defaults of the blob job: any value the job sets is kept, on either job. */
  @Test
  void explicitCodecAndBloomFilterAreKept() throws Exception {
    announceObjects();
    TypedProperties blob = job(UnstructuredMode.BLOB);
    blob.setProperty(CODEC, "zstd");
    blob.setProperty(BLOOM, "true");
    runBlobJob(blob);
    ParquetFooters.assertBaseFiles(jsc().hadoopConfiguration(), blobTable(), CompressionCodecName.ZSTD, true);

    TypedProperties embedding = job(UnstructuredMode.EMBEDDING);
    embedding.setProperty(CODEC, "zstd");
    run(UnstructuredMode.EMBEDDING, embedding);
    ParquetFooters.assertBaseFiles(jsc().hadoopConfiguration(), table(), CompressionCodecName.ZSTD, true);
  }

  /** Each required setting set by the job to the required value, in another case or with spaces, is accepted. */
  @Test
  void sameValueSetByTheJobIsAccepted() throws Exception {
    announceObjects();
    TypedProperties blob = job(UnstructuredMode.BLOB);
    blob.setProperty(INJECTOR, " " + BlobParquetConfigInjector.class.getName() + " ");
    blob.setProperty(RECORD_KEY, " source_path_sha , source_modification_time,source_file_size");
    runBlobJob(blob);
    assertEquals(1, EmbeddingStreamers.commits(blobTable(), storage()).size());

    TypedProperties embedding = job(UnstructuredMode.EMBEDDING);
    embedding.setProperty(MARKERS, "TIMELINE_SERVER_BASED");
    embedding.setProperty(ROLLBACK, "true");
    embedding.setProperty(RECORD_KEY, String.join(" ,", EmbeddingTableSchema.KEY_COLUMNS));
    assertTrue(round(embedding).getCommit().isPresent());
  }

  /** A different value of a required setting is refused at start, naming the setting, the source and the job's mode. */
  @ParameterizedTest(name = "{0} {1}={2}")
  @CsvSource(delimiter = '|', value = {
      "BLOB|hoodie.parquet.write.config.injector.class|org.apache.hudi.utilities.unstructured.embedding.EmbeddingParquetConfigInjector",
      "BLOB|hoodie.datasource.write.recordkey.field|source_modification_time,source_path_sha,source_file_size",
      "BLOB|hoodie.datasource.write.recordkey.field|source_path_sha",
      "EMBEDDING|hoodie.write.markers.type|DIRECT",
      "EMBEDDING|hoodie.rollback.using.markers|false",
      "EMBEDDING|hoodie.parquet.write.config.injector.class|org.apache.hudi.utilities.sources.blob.BlobParquetConfigInjector",
      "EMBEDDING|hoodie.datasource.write.recordkey.field|source_path_sha,source_modification_time,source_file_size"})
  void conflictingValueIsRefusedNamingTheSource(UnstructuredMode job, String key, String value) throws Exception {
    announceObjects();
    TypedProperties props = job(job);
    props.setProperty(key, value);
    String refusal = refusedAtStart(job, props);
    assertTrue(refusal.startsWith(UnstructuredDataSource.name(job) + " requires " + key + "="), refusal);
    assertTrue(refusal.endsWith("configured: " + value), refusal);
  }

  /** The key generator is the job's choice between the complex and the non-partitioned one; any other is refused at start. */
  @ParameterizedTest
  @EnumSource(UnstructuredMode.class)
  void keyGeneratorOtherThanComplexOrNonpartitionedIsRefused(UnstructuredMode job) throws Exception {
    announceObjects();
    TypedProperties props = job(job);
    props.setProperty(KEY_GENERATOR, SimpleKeyGenerator.class.getName());
    assertEquals(UnstructuredDataSource.name(job) + " supports only " + ComplexKeyGenerator.class.getName() + " or "
        + NonpartitionedKeyGenerator.class.getName() + ", configured: " + SimpleKeyGenerator.class.getName(), refusedAtStart(job, props));
  }

  /**
   * An existing table whose stored record key fields are not the job's key list is refused at start, before any write, whether the
   * job leaves the key to the streamer or names its own list.
   */
  @ParameterizedTest
  @CsvSource({"BLOB,false", "BLOB,true", "EMBEDDING,false", "EMBEDDING,true"})
  void existingTableWithAnotherRecordKeyIsRefused(UnstructuredMode job, boolean keySet) throws Exception {
    announceObjects();
    boolean blob = job == UnstructuredMode.BLOB;
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(blob ? HoodieTableType.COPY_ON_WRITE : HoodieTableType.MERGE_ON_READ)
        .setTableName(blob ? "blob_table" : "embedding_table")
        .setTableVersion(HoodieTableVersion.current())
        .setRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING)
        .setRecordKeyFields("source_path_sha")
        .setPartitionFields(blob ? "source_file_extension" : "")
        .setKeyGeneratorClassProp(blob ? ComplexKeyGenerator.class.getName() : NonpartitionedKeyGenerator.class.getName())
        .initTable(storage(), tableOf(job));
    TypedProperties props = job(job, RECORD_KEY);
    if (keySet) {
      props.setProperty(RECORD_KEY, keyOf(job));
    }
    String refusal = refusedAtStart(job, props);
    assertTrue(refusal.startsWith(UnstructuredDataSource.name(job) + " requires the table's " + HoodieTableConfig.RECORDKEY_FIELDS.key() + "="
        + keyOf(job)), refusal);
    assertTrue(refusal.contains("stores source_path_sha"), refusal);
  }

  /**
   * A job naming its key list with spaces runs again on the table its first run created: the stored key is the list in its required
   * form, which the job's list matches.
   */
  @Test
  void jobNamingItsKeyWithSpacesRunsAgainOnItsTable() throws Exception {
    announce(text("a", "alpha"));
    TypedProperties props = job(UnstructuredMode.BLOB);
    props.setProperty(RECORD_KEY, " source_path_sha , source_modification_time,source_file_size");
    runBlobJob(props);
    announce(text("b", "beta"));
    runBlobJob(props);
    assertEquals(2, EmbeddingStreamers.commits(blobTable(), storage()).size());
    assertEquals(keyOf(UnstructuredMode.BLOB), tableConfig(blobTable()).getRecordKeyFieldProp());
  }

  /** Hudi's default Parquet codec for this engine and Spark version (the embedding table never overrides it). */
  private CompressionCodecName hudiDefaultCodec() {
    return CompressionCodecName.fromConf(HoodieWriteConfig.newBuilder().withPath(table()).build().getParquetCompressionCodec());
  }
}

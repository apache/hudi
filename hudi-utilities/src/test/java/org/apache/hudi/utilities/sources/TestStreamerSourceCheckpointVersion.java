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
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;
import org.apache.hudi.utilities.config.JdbcSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen.KinesisShardRange;
import org.apache.hudi.utilities.sources.helpers.gcs.PubsubMessagesFetcher;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates that every non-incremental streamer source emits a V1 checkpoint regardless of the
 * configured write table version or the wrapper class of the input checkpoint (so V2 keys
 * persisted by older releases are read on input but never written back as V2). Hudi incremental
 * sources own their own V1/V2 semantics and are exercised by
 * {@link #testS3AndGcsIncrSourcesStayV1OnBothTableVersions()} plus
 * {@code TestHoodieIncrSource} family.
 */
class TestStreamerSourceCheckpointVersion {

  private static JavaSparkContext jsc;
  private static SparkSession spark;

  @TempDir
  static java.nio.file.Path tempDir;

  @BeforeAll
  static void initSpark() {
    spark = SparkSession.builder().master("local[1]").appName("TestSourceCheckpointVersion").getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
  }

  @AfterAll
  static void stopSpark() {
    if (jsc != null) {
      jsc.stop();
    }
    if (spark != null) {
      spark.stop();
    }
  }

  // Covers AvroDFSSource, JsonDFSSource, CsvDFSSource, ParquetDFSSource, ORCDFSSource.
  @ParameterizedTest
  @ValueSource(ints = {6, 8})
  void testDfsPathSelector(int writeTableVersion) throws IOException {
    TypedProperties props = propsWith(writeTableVersion);
    props.setProperty(DFSPathSelectorConfig.ROOT_INPUT_PATH.key(), tempDir.toString());
    Configuration hadoopConf = jsc.hadoopConfiguration();
    FileSystem fs = FileSystem.get(hadoopConf);
    fs.mkdirs(new Path(tempDir.toString()));
    DFSPathSelector selector = new DFSPathSelector(props, hadoopConf);
    Pair<Option<String>, Checkpoint> result =
        selector.getNextFilePathsAndMaxModificationTime(jsc, Option.empty(), Long.MAX_VALUE);
    assertV1(result.getRight());
  }

  // Covers AvroKafkaSource, JsonKafkaSource, ProtoKafkaSource.
  @ParameterizedTest
  @CsvSource({"6, V1", "6, V2", "8, V1", "8, V2"})
  void testKafkaSource(int writeTableVersion, InputCheckpointKind inputKind) {
    TestableKafkaSource source = new TestableKafkaSource(propsWith(writeTableVersion), jsc, spark);
    KafkaOffsetGen offsetGen = mock(KafkaOffsetGen.class);
    when(offsetGen.getNextOffsetRanges(any(), anyLong(), any())).thenReturn(
        new OffsetRange[] {OffsetRange.create("t", 0, 0L, 0L)});
    when(offsetGen.getTopicName()).thenReturn("t");
    source.offsetGen = offsetGen;
    InputBatch<String> batch = source.fetchNext(makeInputCheckpoint(inputKind, "k"), 1L);
    assertV1(batch.getCheckpointForNextBatch());
  }

  // Covers JsonKinesisSource.
  @ParameterizedTest
  @CsvSource({"6, V1", "6, V2", "8, V1", "8, V2"})
  void testKinesisSource(int writeTableVersion, InputCheckpointKind inputKind) {
    TestableKinesisSource source = new TestableKinesisSource(propsWith(writeTableVersion), jsc, spark);
    KinesisOffsetGen offsetGen = mock(KinesisOffsetGen.class);
    when(offsetGen.getStreamName()).thenReturn("s");
    when(offsetGen.getNextShardRanges(any(), anyLong())).thenReturn(
        new KinesisShardRange[0]);
    source.setOffsetGen(offsetGen);
    InputBatch<JavaRDD<String>> batch = source.fetchNext(makeInputCheckpoint(inputKind, "s"), 1L);
    assertV1(batch.getCheckpointForNextBatch());
  }

  @ParameterizedTest
  @CsvSource({"6, V1", "6, V2", "8, V1", "8, V2"})
  void testJdbcSource(int writeTableVersion, InputCheckpointKind inputKind) throws Exception {
    JdbcSource source = new JdbcSource(propsWith(writeTableVersion), jsc, spark, null);
    Method m = JdbcSource.class.getDeclaredMethod(
        "checkpoint", Dataset.class, boolean.class, Option.class);
    m.setAccessible(true);
    Checkpoint c = (Checkpoint) m.invoke(source, null, false, makeInputCheckpoint(inputKind, "k"));
    assertV1(c);
  }

  @ParameterizedTest
  @CsvSource({"6, V1", "6, V2", "8, V1", "8, V2"})
  void testSqlFileBasedSource(int writeTableVersion, InputCheckpointKind inputKind) throws IOException {
    java.nio.file.Path sqlFile = tempDir.resolve("q.sql");
    Files.write(sqlFile, "SELECT 1".getBytes());
    TypedProperties props = propsWith(writeTableVersion);
    props.setProperty("hoodie.streamer.source.sql.file", sqlFile.toString());
    props.setProperty("hoodie.streamer.source.sql.checkpoint.emit", "true");
    SqlFileBasedSource source = new SqlFileBasedSource(props, jsc, spark, null);
    Pair<Option<Dataset<Row>>, Checkpoint> result =
        invokeRowSourceFetch(source, makeInputCheckpoint(inputKind, "k"));
    assertV1(result.getRight());
  }

  @ParameterizedTest
  @CsvSource({"6, V1", "6, V2", "8, V1", "8, V2"})
  void testHiveIncrPullSource(int writeTableVersion, InputCheckpointKind inputKind) throws Exception {
    Files.createDirectories(tempDir.resolve("20200101000000"));
    TypedProperties props = propsWith(writeTableVersion);
    props.setProperty("hoodie.streamer.source.incrpull.root", tempDir.toString());
    HiveIncrPullSource source = new HiveIncrPullSource(props, jsc, spark, null);
    Method m = HiveIncrPullSource.class.getDeclaredMethod("findCommitToPull", Option.class);
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    Option<Checkpoint> result = (Option<Checkpoint>) m.invoke(
        source, makeInputCheckpoint(inputKind, "00000000000000"));
    assertV1(result.get());
  }

  @ParameterizedTest
  @CsvSource({"6, V1", "6, V2", "8, V1", "8, V2"})
  void testGcsEventsSource(int writeTableVersion, InputCheckpointKind inputKind) {
    PubsubMessagesFetcher fetcher = mock(PubsubMessagesFetcher.class);
    when(fetcher.fetchMessages()).thenReturn(Collections.emptyList());
    TypedProperties props = propsWith(writeTableVersion);
    props.setProperty("hoodie.streamer.source.gcs.project.id", "p");
    props.setProperty("hoodie.streamer.source.gcs.subscription.id", "s");
    GcsEventsSource source = new GcsEventsSource(props, jsc, spark, null, fetcher);
    Pair<Option<Dataset<Row>>, Checkpoint> result =
        invokeRowSourceFetch(source, makeInputCheckpoint(inputKind, "k"));
    assertV1(result.getRight());
  }

  @ParameterizedTest
  @EnumSource(InputCheckpointKind.class)
  void testJdbcSourceIncrementalWithMaxValueEmitsV1(InputCheckpointKind inputKind) throws Exception {
    TypedProperties props = propsWith(8);
    props.setProperty(JdbcSourceConfig.INCREMENTAL_COLUMN.key(), "idx");
    JdbcSource source = new JdbcSource(props, jsc, spark, null);
    StructType schema = new StructType().add("idx", DataTypes.StringType, true);
    List<Row> rows = Arrays.asList(RowFactory.create("100"), RowFactory.create("200"));
    Dataset<Row> dataset = spark.createDataFrame(rows, schema);
    Method m = JdbcSource.class.getDeclaredMethod(
        "checkpoint", Dataset.class, boolean.class, Option.class);
    m.setAccessible(true);
    Checkpoint c = (Checkpoint) m.invoke(source, dataset, true, makeInputCheckpoint(inputKind, "k"));
    assertV1(c);
    assertEquals("200", c.getCheckpointKey());
  }

  // Drives the isIncremental + max==null pass-through to confirm it re-wraps a V2 input as V1.
  @ParameterizedTest
  @EnumSource(InputCheckpointKind.class)
  void testJdbcSourcePassThroughEmitsV1(InputCheckpointKind inputKind) throws Exception {
    TypedProperties props = propsWith(8);
    props.setProperty(JdbcSourceConfig.INCREMENTAL_COLUMN.key(), "idx");
    JdbcSource source = new JdbcSource(props, jsc, spark, null);
    StructType schema = new StructType().add("idx", DataTypes.StringType, true);
    List<Row> rows = Collections.singletonList(RowFactory.create((Object) null));
    Dataset<Row> nullColDataset = spark.createDataFrame(rows, schema);
    Method m = JdbcSource.class.getDeclaredMethod(
        "checkpoint", Dataset.class, boolean.class, Option.class);
    m.setAccessible(true);
    Checkpoint c = (Checkpoint) m.invoke(source, nullColDataset, true, makeInputCheckpoint(inputKind, "k"));
    assertV1(c);
  }

  // Input key sorts after the only commit on disk so findCommitToPull returns empty and
  // readFromCheckpoint enters its pass-through branch.
  @ParameterizedTest
  @EnumSource(InputCheckpointKind.class)
  void testHiveIncrPullSourcePassThroughEmitsV1(InputCheckpointKind inputKind) throws IOException {
    Files.createDirectories(tempDir.resolve("20200101000000"));
    TypedProperties props = propsWith(8);
    props.setProperty("hoodie.streamer.source.incrpull.root", tempDir.toString());
    HiveIncrPullSource source = new HiveIncrPullSource(props, jsc, spark, null);
    InputBatch<?> batch = source.readFromCheckpoint(
        makeInputCheckpoint(inputKind, "30000000000000"), 1L);
    assertV1(batch.getCheckpointForNextBatch());
  }

  @ParameterizedTest
  @CsvSource({"6, V1", "6, V2", "8, V1", "8, V2"})
  void testTranslateCheckpointNormalizesToV1(int writeTableVersion, InputCheckpointKind inputKind) {
    TestableKafkaSource source = new TestableKafkaSource(propsWith(writeTableVersion), jsc, spark);
    Option<Checkpoint> translated = source.translateCheckpoint(makeInputCheckpoint(inputKind, "k"));
    assertV1(translated.get());
    assertEquals("k", translated.get().getCheckpointKey());
  }

  @ParameterizedTest
  @ValueSource(ints = {6, 8})
  void testTranslateCheckpointPreservesEmpty(int writeTableVersion) {
    TestableKafkaSource source = new TestableKafkaSource(propsWith(writeTableVersion), jsc, spark);
    assertTrue(source.translateCheckpoint(Option.empty()).isEmpty());
  }

  @Test
  void testS3AndGcsIncrSourcesStayV1OnBothTableVersions() {
    String s3 = S3EventsHoodieIncrSource.class.getName();
    String gcs = GcsEventsHoodieIncrSource.class.getName();
    assertTrue(CheckpointUtils.DATASOURCES_NOT_SUPPORTED_WITH_CKPT_V2.contains(s3));
    assertTrue(CheckpointUtils.DATASOURCES_NOT_SUPPORTED_WITH_CKPT_V2.contains(gcs));
    assertFalse(CheckpointUtils.shouldTargetCheckpointV2(8, s3));
    assertFalse(CheckpointUtils.shouldTargetCheckpointV2(8, gcs));
  }

  private static TypedProperties propsWith(int writeTableVersion) {
    TypedProperties props = new TypedProperties();
    props.setProperty(WRITE_TABLE_VERSION.key(), String.valueOf(writeTableVersion));
    return props;
  }

  private static void assertV1(Checkpoint c) {
    assertEquals(StreamerCheckpointV1.class, c.getClass());
  }

  @SuppressWarnings("unchecked")
  private static Pair<Option<Dataset<Row>>, Checkpoint> invokeRowSourceFetch(
      RowSource source, Option<Checkpoint> lastCheckpoint) {
    try {
      Method m = source.getClass().getDeclaredMethod("fetchNextBatch", Option.class, long.class);
      m.setAccessible(true);
      return (Pair<Option<Dataset<Row>>, Checkpoint>) m.invoke(source, lastCheckpoint, 1L);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private enum InputCheckpointKind { V1, V2 }

  private static Option<Checkpoint> makeInputCheckpoint(InputCheckpointKind kind, String key) {
    switch (kind) {
      case V1: return Option.of(new StreamerCheckpointV1(key));
      case V2: return Option.of(new StreamerCheckpointV2(key));
      default: throw new IllegalArgumentException("Unsupported kind: " + kind);
    }
  }

  private static class TestableKafkaSource extends KafkaSource<String> {
    TestableKafkaSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark) {
      super(props, jsc, spark, SourceType.JSON, mock(HoodieIngestionMetrics.class),
          new DefaultStreamContext(null, Option.empty()));
    }

    @Override
    protected String toBatch(OffsetRange[] offsetRanges) {
      return "batch";
    }
  }

  private static class TestableKinesisSource extends KinesisSource<JavaRDD<String>> {
    TestableKinesisSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark) {
      super(props, jsc, spark, SourceType.JSON, mock(HoodieIngestionMetrics.class),
          new DefaultStreamContext((SchemaProvider) null, Option.empty()));
    }

    void setOffsetGen(KinesisOffsetGen gen) {
      this.offsetGen = gen;
    }

    @Override
    protected JavaRDD<String> toBatch(KinesisShardRange[] shardRanges, long sourceLimit) {
      return jsc.emptyRDD();
    }

    @Override
    protected String createCheckpointFromBatch(JavaRDD<String> batch,
        KinesisShardRange[] shardRangesWithUnreadRecords,
        KinesisShardRange[] allOpenClosedShardRanges) {
      return "checkpoint";
    }

    @Override
    protected long getRecordCount(JavaRDD<String> batch) {
      return 1L;
    }
  }
}

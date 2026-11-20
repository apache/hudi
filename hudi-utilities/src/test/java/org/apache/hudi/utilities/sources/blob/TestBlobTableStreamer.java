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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.SqlTransformerConfig;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.ParquetFooters;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Actual single-writer commits over a real S3 events Hudi table. */
@Tag("functional")
public class TestBlobTableStreamer extends UtilitiesTestBase {
  private static final long THRESHOLD = 64;

  @TempDir
  Path tempDir;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  private BlobJobHarness harness() {
    return BlobJobHarness.under(tempDir.resolve("src")).install(jsc.hadoopConfiguration());
  }

  private String table() {
    return tempDir.resolve("table").toUri().toString();
  }

  private static void runStreamer(TypedProperties props, String table, long limit) throws Exception {
    runStreamer(props, table, limit, HoodieTableType.COPY_ON_WRITE);
  }

  private static void runStreamer(TypedProperties props, String table, long limit, HoodieTableType tableType) throws Exception {
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table, tableType);
    cfg.sourceLimit = limit;
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  private static HoodieTableMetaClient metaClient(String table) {
    return HoodieTableMetaClient.builder().setBasePath(table)
        .setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build();
  }

  private static List<HoodieInstant> commits(String table) {
    return metaClient(table).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  private static String checkpoint(String table) throws Exception {
    List<HoodieInstant> commits = commits(table);
    return metaClient(table).getActiveTimeline().readCommitMetadata(commits.get(commits.size() - 1))
        .getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1);
  }

  private static List<Row> rows(String table) {
    return sparkSession.read().format("hudi").load(table).orderBy(BlobTableSchema.SOURCE_FILE_NAME).collectAsList();
  }

  private static byte[] storedBytes(String table, Row row) throws Exception {
    Row blob = row.getAs(BlobTableSchema.BLOB);
    if (HoodieSchema.Blob.INLINE.equals(blob.getString(0))) {
      assertNull(blob.get(2));
      return (byte[]) blob.get(1);
    }
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, blob.getString(0));
    assertNull(blob.get(1));
    // a large object stays where it is: an unmanaged reference to its own URI, as listed, never a copy under the table
    Row reference = blob.getStruct(2);
    assertEquals(row.<String>getAs(BlobTableSchema.SOURCE_FILE_PATH), reference.getString(0));
    assertNull(reference.get(1));
    assertNull(reference.get(2));
    assertFalse(reference.getBoolean(3));
    org.apache.hadoop.fs.Path source = new org.apache.hadoop.fs.Path(reference.getString(0));
    try (org.apache.hadoop.fs.FSDataInputStream in = source.getFileSystem(jsc.hadoopConfiguration()).open(source)) {
      return in.readAllBytes();
    }
  }

  /** No committed row of the blob table has a null blob: an object without bytes is quarantined, never committed. */
  private static void assertNoNullBlob(String table) {
    List<String> nullBlobs = sparkSession.read().format("hudi").load(table).filter(functions.col(BlobTableSchema.BLOB).isNull())
        .select(BlobTableSchema.SOURCE_FILE_PATH).collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    assertEquals(Collections.emptyList(), nullBlobs, "committed rows with a null blob");
  }

  /** Every row is the current version of its object: one row per expected key, keyed by the object's version tag. */
  private void assertContent(String table, Map<String, byte[]> expected) throws Exception {
    assertNoNullBlob(table);
    List<Row> rows = rows(table);
    assertEquals(expected.size(), rows.size());
    BlobJobHarness harness = harness();
    for (Row row : rows) {
      String path = row.getAs(BlobTableSchema.SOURCE_FILE_PATH);
      String key = path.substring(BlobJobHarness.sourcePath("").length());
      byte[] bytes = expected.get(key);
      assertTrue(bytes != null, path);
      String sha = BlobFileNames.sourcePathSha(path);
      assertEquals(sha, row.getAs(BlobTableSchema.SOURCE_PATH_SHA));
      assertEquals(Files.getLastModifiedTime(harness.file(key)).toMillis(), row.<Long>getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME));
      assertEquals(harness.versionKey(key), row.getAs("_hoodie_record_key"));
      assertEquals(BlobFileNames.fileName(path), row.getAs(BlobTableSchema.SOURCE_FILE_NAME));
      String extension = BlobFileNames.extension(path);
      assertEquals(extension, row.getAs(BlobTableSchema.SOURCE_FILE_EXTENSION));
      assertEquals(BlobFileNames.contentType(extension), row.getAs(BlobTableSchema.CONTENT_TYPE));
      assertEquals((long) bytes.length, row.<Long>getAs(BlobTableSchema.SOURCE_FILE_SIZE));
      byte[] actual = storedBytes(table, row);
      assertArrayEquals(bytes, actual, key);
      assertEquals(BlobJobHarness.sha256Hex(bytes), BlobJobHarness.sha256Hex(actual), key);
    }
  }

  /** Whether the blob job created the table's out-of-line folder: it never copies, so it never should. */
  private static boolean blobFolderExists(String table) {
    return Files.exists(Paths.get(java.net.URI.create(table + "/.hoodie/.blob")));
  }

  @Test
  public void ingestsIncrementallyWithBlobStorageRule() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("docs/a.pdf", 32);
    sizes.put("img/b.png", 1024);
    sizes.put("video/c.mp4", 256);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, sizes));
    TypedProperties props = harness.jobProperties(THRESHOLD);
    runStreamer(props, table(), Long.MAX_VALUE);
    assertContent(table(), expected);
    expected.putAll(harness.putAndAnnounce(sparkSession, Collections.singletonMap("docs/d.pdf", 10)));
    runStreamer(props, table(), Long.MAX_VALUE);
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(2, commits(table()).size(), "no-new-data starts no additional commit");
    assertContent(table(), expected);
    HoodieSchema schema = new TableSchemaResolver(metaClient(table())).getTableSchema(false);
    assertEquals(new HashSet<>(Arrays.asList(BlobTableSchema.schema().fieldNames())),
        schema.getFields().stream().map(field -> field.name()).collect(Collectors.toSet()));
    assertTrue(schema.toAvroSchema().getField(BlobTableSchema.BLOB).schema().toString().contains("\"logicalType\":\"blob\""));
    assertEquals(BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + "," + BlobTableSchema.SOURCE_FILE_SIZE,
        metaClient(table()).getTableConfig().getRecordKeyFieldProp());
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
  }

  /** Stock key-generator inference for no partition column and no key generator: the non-partitioned generator. */
  @Test
  public void nonPartitionedJobWithNoKeyGeneratorWritesNonPartitionedRows() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("docs/flat.pdf", 32);
    sizes.put("img/large.png", 128);
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, sizes);
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.remove("hoodie.datasource.write.partitionpath.field");
    props.remove("hoodie.datasource.write.keygenerator.class");
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(KeyGeneratorType.NON_PARTITION.name(), metaClient(table()).getTableConfig().getString(HoodieTableConfig.KEY_GENERATOR_TYPE));
    assertContent(table(), expected);
    for (Row row : rows(table())) {
      assertEquals("", row.getAs("_hoodie_partition_path"));
    }
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
  }

  /** Stock key-generator inference for one configured partition column and the three key columns: the complex generator. */
  @Test
  public void onePartitionFieldWithNoKeyGeneratorWritesComplexPartitionedRows() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, Collections.singletonMap("docs/by-extension.pdf", 128));
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.remove("hoodie.datasource.write.keygenerator.class");
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(KeyGeneratorType.COMPLEX.name(), metaClient(table()).getTableConfig().getString(HoodieTableConfig.KEY_GENERATOR_TYPE));
    assertContent(table(), expected);
    assertEquals("pdf", rows(table()).get(0).getAs("_hoodie_partition_path"));
  }

  @Test
  public void splitRoundRestartResumesStockV1Checkpoint() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("a.pdf", 32);
    sizes.put("b.pdf", 32);
    sizes.put("c.pdf", 32);
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, sizes);
    String event = commits(harness.eventsPath()).get(0).requestedTime();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.setProperty(CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.key(), "2");
    runStreamer(props, table(), 64);
    assertEquals(event + "#b.pdf", checkpoint(table()));
    assertEquals(2, rows(table()).size());
    runStreamer(props, table(), 64);
    assertEquals(event + "#c.pdf", checkpoint(table()));
    assertContent(table(), expected);
    runStreamer(props, table(), 64);
    assertEquals(2, commits(table()).size());
  }

  /**
   * The quarantine transformer runs in every round of the blob source, whatever --transformer-class names: a job naming no
   * transformer, only its own, or the quarantine transformer more than once still has one large object referenced in place and
   * every byte stored.
   * The job's own transformer runs at its configured position (the SQL transformer's column is in the table).
   */
  @ParameterizedTest
  @ValueSource(strings = {"", "org.apache.hudi.utilities.transform.SqlQueryBasedTransformer",
      "org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer,org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer",
      "org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer,org.apache.hudi.utilities.transform.SqlQueryBasedTransformer,"
          + "org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer"})
  public void quarantineTransformerRunsWhetherOrNotTheJobNamesIt(String transformers) throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("small.pdf", 32);
    sizes.put("large.pdf", 1024);
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, sizes);
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table());
    cfg.transformerClassNames = transformers.isEmpty() ? null : Arrays.asList(transformers.split(","));
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), "SELECT *, 'applied' AS job_marker FROM <SRC>");
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
    assertEquals(1, commits(table()).size());
    assertContent(table(), expected);
    assertFalse(blobFolderExists(table()), "the large object is referenced where it is");
    List<String> columns = Arrays.asList(sparkSession.read().format("hudi").load(table()).columns());
    assertEquals(transformers.contains("SqlQueryBasedTransformer"), columns.contains("job_marker"), columns.toString());
  }

  /**
   * The embedding transformer belongs to the embedding source and is refused by the blob source at start, before any object
   * is read or any instant starts.
   */
  @ParameterizedTest
  @ValueSource(strings = {"org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer"})
  public void chainTheBlobSourceCannotRunIsRefusedAtStart(String transformers) throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("small.pdf", 32));
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table());
    cfg.transformerClassNames = Arrays.asList(transformers.split(","));
    String refusal = refusedAtStart(cfg, harness.jobProperties(THRESHOLD), HoodieTransformPlanException.class);
    assertTrue(refusal.contains("UnstructuredDataSource does not run with"), refusal);
    assertTrue(refusal.contains(EmbeddingTableTransformer.class.getName()), refusal);
  }

  /** The refusal of a job at start: its message, and the table has no instant. */
  private String refusedAtStart(HoodieStreamer.Config cfg, TypedProperties props, Class<? extends Exception> type) throws Exception {
    Exception failure = assertThrows(Exception.class, () -> new HoodieStreamer(cfg, jsc, Option.of(props)).sync());
    // the streamer keeps each failed source constructor's exception as a suppressed one
    String refusal = EmbeddingStreamers.causeOf(failure, type)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
    // the streamer initializes the table before it builds the source; the refusal leaves it with no instant in any state
    assertTrue(Files.exists(Paths.get(java.net.URI.create(cfg.targetBasePath + "/.hoodie/hoodie.properties"))), cfg.targetBasePath);
    assertEquals(Collections.emptyList(), metaClient(cfg.targetBasePath).getActiveTimeline().getInstants(), "no instant was started");
    return refusal;
  }

  /**
   * The blob table's Parquet settings are the job's (blob-table.properties: uncompressed, no record-key bloom filter), and the
   * source does not change them.
   */
  @Test
  public void parquetSettingsAreTheJobs() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("small.pdf", 32));
    runStreamer(harness.jobProperties(THRESHOLD), table(), Long.MAX_VALUE);
    ParquetFooters.assertBaseFiles(jsc.hadoopConfiguration(), table(), CompressionCodecName.UNCOMPRESSED, false);
  }

  /**
   * A table keeps its job: the embedding job on a blob table is refused at start, naming the record key, whether it sets the blob
   * table's key itself or leaves it unset; the blob job on an embedding table likewise. Nothing is written.
   */
  @Test
  public void modeSwitchOnExistingTableRefused() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32));
    runStreamer(harness.jobProperties(THRESHOLD), table(), Long.MAX_VALUE);
    assertEquals(1, commits(table()).size());
    HoodieStreamer.Config embeddingCfg = BlobJobHarness.streamerConfig(table());
    embeddingCfg.transformerClassNames = Collections.singletonList(EmbeddingTableTransformer.class.getName());

    TypedProperties keySet = harness.jobProperties(THRESHOLD);
    keySet.setProperty("hoodie.unstructured.source.mode", "embedding");
    keySet.setProperty("hoodie.unstructured.embedding.mode", "prep_only");
    String setRefusal = modeSwitchRefusal(embeddingCfg, keySet);
    assertTrue(setRefusal.startsWith("UnstructuredDataSource (mode embedding) requires hoodie.datasource.write.recordkey.field="),
        setRefusal);

    TypedProperties keyUnset = TypedProperties.copy(keySet);
    keyUnset.remove("hoodie.datasource.write.recordkey.field");
    String unsetRefusal = modeSwitchRefusal(embeddingCfg, keyUnset);
    assertTrue(unsetRefusal.startsWith("UnstructuredDataSource (mode embedding) requires the table's hoodie.table.recordkey.fields="),
        unsetRefusal);
    assertEquals(1, commits(table()).size(), "nothing was written to the blob table");

    String embeddingTable = tempDir.resolve("embedding-table").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("blob_table")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .setPartitionFields(BlobTableSchema.SOURCE_FILE_EXTENSION)
        .setKeyGeneratorClassProp("org.apache.hudi.keygen.ComplexKeyGenerator")
        .initTable(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), embeddingTable);
    TypedProperties blob = harness.jobProperties(THRESHOLD);
    blob.remove("hoodie.datasource.write.recordkey.field");
    String blobRefusal = modeSwitchRefusal(BlobJobHarness.streamerConfig(embeddingTable), blob);
    assertTrue(blobRefusal.startsWith("UnstructuredDataSource (mode blob) requires the table's hoodie.table.recordkey.fields="),
        blobRefusal);
    assertTrue(commits(embeddingTable).isEmpty());
  }

  private static String modeSwitchRefusal(HoodieStreamer.Config cfg, TypedProperties props) {
    Exception failure = assertThrows(Exception.class, () -> new HoodieStreamer(cfg, jsc, Option.of(props)).sync());
    return EmbeddingStreamers.causeOf(failure, IllegalArgumentException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
  }

  /** A preceding user transformer can mark one row invalid without altering source IO policy. */
  public static class NullOneBlobTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext context, SparkSession session, Dataset<Row> rows, TypedProperties props) {
      return rows.withColumn(BlobTableSchema.BLOB, functions.when(functions.col(BlobTableSchema.SOURCE_FILE_NAME).equalTo("bad.pdf"),
          functions.lit(null).cast(rows.schema().apply(BlobTableSchema.BLOB).dataType())).otherwise(functions.col(BlobTableSchema.BLOB)));
    }
  }

  /** The job's own transformers run only after the quarantine transformer: one placed before it is refused at start. */
  @Test
  public void transformerBeforeTheQuarantineTransformerIsRefusedAtStart() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("good.pdf", 128));
    TypedProperties props = BlobJobHarness.withErrorTable(harness.jobProperties(THRESHOLD), tempDir.resolve("errors").toString());
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table());
    cfg.transformerClassNames = Arrays.asList(NullOneBlobTransformer.class.getName(), BlobQuarantineTransformer.class.getName());
    Exception failure = assertThrows(Exception.class, () -> new HoodieStreamer(cfg, jsc, Option.of(props)).sync());
    String causes = EmbeddingStreamers.causes(failure);
    assertTrue(causes.contains("runs the job's own transformers only after " + BlobQuarantineTransformer.class.getName()), causes);
    assertTrue(commits(table()).isEmpty(), "nothing was written");
  }

}

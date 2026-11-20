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

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.SqlTransformerConfig;
import org.apache.hudi.utilities.sources.TestUnstructuredDataSourceTargetSchema.UserTargetSchemaProvider;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSchemaContract;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A customer's own transformers on the unstructured jobs: run after the required transformer, they may add columns, which reach
 * the table; a chain that would break the table's contract is refused at start, and a round whose rows break it is rolled back.
 */
@Tag("functional")
public class TestUnstructuredCustomerTransformers extends UtilitiesTestBase {

  private static final String ADD_NOTE = "SELECT *, CAST('note' AS STRING) AS ingest_note FROM <SRC>";
  private static final String SQL = SqlQueryBasedTransformer.class.getName();

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

  @AfterEach
  public void resetSchema() {
    UserTargetSchemaProvider.target = null;
  }

  private BlobJobHarness harness() {
    return BlobJobHarness.under(tempDir.resolve("src")).install(jsc.hadoopConfiguration());
  }

  private String table(String name) {
    return tempDir.resolve(name).toUri().toString();
  }

  private static void sync(HoodieStreamer.Config cfg, TypedProperties props) throws Exception {
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  private static HoodieTableMetaClient metaClient(String table) {
    return HoodieTableMetaClient.builder().setBasePath(table).setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build();
  }

  private static List<HoodieInstant> commits(String table) {
    return metaClient(table).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  private static String refusal(Exception failure) {
    return EmbeddingStreamers.causeOf(failure, HoodieSchemaException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
  }

  /** The job of {@code mode} over the harness's events table, writing {@code table}, with {@code transformers} configured. */
  private static final class Job {
    final HoodieStreamer.Config cfg;
    final TypedProperties props;

    Job(HoodieStreamer.Config cfg, TypedProperties props) {
      this.cfg = cfg;
      this.props = props;
    }
  }

  private Job job(UnstructuredMode mode, BlobJobHarness harness, String table, String... transformers) throws IOException {
    HoodieStreamer.Config cfg;
    TypedProperties props;
    if (mode == UnstructuredMode.BLOB) {
      cfg = BlobJobHarness.streamerConfig(table);
      props = harness.jobProperties(64);
    } else {
      cfg = EmbeddingStreamers.config(table);
      cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
      props = EmbeddingStreamers.jobProps(table, harness.eventsPath(), BucketFolderFileSystem.SCHEME, 1024 * 1024,
          EmbeddingTableConfig.MODE_PREP_ONLY, new TypedProperties());
    }
    cfg.transformerClassNames = Arrays.asList(transformers);
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), ADD_NOTE);
    return new Job(cfg, props);
  }

  private static S3EventsTableFixture.Event put(UnstructuredMode mode, BlobJobHarness harness, String key) throws IOException {
    return mode == UnstructuredMode.BLOB ? harness.put(key, BlobJobHarness.fileBytes(key, 1024))
        : harness.put(key, PdfFixtures.text("Omicron pi rho sigma " + key + "."));
  }

  /** The user's target schema: the table's contract and the column the customer's transformer adds. */
  private static StructType targetWithNote(UnstructuredMode mode, TypedProperties props) {
    return UnstructuredSchemaContract.contract(mode, props, Option.empty())
        .add(DataTypes.createStructField("ingest_note", DataTypes.StringType, true));
  }

  @ParameterizedTest(name = "{0} target schema {1}")
  @CsvSource({"BLOB,false", "BLOB,true", "EMBEDDING,false", "EMBEDDING,true"})
  public void customerColumnReachesTheTableInBothRounds(UnstructuredMode mode, boolean withTarget) throws Exception {
    BlobJobHarness harness = harness();
    String table = table(mode + "-" + withTarget);
    Job job = job(mode, harness, table, SQL);
    if (withTarget) {
      job.cfg.schemaProviderClassName = UserTargetSchemaProvider.class.getName();
      UserTargetSchemaProvider.target = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(targetWithNote(mode, job.props),
          "unstructured_record", "hoodie.unstructured");
    }
    harness.announce(sparkSession, Collections.singletonList(put(mode, harness, "docs/a.pdf")));
    sync(job.cfg, job.props);
    harness.announce(sparkSession, Collections.singletonList(put(mode, harness, "docs/b.pdf")));
    sync(job.cfg, job.props);

    assertEquals(2, commits(table).size());
    List<Row> notes = sparkSession.read().format("hudi").load(table).select("source_file_name", "ingest_note").collectAsList();
    assertEquals(Arrays.asList("a.pdf", "b.pdf"), notes.stream().map(row -> row.getString(0)).distinct().sorted().collect(Collectors.toList()));
    assertTrue(notes.stream().allMatch(row -> "note".equals(row.getString(1))), "the column is filled in both rounds: " + notes);
  }

  /**
   * A customer who stops running the transformer that added a column keeps a writable table: the embedding round writes the
   * table's column, null; the blob round's rows lack it, as any structured stream's would.
   */
  @Test
  public void embeddingRoundAfterTheCustomerTransformerIsRemovedCommits() throws Exception {
    BlobJobHarness harness = harness();
    String table = table("removed");
    Job job = job(UnstructuredMode.EMBEDDING, harness, table, SQL);
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/a.pdf")));
    sync(job.cfg, job.props);
    job.cfg.transformerClassNames = Collections.emptyList();
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/b.pdf")));
    sync(job.cfg, job.props);
    assertEquals(2, commits(table).size());
    List<Row> notes = sparkSession.read().format("hudi").load(table).select("source_file_name", "ingest_note").collectAsList();
    assertTrue(notes.stream().anyMatch(row -> "b.pdf".equals(row.getString(0)) && row.isNullAt(1)), notes.toString());
  }

  /**
   * A model declared next to the active one, whose column the table does not have yet, is not required of the round's rows: they
   * carry the table's columns and the active model's.
   */
  @Test
  public void declaredModelTheTableDoesNotHaveYetPassesTheGuard() throws Exception {
    BlobJobHarness harness = harness();
    String table = table("two-models");
    HoodieStreamer.Config cfg = EmbeddingStreamers.config(table);
    TypedProperties props = EmbeddingStreamers.jobProps(table, harness.eventsPath(), BucketFolderFileSystem.SCHEME, 1024 * 1024,
        EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, new TypedProperties());
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/a.pdf")));
    sync(cfg, props);
    String model = EmbeddingStreamers.MODEL;
    props.setProperty(EmbeddingTableConfig.MODELS.key(), model + ",later");
    props.setProperty(EmbeddingTableConfig.ACTIVE_MODELS.key(), model);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "later" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "4");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "later" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/b.pdf")));
    sync(cfg, props);
    assertEquals(2, commits(table).size());
  }

  /** A projection that drops _corrupt_record, with the error table on, is refused at start: error rows would be written as good rows. */
  @Test
  public void projectionDroppingCorruptRecordRefusedAtStart() throws Exception {
    BlobJobHarness harness = harness();
    String table = table("projection");
    Job job = job(UnstructuredMode.BLOB, harness, table, BlobQuarantineTransformer.class.getName(), SQL);
    BlobJobHarness.withErrorTable(job.props, table("projection-errors"));
    job.props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), "SELECT source_path_sha, source_modification_time, "
        + "source_file_path, source_file_name, source_file_extension, source_file_size, content_type, blob FROM <SRC>");
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.BLOB, harness, "docs/a.pdf")));
    String refused = refusal(assertThrows(Exception.class, () -> sync(job.cfg, job.props)));
    assertTrue(refused.contains("it drops the column _corrupt_record, which carries the error rows to the error table"), refused);
    assertTrue(commits(table).isEmpty(), "nothing was written");
  }

  /** The job's own transformers run only after the required one: a filter placed before it is refused at start. */
  @Test
  public void beforeRequiredFilterRefusedAtStart() throws Exception {
    BlobJobHarness harness = harness();
    String table = table("before");
    Job job = job(UnstructuredMode.BLOB, harness, table, SQL, BlobQuarantineTransformer.class.getName());
    job.props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), "SELECT * FROM <SRC> WHERE source_file_extension = 'pdf'");
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.BLOB, harness, "docs/a.pdf")));
    String refused = refusal(assertThrows(Exception.class, () -> sync(job.cfg, job.props)));
    assertTrue(refused.contains("runs the job's own transformers only after " + BlobQuarantineTransformer.class.getName()), refused);
    assertTrue(commits(table).isEmpty());
  }

  /**
   * A transformer whose plan keeps every column but whose rows drop a key column passes the start-time plan; the guard refuses the
   * round's rows before its instant starts: nothing is committed and no instant is left.
   */
  @Test
  public void guardRollsBackLyingTransformer() throws Exception {
    BlobJobHarness harness = harness();
    String table = table("lying");
    Job job = job(UnstructuredMode.BLOB, harness, table, LyingTransformer.class.getName());
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.BLOB, harness, "docs/a.pdf")));
    String refused = refusal(assertThrows(Exception.class, () -> sync(job.cfg, job.props)));
    assertTrue(refused.startsWith("UnstructuredDataSource (mode blob) refuses the rows of the round: column source_path_sha must be"),
        refused);
    HoodieTableMetaClient metaClient = metaClient(table);
    assertTrue(commits(table).isEmpty(), "the round did not commit");
    assertFalse(metaClient.getActiveTimeline().filterInflightsAndRequested().getInstants().stream()
        .anyMatch(instant -> instant.getAction().equals("commit")), "no instant is left");
    // the blob job writes nothing beside its data files
    assertEquals(0, outOfLineFiles(table), "nothing is left under .hoodie/.blob");
  }

  private static long outOfLineFiles(String table) throws IOException {
    Path blobs = Paths.get(URI.create(table)).resolve(".hoodie").resolve(".blob");
    if (!Files.exists(blobs)) {
      return 0;
    }
    try (Stream<Path> files = Files.walk(blobs)) {
      return files.filter(Files::isRegularFile).filter(f -> !f.getFileName().toString().startsWith(".")).count();
    }
  }

  /**
   * Only the declared models' columns are model columns: a prep_only table with a VECTOR column of the customer's takes its next
   * prep_only round.
   */
  @Test
  public void prepOnlyRoundPassesOnATableWithACustomerVectorColumn() throws Exception {
    BlobJobHarness harness = harness();
    String table = table("customer-vector");
    Job job = job(UnstructuredMode.EMBEDDING, harness, table, AddVectorTransformer.class.getName());
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/a.pdf")));
    sync(job.cfg, job.props);
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/b.pdf")));
    sync(job.cfg, job.props);
    assertEquals(2, commits(table).size());
    StructType stored = sparkSession.read().format("hudi").load(table).schema();
    assertTrue(Arrays.asList(stored.fieldNames()).contains(AddVectorTransformer.COLUMN), stored.treeString());
  }

  /**
   * A customer's transformer may add VECTOR and BLOB columns of its own to the embedding table, filled, as long as their names
   * conflict with no contract column: both rounds commit, and the values and the Hudi types read back.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource({EmbeddingTableConfig.MODE_PREP_ONLY, EmbeddingTableConfig.MODE_PREP_AND_COMPUTE})
  public void customerVectorAndBlobColumnsAreWrittenAndReadBack(String embeddingMode) throws Exception {
    BlobJobHarness harness = harness();
    String table = table("customer-vector-blob-" + embeddingMode);
    Job job = job(UnstructuredMode.EMBEDDING, harness, table, FillVectorAndBlobTransformer.class.getName());
    if (EmbeddingTableConfig.MODE_PREP_AND_COMPUTE.equals(embeddingMode)) {
      job.props.putAll(EmbeddingStreamers.jobProps(table, harness.eventsPath(), BucketFolderFileSystem.SCHEME, 1024 * 1024,
          embeddingMode, new TypedProperties()));
      job.props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), ADD_NOTE);
    }
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/a.pdf")));
    sync(job.cfg, job.props);
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/b.pdf")));
    sync(job.cfg, job.props);
    assertEquals(2, commits(table).size());

    Dataset<Row> stored = sparkSession.read().format("hudi").load(table);
    assertEquals("VECTOR(4)", stored.schema().apply(FillVectorAndBlobTransformer.VECTOR).metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    assertEquals(BlobTableSchema.schema().apply(BlobTableSchema.BLOB).metadata().getString(HoodieSchema.TYPE_METADATA_FIELD),
        stored.schema().apply(FillVectorAndBlobTransformer.BLOB).metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    List<Row> rows = stored.select("source_file_name", FillVectorAndBlobTransformer.VECTOR, FillVectorAndBlobTransformer.BLOB + ".type",
        FillVectorAndBlobTransformer.BLOB + ".data").collectAsList();
    assertEquals(Arrays.asList("a.pdf", "b.pdf"), rows.stream().map(row -> row.getString(0)).distinct().sorted().collect(Collectors.toList()));
    for (Row row : rows) {
      assertEquals(Arrays.asList(1f, 2f, 3f, 4f), row.getList(1), row.toString());
      assertEquals("INLINE", row.getString(2), row.toString());
      assertArrayEquals(FillVectorAndBlobTransformer.BYTES, (byte[]) row.get(3), row.toString());
    }
  }

  /** Adds a VECTOR(4) and an inline BLOB column of the customer's, both filled. */
  public static class FillVectorAndBlobTransformer implements Transformer {
    static final String VECTOR = "user_vector";
    static final String BLOB = "user_blob";
    static final byte[] BYTES = "customer bytes".getBytes(StandardCharsets.UTF_8);

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties props) {
      Metadata vector = new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4)").build();
      StructField blobField = BlobTableSchema.schema().apply(BlobTableSchema.BLOB);
      StructType blobType = (StructType) blobField.dataType();
      Column blob = functions.struct(functions.lit("INLINE").as(blobType.fields()[0].name()),
          functions.lit(BYTES).as(blobType.fields()[1].name()),
          functions.lit(null).cast(blobType.fields()[2].dataType()).as(blobType.fields()[2].name())).cast(blobType);
      return rows.withColumn(VECTOR, functions.array(functions.lit(1f), functions.lit(2f), functions.lit(3f), functions.lit(4f)), vector)
          .withColumn(BLOB, blob, blobField.metadata());
    }
  }

  /** Adds a nullable VECTOR(4) column of the customer's, left null. */
  public static class AddVectorTransformer implements Transformer {
    static final String COLUMN = "user_vector";

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties props) {
      Metadata vector = new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4)").build();
      // an array of non-null floats, left null: the type a Hudi VECTOR column takes
      return rows.withColumn(COLUMN, functions.when(functions.lit(false), functions.array(functions.lit(0f), functions.lit(0f),
          functions.lit(0f), functions.lit(0f))), vector);
    }
  }

  /**
   * The round writes with the user's target schema, so every column it writes must be in it: on an existing embedding table whose
   * round keeps {@code ingest_note}, here overwritten in place by a transformer, a target schema without it is refused at start.
   */
  @Test
  public void tableColumnTheRoundWritesMustBeInTheTargetSchema() throws Exception {
    BlobJobHarness harness = harness();
    String table = table("overwritten");
    Job job = job(UnstructuredMode.EMBEDDING, harness, table, SQL);
    job.cfg.schemaProviderClassName = UserTargetSchemaProvider.class.getName();
    UserTargetSchemaProvider.target = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        targetWithNote(UnstructuredMode.EMBEDDING, job.props), "unstructured_record", "hoodie.unstructured");
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/a.pdf")));
    sync(job.cfg, job.props);
    assertEquals(1, commits(table).size());

    job.cfg.transformerClassNames = Collections.singletonList(OverwriteNoteTransformer.class.getName());
    UserTargetSchemaProvider.target = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        UnstructuredSchemaContract.contract(UnstructuredMode.EMBEDDING, job.props, Option.empty()), "unstructured_record",
        "hoodie.unstructured");
    harness.announce(sparkSession, Collections.singletonList(put(UnstructuredMode.EMBEDDING, harness, "docs/b.pdf")));
    String refused = refusal(assertThrows(Exception.class, () -> sync(job.cfg, job.props)));
    assertTrue(refused.contains("the round writes the column ingest_note as string (added by the transformer chain or kept from the table), which the target schema does not have"), refused);
    assertEquals(1, commits(table).size(), "nothing was written");
  }

  /** Overwrites the table's {@code ingest_note} in place: the column count stays the same. */
  public static class OverwriteNoteTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties props) {
      return rows.withColumn("ingest_note", functions.lit("again"));
    }
  }

  /** Keeps every column in its plan, but drops source_path_sha from the rows it returns. */
  public static class LyingTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties props) {
      return rows.drop("source_path_sha");
    }

    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incoming, TypedProperties props) {
      return incoming;
    }
  }
}

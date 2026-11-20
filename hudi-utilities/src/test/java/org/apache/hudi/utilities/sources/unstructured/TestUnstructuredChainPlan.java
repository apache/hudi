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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.SqlTransformerConfig;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The start-time plan of an unstructured job's transformer chain: the job's own transformers run after the required one, may add
 * columns and filter rows, and may not drop or change a column they are given.
 */
public class TestUnstructuredChainPlan extends UtilitiesTestBase {

  private static final String QUARANTINE = BlobQuarantineTransformer.class.getName();
  private static final String EMBEDDING = EmbeddingTableTransformer.class.getName();
  private static final String GUARD = UnstructuredSchemaGuard.class.getName();
  private static final String SQL = SqlQueryBasedTransformer.class.getName();
  private static final String DROP = DropColumnTransformer.class.getName();

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

  private TypedProperties blob(String sql) {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "blob");
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), tempDir.resolve("table").toUri().toString());
    if (sql != null) {
      props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), sql);
    }
    return props;
  }

  private TypedProperties embedding(String sql) {
    TypedProperties props = blob(sql);
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "embedding");
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    return props;
  }

  private static void plan(UnstructuredMode mode, TypedProperties props, Option<HoodieSchema> target, String... chain) {
    UnstructuredSchemaContract.checkChainPlan(mode, mode.requiredTransformer(), UnstructuredSchemaGuard.class, Arrays.asList(chain),
        jsc, sparkSession, props, target, Option.empty());
  }

  private static String refusal(UnstructuredMode mode, TypedProperties props, String... chain) {
    return assertThrows(HoodieSchemaException.class, () -> plan(mode, props, Option.empty(), chain)).getMessage();
  }

  private static TypedProperties withErrorTable(TypedProperties props) {
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "true");
    return props;
  }

  @Test
  void afterAddPasses() {
    String add = "SELECT *, CAST('note' AS STRING) AS ingest_note FROM <SRC>";
    plan(UnstructuredMode.BLOB, blob(add), Option.empty(), QUARANTINE, SQL, GUARD);
    plan(UnstructuredMode.BLOB, withErrorTable(blob(add)), Option.empty(), QUARANTINE, SQL, GUARD);
    plan(UnstructuredMode.EMBEDDING, embedding(add), Option.empty(), EMBEDDING, SQL, GUARD);
    plan(UnstructuredMode.BLOB, blob(null), Option.empty(), QUARANTINE, GUARD);
    // a filter after the required transformer keeps every column
    plan(UnstructuredMode.BLOB, blob("SELECT * FROM <SRC> WHERE source_file_extension = 'pdf'"), Option.empty(), QUARANTINE, SQL, GUARD);
  }

  @Test
  void beforeRequiredRefused() {
    for (String sql : new String[] {"SELECT * FROM <SRC> WHERE source_file_extension = 'pdf'",
        "SELECT *, CAST('note' AS STRING) AS ingest_note FROM <SRC>"}) {
      String refused = refusal(UnstructuredMode.BLOB, blob(sql), SQL, QUARANTINE, GUARD);
      assertTrue(refused.contains("runs the job's own transformers only after " + QUARANTINE + ": --transformer-class places [" + SQL
          + "] before it"), refused);
    }
  }

  @Test
  void afterDropOfAContractColumnRefused() {
    TypedProperties props = blob(null);
    props.setProperty(DropColumnTransformer.COLUMN, "blob");
    String refused = refusal(UnstructuredMode.BLOB, props, QUARANTINE, DROP, GUARD);
    assertTrue(refused.startsWith("UnstructuredDataSource (mode blob) refuses the transformer " + DROP + ": it drops the column blob"),
        refused);
  }

  /** A projection that drops the error column would have the error rows written as good rows. */
  @Test
  void afterDropOfCorruptRecordRefusedWithTheErrorTable() {
    String projection = "SELECT source_path_sha, source_modification_time, source_file_path, source_file_name, source_file_extension,"
        + " source_file_size, content_type, blob FROM <SRC>";
    String refused = refusal(UnstructuredMode.BLOB, withErrorTable(blob(projection)), QUARANTINE, SQL, GUARD);
    assertTrue(refused.contains("it drops the column _corrupt_record, which carries the error rows"), refused);
    plan(UnstructuredMode.BLOB, blob(projection), Option.empty(), QUARANTINE, SQL, GUARD);
  }

  @Test
  void retypeRefused() {
    String refused = refusal(UnstructuredMode.BLOB,
        blob("SELECT source_path_sha, source_modification_time, source_file_path, source_file_name, source_file_extension,"
            + " CAST(source_file_size AS STRING) AS source_file_size, content_type, blob FROM <SRC>"), QUARANTINE, SQL, GUARD);
    assertTrue(refused.contains("it changes the column source_file_size from bigint to string"), refused);
  }

  @Test
  void nameCollisionRefused() {
    assertTrue(refusal(UnstructuredMode.BLOB, blob("SELECT *, 'x' AS `CONTENT_TYPE2`, 'y' AS `Source_File_Name` FROM <SRC>"),
        QUARANTINE, SQL, GUARD).contains("the column Source_File_Name has a reserved name"));
    TypedProperties partitioned = blob("SELECT *, 'x' AS `Country` FROM <SRC>");
    partitioned.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "country");
    assertTrue(refusal(UnstructuredMode.BLOB, partitioned, QUARANTINE, SQL, GUARD).contains("the column Country has a reserved name"));
  }

  /** Each {@code <id>:<class>} entry is planned with its own suffixed properties, as the round runs it. */
  @Test
  void idFormEntriesArePlannedWithTheirOwnProperties() {
    TypedProperties props = blob(null);
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key() + ".a", "SELECT *, CAST('a' AS STRING) AS note_a FROM <SRC>");
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key() + ".b", "SELECT *, CAST('b' AS STRING) AS note_b FROM <SRC>");
    plan(UnstructuredMode.BLOB, props, Option.empty(), "BlobQuarantineTransformer:" + QUARANTINE, "a:" + SQL, "b:" + SQL,
        "UnstructuredSchemaGuard:" + GUARD);
    // an unsuffixed key is not what an id-form entry runs
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), "SELECT source_path_sha FROM <SRC>");
    plan(UnstructuredMode.BLOB, props, Option.empty(), "BlobQuarantineTransformer:" + QUARANTINE, "a:" + SQL, "b:" + SQL,
        "UnstructuredSchemaGuard:" + GUARD);
  }

  /** A transformer may not add a column the contract fixes a second time. */
  @Test
  void addingAFixedColumnAgainRefused() {
    String refused = refusal(UnstructuredMode.BLOB, blob("SELECT *, CAST('x' AS STRING) AS content_type FROM <SRC>"), QUARANTINE, SQL, GUARD);
    assertTrue(refused.contains("the column content_type is given twice"), refused);
  }

  @Test
  void addedColumnMustBeInTheTargetSchema() {
    String add = "SELECT *, CAST('note' AS STRING) AS ingest_note FROM <SRC>";
    StructType contract = UnstructuredSchemaContract.contract(UnstructuredMode.BLOB, blob(null), Option.empty());
    HoodieSchema without = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(contract, "t", "ns");
    HoodieSchemaException refused = assertThrows(HoodieSchemaException.class,
        () -> plan(UnstructuredMode.BLOB, blob(add), Option.of(without), QUARANTINE, SQL, GUARD));
    assertTrue(refused.getMessage().contains("the round writes the column ingest_note as string (added by the transformer chain or kept from the table), which the target schema does not have"),
        refused.getMessage());
    HoodieSchema with = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        contract.add(DataTypes.createStructField("ingest_note", DataTypes.StringType, true)), "t", "ns");
    plan(UnstructuredMode.BLOB, blob(add), Option.of(with), QUARANTINE, SQL, GUARD);
  }

  /**
   * The round resolves a column given twice only in the closing guard, so an entry after the one that adds it again sees both
   * columns; the plan sees them too, and refuses the entry that reads the name, as the round would fail it.
   */
  @Test
  void readingAColumnAddedAgainByAnEarlierEntryRefused() {
    TypedProperties props = blob(null);
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key() + ".a", "SELECT *, CAST('a' AS STRING) AS ingest_note FROM <SRC>");
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key() + ".b", "SELECT *, CAST('b' AS STRING) AS ingest_note FROM <SRC>");
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key() + ".c", "SELECT *, upper(ingest_note) AS ingest_tag FROM <SRC>");
    String refused = refusal(UnstructuredMode.BLOB, props, "BlobQuarantineTransformer:" + QUARANTINE, "a:" + SQL, "b:" + SQL, "c:" + SQL,
        "UnstructuredSchemaGuard:" + GUARD);
    assertTrue(refused.startsWith("UnstructuredDataSource (mode blob) cannot plan the transformer c:" + SQL), refused);
    // without the reader, the column added again resolves last-wins at the end of the chain and the plan passes
    plan(UnstructuredMode.BLOB, props, Option.empty(), "BlobQuarantineTransformer:" + QUARANTINE, "a:" + SQL, "b:" + SQL,
        "UnstructuredSchemaGuard:" + GUARD);
  }

  /** A customer column the table already has, added again by the chain, must still be in the user's target schema. */
  @Test
  void columnTheTableHasMustStillBeInTheTargetSchema() {
    TypedProperties props = embedding("SELECT *, CAST('note' AS STRING) AS ingest_note FROM <SRC>");
    StructType contract = UnstructuredSchemaContract.contract(UnstructuredMode.EMBEDDING, props, Option.empty());
    StructType table = contract.add(DataTypes.createStructField("ingest_note", DataTypes.StringType, true));
    HoodieSchema without = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(contract, "t", "ns");
    HoodieSchemaException refused = assertThrows(HoodieSchemaException.class,
        () -> UnstructuredSchemaContract.checkChainPlan(UnstructuredMode.EMBEDDING, EmbeddingTableTransformer.class,
            UnstructuredSchemaGuard.class, Arrays.asList(EMBEDDING, SQL, GUARD), jsc, sparkSession, props, Option.of(without),
            Option.of(table)));
    assertTrue(refused.getMessage().contains("the round writes the column ingest_note as string (added by the transformer chain or kept from the table), which the target schema does not have"),
        refused.getMessage());
  }

  @Test
  void embeddingTransformedSchemaRunsNoSparkJob() throws Exception {
    AtomicInteger jobs = new AtomicInteger();
    SparkListener counter = new SparkListener() {
      @Override
      public void onJobStart(SparkListenerJobStart jobStart) {
        jobs.incrementAndGet();
      }
    };
    sparkSession.sparkContext().addSparkListener(counter);
    try {
      StructType blobRows = UnstructuredSchemaContract.contract(UnstructuredMode.BLOB, blob(null), Option.empty());
      StructType planned = new EmbeddingTableTransformer().transformedSchema(jsc, sparkSession, blobRows, withErrorTable(embedding(null)));
      sparkSession.sparkContext().listenerBus().waitUntilEmpty();
      assertEquals(0, jobs.get(), "planning ran a Spark job");
      StructType expected = EmbeddingTableSchema.build(EmbeddingTableConfig.from(embedding(null)))
          .add(DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true));
      assertEquals(expected, planned);
    } finally {
      sparkSession.sparkContext().removeSparkListener(counter);
    }
  }

  /** Drops the column {@value #COLUMN} names, in its plan and its output. */
  public static class DropColumnTransformer implements Transformer {
    static final String COLUMN = "test.drop.column";

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties props) {
      return rows.drop(props.getString(COLUMN));
    }
  }

  /** A partition path into a struct column that the job's own transformer adds is produced, so the plan passes (RV32-3). */
  @Test
  void nestedPartitionPathProducedByTransformerPasses() {
    TypedProperties props = blob("SELECT *, named_struct('region', CAST('eu' AS STRING)) AS ingest_meta FROM <SRC>");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "ingest_meta.region");
    plan(UnstructuredMode.BLOB, props, Option.empty(), QUARANTINE, SQL, GUARD);
  }

  /** A nested partition path that no transformer produces is still refused, naming the path. */
  @Test
  void missingNestedPartitionPathRefused() {
    TypedProperties props = blob("SELECT *, named_struct('region', CAST('eu' AS STRING)) AS ingest_meta FROM <SRC>");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "ingest_meta.country");
    String refused = refusal(UnstructuredMode.BLOB, props, QUARANTINE, SQL, GUARD);
    assertTrue(refused.contains("refuses partition column ingest_meta.country"), refused);
  }

  @Test
  void requiredBlobSubclassProducesTopLevelPartitionField() {
    TypedProperties props = blob(null);
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "region");
    plan(UnstructuredMode.BLOB, props, Option.empty(), PartitionedBlobTransformer.class.getName(), GUARD);
  }

  @Test
  void requiredBlobSubclassProducesNestedPartitionField() {
    TypedProperties props = blob(null);
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "ingest_meta.region");
    plan(UnstructuredMode.BLOB, props, Option.empty(), PartitionedBlobTransformer.class.getName(), GUARD);
  }

  @Test
  void requiredEmbeddingSubclassProducesPartitionFieldOnNewTable() {
    TypedProperties props = embedding(null);
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "region");
    plan(UnstructuredMode.EMBEDDING, props, Option.empty(), PartitionedEmbeddingTransformer.class.getName(), GUARD);
  }

  private static StructType partitionSchema(StructType schema) {
    return schema.add("region", DataTypes.StringType, true)
        .add("ingest_meta", new StructType().add("region", DataTypes.StringType, true), true);
  }

  private static Dataset<Row> partitionRows(Dataset<Row> rows) {
    return rows.withColumn("region", org.apache.spark.sql.functions.lit("eu"))
        .withColumn("ingest_meta", org.apache.spark.sql.functions.struct(org.apache.spark.sql.functions.lit("eu").alias("region")));
  }

  public static class PartitionedBlobTransformer extends BlobQuarantineTransformer {
    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession spark, StructType incoming, TypedProperties props) {
      return partitionSchema(super.transformedSchema(jsc, spark, incoming, props));
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props) {
      return partitionRows(super.apply(jsc, spark, rows, props));
    }
  }

  public static class PartitionedEmbeddingTransformer extends EmbeddingTableTransformer {
    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession spark, StructType incoming, TypedProperties props) {
      return partitionSchema(super.transformedSchema(jsc, spark, incoming, props));
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props, String instant) {
      return partitionRows(super.apply(jsc, spark, rows, props, instant));
    }
  }

  @Test
  void requiredSubclassDuplicateExtraUsesItsLastPresentPartitionField() {
    TypedProperties props = blob(null);
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "ingest_meta.region");
    props.setProperty("test.partition.first.region", "false");
    plan(UnstructuredMode.BLOB, props, Option.empty(), DuplicatePartitionBlobTransformer.class.getName(), GUARD);
  }

  @Test
  void requiredSubclassDuplicateExtraRefusesPartitionAbsentFromItsLastField() {
    TypedProperties props = blob(null);
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "ingest_meta.region");
    props.setProperty("test.partition.first.region", "true");
    String refused = refusal(UnstructuredMode.BLOB, props, DuplicatePartitionBlobTransformer.class.getName(), GUARD);
    assertTrue(refused.contains("refuses partition column ingest_meta.region"), refused);
  }

  public static class DuplicatePartitionBlobTransformer extends BlobQuarantineTransformer {
    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession spark, StructType incoming, TypedProperties props) {
      boolean regionFirst = props.getBoolean("test.partition.first.region", true);
      String first = regionFirst ? "region" : "country";
      String last = regionFirst ? "country" : "region";
      return super.transformedSchema(jsc, spark, incoming, props)
          .add("ingest_meta", new StructType().add(first, DataTypes.StringType, true), true)
          .add("ingest_meta", new StructType().add(last, DataTypes.StringType, true), true);
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props) {
      boolean regionFirst = props.getBoolean("test.partition.first.region", true);
      String first = regionFirst ? "region" : "country";
      String last = regionFirst ? "country" : "region";
      return super.apply(jsc, spark, rows, props).selectExpr("*",
          "named_struct('" + first + "', 'eu') AS ingest_meta", "named_struct('" + last + "', 'eu') AS ingest_meta");
    }
  }
}

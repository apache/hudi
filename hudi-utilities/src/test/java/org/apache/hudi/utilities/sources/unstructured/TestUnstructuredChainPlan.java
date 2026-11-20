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
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.SqlTransformerConfig;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The start-time plan of an unstructured job's transformer chain: the job's own transformers run after the required one, may add
 * columns and filter rows, and may not drop or change a column they are given.
 */
public class TestUnstructuredChainPlan extends UtilitiesTestBase {

  private static final String QUARANTINE = BlobQuarantineTransformer.class.getName();
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

  /** Drops the column {@value #COLUMN} names, in its plan and its output. */
  public static class DropColumnTransformer implements Transformer {
    static final String COLUMN = "test.drop.column";

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties props) {
      return rows.drop(props.getString(COLUMN));
    }
  }
}

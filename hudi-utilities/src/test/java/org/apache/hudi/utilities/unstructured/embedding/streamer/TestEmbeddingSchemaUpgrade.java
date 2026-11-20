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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.compute.ComputeCounters;
import org.apache.hudi.utilities.unstructured.embedding.compute.ComputeRound;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingComputePermanentFailureException;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Customer scratch-name columns and legacy writer schemas survive subsequent rounds. */
@Tag("functional")
public class TestEmbeddingSchemaUpgrade extends EmbeddingRoundTestBase {

  private static final String CUSTOMER = "_embedding_compute_error";
  private static final String CUSTOMER_SUFFIX = "_EMBEDDING_COMPUTE_ERROR_1";
  private static final String CUSTOMER_SECOND_SUFFIX = "_embedding_compute_error_2";

  /** Suffixes are customer fields too, including a case variant Spark resolves case insensitively. */
  public static class AddCustomerColumns implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props) {
      return rows.withColumn(CUSTOMER, functions.lit("customer value"))
          .withColumn(CUSTOMER_SUFFIX, functions.lit("customer suffix value"))
          .withColumn(CUSTOMER_SECOND_SUFFIX, functions.lit("customer second suffix value"));
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void customerWorkingNamesSurviveTwoRoundsAndPrepOnlyTransition(boolean prepOnlyFirst, boolean errorTable) throws Exception {
    FixtureFile first = text("a", "alpha");
    announce(first);
    TypedProperties initial = textOnlyProps(table(), prepOnlyFirst ? EmbeddingTableConfig.MODE_PREP_ONLY : EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), String.valueOf(errorTable));
    Round firstRound = EmbeddingStreamers.round(spark(), initial, cfg -> cfg.transformerClassNames = Arrays.asList(
        EmbeddingTableTransformer.class.getName(), AddCustomerColumns.class.getName()));
    assertTrue(firstRound.getCommit().isPresent());
    assertTrue(firstRound.getErrors().isEmpty());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), firstRound.getCommit().get()));

    FixtureFile second = text("b", "beta");
    announce(second);
    Round secondRound = round(textOnlyProps(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), String.valueOf(errorTable)));

    assertTrue(secondRound.getErrors().isEmpty(), secondRound.toString());
    assertEquals(2, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), secondRound.getCommit().get()));
    Map<String, Row> stored = rows(table());
    assertEquals(2, stored.size());
    Row old = stored.get(key(first, EmbeddingTableSchema.MODALITY_TEXT, 0));
    assertEquals("customer value", old.getAs(CUSTOMER));
    assertEquals("customer suffix value", old.getAs(CUSTOMER_SUFFIX));
    assertEquals("customer second suffix value", old.getAs(CUSTOMER_SECOND_SUFFIX));
    Row added = stored.get(key(second, EmbeddingTableSchema.MODALITY_TEXT, 0));
    assertNull(added.getAs(CUSTOMER));
    assertNull(added.getAs(CUSTOMER_SUFFIX));
    assertNull(added.getAs(CUSTOMER_SECOND_SUFFIX));
    assertNotNull(vector(added));
    StructType schema = EmbeddingTableSchema.writerReadSchema(table(), storage()).get();
    assertFalse(Arrays.asList(schema.fieldNames()).contains(CUSTOMER + "_3"), schema.treeString());
  }

  /** A permanent result must use the private outcome slot even when customer error columns already exist. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void permanentOutcomeUsesPrivateColumnWithEitherErrorTableSetting(boolean errorTable) throws Exception {
    announce(text("a", "alpha"));
    EmbeddingStreamers.round(spark(), textOnlyProps(table(), EmbeddingTableConfig.MODE_PREP_ONLY,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), String.valueOf(errorTable)), cfg -> cfg.transformerClassNames = Arrays.asList(
            EmbeddingTableTransformer.class.getName(), AddCustomerColumns.class.getName()));
    FixtureFile failed = text("b", "beta " + ScriptedEmbeddingClient.PERMANENT);
    announce(failed);
    TypedProperties next = textOnlyProps(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), String.valueOf(errorTable));
    if (!errorTable) {
      Exception failure = assertThrows(Exception.class, () -> round(next));
      assertTrue(EmbeddingStreamers.causeOf(failure, EmbeddingComputePermanentFailureException.class).isPresent(),
          EmbeddingStreamers.causes(failure));
      assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
      assertEquals(1, rows(table()).size());
      return;
    }
    Round computed = round(next);
    assertEquals(1, computed.getErrors().size(), computed.toString());
    assertEquals(failed.sha, computed.getErrors().get(0).get(EmbeddingTableSchema.SOURCE_PATH_SHA));
    assertEquals(EmbeddingStreamers.MODEL, computed.getErrors().get(0).get("model"));
    assertTrue(String.valueOf(computed.getErrors().get(0).get("error")).contains("injected permanent failure"));
    Row failedRow = rows(table()).get(key(failed, EmbeddingTableSchema.MODALITY_TEXT, 0));
    assertNull(vector(failedRow));
    assertNull(failedRow.getAs(CUSTOMER));
    assertEquals("beta " + ScriptedEmbeddingClient.PERMANENT, failedRow.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
  }

  /** Direct prepared rows carry non-null customer values, so even a wrong-slot write cannot be hidden by prep's NULLs. */
  @ParameterizedTest
  @ValueSource(strings = {"_embedding_compute_error", "_EMBEDDING_COMPUTE_ERROR"})
  public void computePreservesCustomerValuesAndRoutesOnlyItsOwnErrors(String customer) {
    TypedProperties props = textOnlyProps(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    EmbeddingModelSpec model = config.model(EmbeddingStreamers.MODEL);
    StructType schema = EmbeddingTableSchema.build(config).add(customer, DataTypes.StringType, true)
        .add(CUSTOMER_SUFFIX, DataTypes.StringType, true).add(CUSTOMER_SECOND_SUFFIX, DataTypes.StringType, true);
    Row good = preparedText(schema, "good", "alpha", customer);
    Row bad = preparedText(schema, "bad", ScriptedEmbeddingClient.PERMANENT, customer);
    Dataset<Row> embedded = ComputeRound.embed(spark(), spark().createDataFrame(Arrays.asList(good, bad), schema).javaRDD(),
        schema, model, props, new BlobBytesSource(storage()), ComputeCounters.register(spark().sparkContext()), StorageLevel.MEMORY_AND_DISK());
    try {
      StructType output = schema.add(ComputeRound.CORRUPT_RECORD, DataTypes.StringType, true);
      List<Row> data = ComputeRound.dataRows(embedded, output).orderBy(EmbeddingTableSchema.SOURCE_PATH_SHA).collectAsList();
      assertEquals(2, data.size());
      for (Row row : data) {
        assertEquals("customer value", row.getAs(customer));
        assertEquals("customer suffix value", row.getAs(CUSTOMER_SUFFIX));
        assertEquals("customer second suffix value", row.getAs(CUSTOMER_SECOND_SUFFIX));
        assertNull(row.getAs(ComputeRound.CORRUPT_RECORD));
      }
      List<Row> errors = ComputeRound.errorRows(embedded, output, model).select(ComputeRound.CORRUPT_RECORD).collectAsList();
      assertEquals(1, errors.size());
      assertTrue(errors.get(0).getString(0).contains("injected permanent failure"), errors.toString());
      assertTrue(errors.get(0).getString(0).contains("\"source_path_sha\":\"bad\""), errors.toString());
      assertNull(vector(data.get(0)));
      assertNotNull(vector(data.get(1)));
    } finally {
      embedded.unpersist(false);
    }
  }

  private static Row preparedText(StructType schema, String sha, String text, String customer) {
    Object[] values = new Object[schema.size()];
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)] = sha;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME)] = 1234L;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_SIZE)] = 99L;
    values[schema.fieldIndex(EmbeddingTableSchema.MODALITY)] = EmbeddingTableSchema.MODALITY_TEXT;
    values[schema.fieldIndex(EmbeddingTableSchema.CHUNK_ID)] = 0L;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_PATH)] = "s3://bucket/" + sha + ".pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_NAME)] = sha + ".pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_EXTENSION)] = "pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)] = text;
    values[schema.fieldIndex(customer)] = "customer value";
    values[schema.fieldIndex(CUSTOMER_SUFFIX)] = "customer suffix value";
    values[schema.fieldIndex(CUSTOMER_SECOND_SUFFIX)] = "customer second suffix value";
    return RowFactory.create(values);
  }

  @ParameterizedTest
  @CsvSource({"prep_only,false", "prep_only,true", "prep_and_compute,false", "prep_and_compute,true"})
  public void legacyTableMigratesBeforePrepOrTextComputeAndContinuesIntoCompute(String mode, boolean errorTable) throws Exception {
    TypedProperties legacyProps = textOnlyProps(table(), EmbeddingTableConfig.MODE_PREP_ONLY);
    StructType legacy = new StructType(Arrays.stream(EmbeddingTableSchema.build(EmbeddingTableConfig.from(legacyProps)).fields())
        .filter(field -> !EmbeddingTableSchema.PREPARED_IMAGE.equals(field.name())).toArray(StructField[]::new))
        .add("legacy_note", DataTypes.StringType, true);
    writeLegacyTable(legacyProps, legacy);
    assertFalse(Arrays.asList(EmbeddingTableSchema.writerReadSchema(table(), storage()).get().fieldNames())
        .contains(EmbeddingTableSchema.PREPARED_IMAGE));
    boolean prepOnly = EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode);
    byte[] png = ImageFixtures.image("png", 20, 10);
    FixtureFile added = prepOnly ? object("b.png", png) : text("b", "beta");
    announce(added);

    TypedProperties upgradeProps = textOnlyProps(table(), mode, HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), String.valueOf(errorTable));
    if (prepOnly) {
      upgradeProps.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "png," + TEXT_EXTENSION);
    }
    Round upgraded = round(upgradeProps);

    assertTrue(upgraded.getErrors().isEmpty(), upgraded.toString());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), upgraded.getCommit().get()));
    StructField imageField = EmbeddingTableSchema.writerReadSchema(table(), storage()).get().apply(EmbeddingTableSchema.PREPARED_IMAGE);
    assertTrue(imageField.nullable());
    assertEquals(EmbeddingTableSchema.preparedImageField().dataType(), imageField.dataType());
    assertEquals("BLOB", imageField.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    Map<String, Row> stored = rows(table());
    assertEquals(2, stored.size());
    Row old = stored.values().stream().filter(row -> "legacy".equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA))).findFirst().get();
    assertEquals("legacy customer value", old.getAs("legacy_note"));
    assertNull(old.getAs(EmbeddingTableSchema.PREPARED_IMAGE));
    Row fresh = stored.get(key(added, prepOnly ? EmbeddingTableSchema.MODALITY_IMAGE : EmbeddingTableSchema.MODALITY_TEXT, 0));
    assertNotNull(fresh);
    if (prepOnly) {
      Row image = fresh.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      assertNotNull(image);
      assertEquals(HoodieSchema.Blob.INLINE, image.getString(0));
      assertArrayEquals(png, (byte[]) image.get(1));
    } else {
      assertNotNull(vector(fresh));
      assertEquals("beta", fresh.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    }

    FixtureFile next = text("c", "gamma");
    announce(next);
    Round computed = round(textOnlyProps(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), String.valueOf(errorTable)));
    assertTrue(computed.getErrors().isEmpty(), computed.toString());
    assertEquals(3, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), computed.getCommit().get()));
    assertNotNull(vector(rows(table()).get(key(next, EmbeddingTableSchema.MODALITY_TEXT, 0))));
    assertEquals(3, rows(table()).size());
    if (prepOnly) {
      Row retained = rows(table()).get(key(added, EmbeddingTableSchema.MODALITY_IMAGE, 0)).getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      assertArrayEquals(png, (byte[]) retained.get(1));
    }
  }

  /** Seed an actual old writer schema, independently of the current transformer's migration logic. */
  private void writeLegacyTable(TypedProperties props, StructType schema) {
    Object[] values = new Object[schema.size()];
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)] = "legacy";
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME)] = 1234L;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_SIZE)] = 99L;
    values[schema.fieldIndex(EmbeddingTableSchema.MODALITY)] = EmbeddingTableSchema.MODALITY_TEXT;
    values[schema.fieldIndex(EmbeddingTableSchema.CHUNK_ID)] = 0L;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_PATH)] = "s3://bucket/legacy.pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_NAME)] = "legacy.pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_EXTENSION)] = "pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)] = "legacy text";
    values[schema.fieldIndex("legacy_note")] = "legacy customer value";
    Map<String, String> options = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      options.put(key, props.getString(key));
    }
    options.put("hoodie.table.name", "embedding_table");
    options.put("hoodie.datasource.write.table.type", "MERGE_ON_READ");
    options.put("hoodie.datasource.write.operation", "upsert");
    options.put("hoodie.parquet.compression.codec", "uncompressed");
    spark().createDataFrame(Collections.singletonList(RowFactory.create(values)), schema).write().format("hudi").options(options)
        .mode(SaveMode.Append).save(table());
    // The fixture models an older writer schema within the declared table contract.
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setBasePath(table()).setConf(storage()).build();
    HoodieTableConfig.update(meta.getStorage(), meta.getMetaPath(), UnstructuredTableTestProperties.embedding(props));
  }

  /** The compute fixtures announce text and use a text-only model, including when their error table is disabled. */
  private TypedProperties textOnlyProps(String table, String mode, String... overrides) {
    TypedProperties props = props(table, mode, overrides);
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), TEXT_EXTENSION);
    return props;
  }
}

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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code prep_only} embedding table has no model (VECTOR) column; a round of the other mode on an existing table is not refused.
 */
@Tag("functional")
public class TestEmbeddingPrepOnlyNoModelColumn extends EmbeddingRoundTestBase {

  @Test
  public void newPrepOnlyTableHasNoModelColumn() throws Exception {
    announce(text("a", "alpha", "beta"), text("b", "gamma"));

    Round round = round(props(table(), EmbeddingTableConfig.MODE_PREP_ONLY));

    assertTrue(round.getCommit().isPresent(), round.toString());
    StructType schema = EmbeddingTableSchema.writerReadSchema(table(), storage()).get();
    for (StructField field : schema.fields()) {
      assertFalse(isVector(field), "no VECTOR column under prep_only: " + field);
    }
    List<String> names = Arrays.asList(schema.fieldNames());
    assertFalse(names.contains(EmbeddingStreamers.MODEL_COLUMN), names.toString());
    assertTrue(names.containsAll(EmbeddingTableSchema.BASE_COLUMNS), names.toString());
    // the fixed columns and nothing else (no user partition columns are configured)
    List<String> dataColumns = names.stream().filter(name -> !HoodieRecord.HOODIE_META_COLUMNS.contains(name))
        .collect(Collectors.toList());
    assertEquals(EmbeddingTableSchema.BASE_COLUMNS, dataColumns);
  }

  @Test
  public void prepOnlyTableReadsBackThroughSparkSql() throws Exception {
    FixtureFile a = text("a", "alpha", "beta");
    FixtureFile b = text("b", "gamma");
    announce(a, b);
    round(props(table(), EmbeddingTableConfig.MODE_PREP_ONLY));

    String view = "prep_only_no_model_column_" + System.nanoTime();
    spark().read().format("hudi").load(table()).createOrReplaceTempView(view);
    List<Row> rows = spark().sql("SELECT source_path_sha, chunk_id, prepared_text_chunk FROM " + view
        + " ORDER BY source_path_sha, chunk_id").collectAsList();

    assertEquals(3, rows.size(), rows.toString());
    Map<String, List<String>> chunks = new TreeMap<>();
    for (Row row : rows) {
      chunks.computeIfAbsent(row.getString(0), sha -> new ArrayList<>()).add(row.getString(2));
    }
    Map<String, List<String>> expected = new TreeMap<>();
    expected.put(a.sha, Arrays.asList("alpha", "beta"));
    expected.put(b.sha, Arrays.asList("gamma"));
    assertEquals(expected, chunks);
    List<String> columns = Arrays.asList(spark().sql("SELECT * FROM " + view).columns());
    assertFalse(columns.contains(EmbeddingStreamers.MODEL_COLUMN), columns.toString());
  }

  /**
   * A table's VECTOR columns no longer decide its mode: a prep_and_compute round on a table a prep_only round wrote commits, and
   * the rows written in the other mode keep a null vector. One mode per table is the job's rule, not a start-time refusal.
   */
  @Test
  public void modeSwitchOnAnExistingTableIsNotRefused() throws Exception {
    announce(text("a", "alpha", "beta"));
    assertTrue(round(props(table(), EmbeddingTableConfig.MODE_PREP_ONLY)).getCommit().isPresent());
    announce(text("c", "delta"));
    Round computed = round(props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE));
    assertTrue(computed.getCommit().isPresent(), computed.toString());
    List<Row> vectors = spark().read().format("hudi").load(table())
        .select(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, EmbeddingStreamers.MODEL_COLUMN).collectAsList();
    assertTrue(vectors.stream().anyMatch(row -> row.isNullAt(1)), "a prep_only row keeps a null vector: " + vectors);
    assertTrue(vectors.stream().anyMatch(row -> !row.isNullAt(1)), "the prep_and_compute row has its vector: " + vectors);

    // and back: a prep_only round on the computed table commits, its new row with a null vector
    announce(text("e", "epsilon"));
    assertTrue(round(props(table(), EmbeddingTableConfig.MODE_PREP_ONLY)).getCommit().isPresent());
    assertEquals(3, EmbeddingStreamers.commits(table(), storage()).size());
  }

  /**
   * End to end: the user's path partition column follows each prepared row into its partition, and the prep_only table carries
   * the fixed columns plus that column, with no model (VECTOR) column.
   */
  @Test
  public void prepOnlyRoundWritesPathPartitionColumnsAndNoModelColumn() throws Exception {
    FixtureFile blue = text("team=blue/a", "alpha", "beta");
    FixtureFile red = text("team=red/b", "gamma");
    announce(blue, red);
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_ONLY,
        "hoodie.streamer.source.cloud.data.partition.fields.from.path", "team",
        "hoodie.datasource.write.partitionpath.field", "team",
        "hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");

    Round round = round(props);

    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertTrue(round.getCommit().isPresent(), round.toString());
    assertEquals(3, round.getRowsWritten(), round.toString());
    StructType schema = EmbeddingTableSchema.writerReadSchema(table(), storage()).get();
    for (StructField field : schema.fields()) {
      assertFalse(isVector(field), "no VECTOR column under prep_only: " + field);
    }
    List<String> dataColumns = Arrays.stream(schema.fieldNames())
        .filter(name -> !HoodieRecord.HOODIE_META_COLUMNS.contains(name)).collect(Collectors.toList());
    List<String> expectedColumns = new ArrayList<>(EmbeddingTableSchema.BASE_COLUMNS);
    expectedColumns.add("team");
    assertEquals(expectedColumns, dataColumns);

    List<Row> stored = spark().read().format("hudi").load(table())
        .select(EmbeddingTableSchema.SOURCE_PATH_SHA, EmbeddingTableSchema.PREPARED_TEXT_CHUNK, "team",
            HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD).collectAsList();
    assertEquals(3, stored.size(), stored.toString());
    Set<List<String>> expected = new HashSet<>(Arrays.asList(
        Arrays.asList(blue.sha, "alpha", "blue", "blue"),
        Arrays.asList(blue.sha, "beta", "blue", "blue"),
        Arrays.asList(red.sha, "gamma", "red", "red")));
    Set<List<String>> actual = new HashSet<>();
    for (Row row : stored) {
      actual.add(Arrays.asList(row.getString(0), row.getString(1), row.getString(2), row.getString(3)));
      assertTrue(Files.isRegularFile(localPath(table()).resolve(row.getString(3)).resolve(row.getString(4))),
          "the stored partition path names the actual data file: " + row);
    }
    assertEquals(expected, actual);
    assertEquals("team", EmbeddingStreamers.metaClient(table(), storage()).getTableConfig().getPartitionFieldProp());
  }

  private static boolean isVector(StructField field) {
    return field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD)
        && field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD).startsWith("VECTOR(");
  }
}

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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;
import org.apache.hudi.utilities.config.ORCDFSSourceConfig;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates that {@link ORCDFSSource} hands the files picked by its path selector to the ORC
 * reader, honours the merge-schema option of {@link ORCDFSSourceConfig} and reports an empty batch
 * once the input path is drained.
 */
class TestORCDFSSource {

  private static final int NUM_RECORDS = 2;

  private static JavaSparkContext jsc;
  private static SparkSession spark;

  @TempDir
  static java.nio.file.Path tempDir;

  @BeforeAll
  static void initSpark() {
    spark = SparkSession.builder()
        .master("local[1]")
        .appName("TestORCDFSSource")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFetchNextBatch(boolean mergeSchema) throws IOException {
    String sourceRoot = tempDir.resolve("orc_merge_schema_" + mergeSchema).toString();
    List<GenericRecord> written = writeOrcFile(sourceRoot);
    TypedProperties props = new TypedProperties();
    props.setProperty(DFSPathSelectorConfig.ROOT_INPUT_PATH.key(), sourceRoot);
    props.setProperty(ORCDFSSourceConfig.ORC_DFS_MERGE_SCHEMA.key(), String.valueOf(mergeSchema));
    ORCDFSSource source = new ORCDFSSource(props, jsc, spark, null);

    Pair<Option<Dataset<Row>>, Checkpoint> batch = source.fetchNextBatch(Option.empty(), Long.MAX_VALUE);
    assertNotNull(batch.getRight());
    Dataset<Row> rows = batch.getLeft().get();
    assertTrue(Arrays.asList(rows.columns()).contains("_row_key"));
    assertEquals(recordKeysOf(written), rows.select("_row_key").collectAsList().stream()
        .map(row -> row.getString(0)).sorted().collect(Collectors.toList()));

    // the checkpoint holds the modification time of the files already read, so nothing is selected
    // on the next round and the source reports an empty batch
    Pair<Option<Dataset<Row>>, Checkpoint> nextBatch =
        source.fetchNextBatch(Option.of(batch.getRight()), Long.MAX_VALUE);
    assertTrue(nextBatch.getLeft().isEmpty());
    assertNotNull(nextBatch.getRight());
  }

  @Test
  void testMergeSchemaConfig() {
    assertEquals("hoodie.streamer.source.orc.dfs.merge.schema.enable",
        ORCDFSSourceConfig.ORC_DFS_MERGE_SCHEMA.key());
    assertTrue(ORCDFSSourceConfig.ORC_DFS_MERGE_SCHEMA.defaultValue());
    assertEquals(Collections.emptyList(), ORCDFSSourceConfig.ORC_DFS_MERGE_SCHEMA.getAlternatives());
  }

  /**
   * Writes the source file with the ORC writer rather than with spark: this module pulls in
   * {@code orc-core-nohive}, which makes spark's own ORC writer fail on the test class-path.
   */
  private static List<GenericRecord> writeOrcFile(String sourceRoot) throws IOException {
    List<GenericRecord> records = UtilitiesTestBase.Helpers.toGenericRecords(
        new HoodieTestDataGenerator().generateInserts("000", NUM_RECORDS));
    UtilitiesTestBase.Helpers.saveORCToDFS(records, new Path(sourceRoot + "/1.orc"));
    return records;
  }

  private static List<String> recordKeysOf(List<GenericRecord> records) {
    return records.stream().map(record -> record.get("_row_key").toString()).sorted().collect(Collectors.toList());
  }
}

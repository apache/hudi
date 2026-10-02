/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieFileIndex;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.collection.JavaConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A snapshot read merges base and log records with the merge mode and ordering fields persisted in
 * the table config. Read options that happen to share a table config key must not change that:
 * the persisted config is authoritative, and a read must not return different rows because an
 * option was passed, or depending on which files a task happened to read first.
 *
 * <p>The table uses {@link RecordMergeMode#EVENT_TIME_ORDERING} on {@code ts}. Every key is first
 * written with {@code ts = 10} and value {@code A}, then updated with a lower {@code ts = 5} and
 * value {@code B}, which event time ordering must discard, so every read must return {@code A}.
 * All file groups are packed into a single task, so that later file groups of the task are read
 * after earlier ones have been set up. The table config of the relation must also be left as
 * persisted after the read.
 */
@Tag("functional")
class TestReadOptionsDoNotOverrideTableConfig extends SparkClientFunctionalTestHarness {

  private static final int NUM_PARTITIONS = 4;
  private static final int KEYS_PER_PARTITION = 5;

  private static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("part", DataTypes.StringType, false),
      DataTypes.createStructField("ts", DataTypes.LongType, false),
      DataTypes.createStructField("other_ts", DataTypes.LongType, false),
      DataTypes.createStructField("value", DataTypes.StringType, true)});

  private static final String[] PACKING_CONFIGS = {
      "spark.sql.files.minPartitionNum", "spark.sql.files.openCostInBytes"};

  private final Map<String, String> savedSessionConfs = new HashMap<>();

  @BeforeEach
  void packFilesIntoOneTask() {
    for (String conf : PACKING_CONFIGS) {
      if (spark().conf().contains(conf)) {
        savedSessionConfs.put(conf, spark().conf().get(conf));
      }
    }
    spark().conf().set("spark.sql.files.minPartitionNum", "1");
    spark().conf().set("spark.sql.files.openCostInBytes", "1");
  }

  @AfterEach
  void restoreSessionConfs() {
    for (String conf : PACKING_CONFIGS) {
      if (savedSessionConfs.containsKey(conf)) {
        spark().conf().set(conf, savedSessionConfs.get(conf));
      } else {
        spark().conf().unset(conf);
      }
    }
  }

  @Test
  void testMergeModeReadOptionDoesNotOverrideTableConfig() {
    String basePath = writeTable();
    assertAllRowsKeepFirstValue(read(basePath, Collections.emptyMap()), "a read without options");
    Dataset<Row> df = read(basePath, Collections.singletonMap(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.COMMIT_TIME_ORDERING.name()));
    assertAllRowsKeepFirstValue(df, "a read with " + HoodieTableConfig.RECORD_MERGE_MODE.key() + "=" + RecordMergeMode.COMMIT_TIME_ORDERING);
    assertRelationTableConfigUnchanged(df);
  }

  @Test
  void testOrderingFieldsReadOptionDoesNotOverrideTableConfig() {
    String basePath = writeTable();
    // other_ts orders the update after the insert; the persisted ordering field ts does not.
    Dataset<Row> df = read(basePath, Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "other_ts"));
    assertAllRowsKeepFirstValue(df, "a read with " + HoodieTableConfig.ORDERING_FIELDS.key() + "=other_ts");
    assertRelationTableConfigUnchanged(df);
  }

  private String writeTable() {
    String basePath = basePath();
    Map<String, String> options = new HashMap<>();
    options.put(HoodieTableConfig.NAME.key(), "merge_config_from_table");
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), HoodieTableType.MERGE_ON_READ.name());
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "part");
    options.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "ts");
    options.put(HoodieWriteConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.EVENT_TIME_ORDERING.name());
    options.put("hoodie.insert.shuffle.parallelism", "2");
    options.put("hoodie.upsert.shuffle.parallelism", "2");
    write(rows(10L, 1L, "A"), options, basePath);
    write(rows(5L, 2L, "B"), options, basePath);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf()).build();
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig().getRecordMergeMode());
    assertEquals("ts", metaClient.getTableConfig().getOrderingFieldsStr().orElse(null));
    assertTrue(countLogFiles(tempDir) >= NUM_PARTITIONS, "Every file group should have a log file to merge");
    return basePath;
  }

  private Dataset<Row> read(String basePath, Map<String, String> options) {
    Dataset<Row> df = spark().read().format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
        .options(options)
        .load(basePath);
    assertEquals(1, df.rdd().getNumPartitions(), "All file groups should be read by a single task");
    return df;
  }

  private static void assertAllRowsKeepFirstValue(Dataset<Row> df, String description) {
    List<Row> rows = df.select("key", "value", "ts").collectAsList();
    assertEquals(NUM_PARTITIONS * KEYS_PER_PARTITION, rows.size(), description);
    List<String> overridden = rows.stream()
        .filter(row -> !"A".equals(row.getString(1)))
        .map(row -> row.getString(0) + "=" + row.getString(1) + "@ts" + row.getLong(2))
        .sorted()
        .collect(Collectors.toList());
    assertTrue(overridden.isEmpty(), "With event time ordering on ts persisted in the table config, "
        + description + " must keep the ts=10 value A for every key, but these keys returned the lower-ts update: "
        + overridden);
  }

  /**
   * The relation's meta client is the one the reader state is built from on the driver; the read options must not
   * have been written into its table config.
   */
  private static void assertRelationTableConfigUnchanged(Dataset<Row> df) {
    LogicalPlan relationPlan = JavaConverters.seqAsJavaList(df.queryExecution().optimizedPlan().collectLeaves()).stream()
        .filter(plan -> plan instanceof LogicalRelation)
        .findFirst()
        .orElseThrow(() -> new AssertionError("No relation in the plan of the read"));
    HadoopFsRelation relation = (HadoopFsRelation) ((LogicalRelation) relationPlan).relation();
    HoodieTableConfig tableConfig = ((HoodieFileIndex) relation.location()).metaClient().getTableConfig();
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, tableConfig.getRecordMergeMode());
    assertEquals("ts", tableConfig.getOrderingFieldsStr().orElse(null));
  }

  private static List<Row> rows(long ts, long otherTs, String value) {
    return IntStream.range(0, NUM_PARTITIONS * KEYS_PER_PARTITION)
        .mapToObj(i -> RowFactory.create(String.format("key%02d", i), "p" + (i % NUM_PARTITIONS), ts, otherTs, value))
        .collect(Collectors.toList());
  }

  private void write(List<Row> rows, Map<String, String> options, String basePath) {
    spark().createDataset(rows, SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(SCHEMA))
        .write()
        .format("hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(basePath);
  }

  private static long countLogFiles(Path tableDir) {
    try (Stream<Path> files = Files.walk(tableDir)) {
      return files.filter(p -> !p.toString().contains("/.hoodie/") && p.getFileName().toString().contains(".log.")).count();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

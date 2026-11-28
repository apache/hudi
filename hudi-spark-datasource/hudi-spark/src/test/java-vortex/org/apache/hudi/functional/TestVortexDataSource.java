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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.DefaultSparkRecordMerger;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import dev.vortex.jni.NativeRuntime;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for Hudi tables with the Vortex base file format, through the Spark datasource.
 */
@DisabledIfSystemProperty(named = "vortex.skip.tests", matches = "true")
public class TestVortexDataSource extends HoodieSparkClientTestBase {

  private static final String MAX_PARTITION_BYTES = "spark.sql.files.maxPartitionBytes";
  private static final String OPEN_COST_IN_BYTES = "spark.sql.files.openCostInBytes";

  @AfterEach
  public void resetSqlConf() {
    sparkSession.conf().unset(MAX_PARTITION_BYTES);
    sparkSession.conf().unset(OPEN_COST_IN_BYTES);
  }

  /**
   * Spark splits large base files into many partitions. Each row must come back exactly once.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testReadOfSplitFiles(HoodieTableType tableType) {
    int numRows = 20000;
    String tablePath = basePath + "/split_" + tableType.name().toLowerCase();
    write(tableType, tablePath, createRows(numRows, 0), SaveMode.Overwrite);

    sparkSession.conf().set(MAX_PARTITION_BYTES, "16384");
    sparkSession.conf().set(OPEN_COST_IN_BYTES, "1");

    Dataset<Row> read = sparkSession.read().format("hudi").load(tablePath);
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      assertTrue(read.rdd().getNumPartitions() > 1, "The test must make Spark split the base file");
    }

    assertEquals(numRows, read.count());
    assertEquals(numRows, read.select("id").distinct().count());
    assertEquals(numRows, read.select("id", "name", "score").collectAsList().size());
    assertEquals(sumOfIds(numRows), read.selectExpr("sum(id)").first().getLong(0));
  }

  /**
   * Vortex evaluates the filters that it can. The result must be the same as without pushdown,
   * also after updates that are in log files.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testFilterPushdown(HoodieTableType tableType) {
    int numRows = 5000;
    String tablePath = basePath + "/filter_" + tableType.name().toLowerCase();
    write(tableType, tablePath, createRows(numRows, 0), SaveMode.Overwrite);

    // Update the score of the first 500 rows. For MOR, the updates are in log files.
    Dataset<Row> updates = createRows(500, 0).withColumn("score", functions.lit(2.0)).withColumn("ts", functions.lit(2L));
    write(tableType, tablePath, updates, SaveMode.Append);

    Dataset<Row> table = sparkSession.read().format("hudi").load(tablePath);
    assertEquals(numRows, table.count());

    assertEquals(500, table.filter("score > 1.5").count());
    assertEquals(500, table.filter("id < 500 AND score = 2.0").count());
    assertEquals(4500, table.filter("id >= 500").count());
    assertEquals(3, table.filter("id IN (1, 2, 4999, 6000)").count());
    assertEquals(1667, table.filter("part = 'p0'").count());
    assertEquals(numRows, table.filter("name LIKE '%ame-%'").count());
    assertEquals(numRows, table.filter("name LIKE 'name-%'").count());

    long expectedPrefixMatches = createRows(numRows, 0).filter("name LIKE 'name-a%'").count();
    assertTrue(expectedPrefixMatches > 0);
    assertEquals(expectedPrefixMatches, table.filter("name LIKE 'name-a%'").count());
    assertEquals(0, table.filter("name IS NULL").count());
    assertEquals(sumOfIds(numRows) - sumOfIds(1000), table.filter("id >= 1000").selectExpr("sum(id)").first().getLong(0));

    // A query that reads only some columns.
    List<Row> projected = table.filter("id = 42").select("name", "id").collectAsList();
    assertEquals(1, projected.size());
    assertEquals(42L, projected.get(0).getLong(1));
  }

  /**
   * The read concurrency settings are read options. A value that is not a number makes the read
   * fail, which shows that the value gets to the Vortex reader.
   */
  @Test
  public void testReadConcurrencyOptions() {
    int numRows = 5000;
    String tablePath = basePath + "/concurrency";
    write(HoodieTableType.COPY_ON_WRITE, tablePath, createRows(numRows, 0), SaveMode.Overwrite);

    Dataset<Row> table = sparkSession.read().format("hudi")
        .option(HoodieStorageConfig.VORTEX_READ_SPLIT_CONCURRENCY.key(), "2")
        .option(HoodieStorageConfig.VORTEX_READ_IO_CONCURRENCY.key(), "2")
        .load(tablePath);
    assertEquals(numRows, table.count());
    assertEquals(sumOfIds(numRows), table.selectExpr("sum(id)").first().getLong(0));

    assertInvalidOptionFails(tablePath, HoodieStorageConfig.VORTEX_READ_SPLIT_CONCURRENCY.key());
    assertInvalidOptionFails(tablePath, HoodieStorageConfig.VORTEX_READ_IO_CONCURRENCY.key());
  }

  /**
   * The worker thread setting gets to the Vortex writers of both write paths and to the Vortex
   * reader. The workers of the JVM only increase, thus each step asks for more workers than the
   * step before it.
   */
  @Test
  public void testWorkerThreadsOption() {
    int numRows = 1000;
    String tablePath = basePath + "/workers";
    String key = HoodieStorageConfig.VORTEX_WORKER_THREADS.key();

    write(HoodieTableType.COPY_ON_WRITE, tablePath, createRows(numRows, 0), SaveMode.Overwrite, key, "13");
    assertTrue(NativeRuntime.workerCount() >= 13, "the upsert write path must add the workers");

    write(HoodieTableType.COPY_ON_WRITE, tablePath, createRows(numRows, 0), SaveMode.Overwrite, key, "14",
        DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    assertTrue(NativeRuntime.workerCount() >= 14, "the bulk insert write path must add the workers");

    Dataset<Row> table = sparkSession.read().format("hudi").option(key, "15").load(tablePath);
    assertEquals(sumOfIds(numRows), table.selectExpr("sum(id)").first().getLong(0));
    assertTrue(NativeRuntime.workerCount() >= 15, "the read path must add the workers");

    assertInvalidOptionFails(tablePath, key);
  }

  private void assertInvalidOptionFails(String tablePath, String key) {
    Dataset<Row> invalid = sparkSession.read().format("hudi").option(key, "not-a-number").load(tablePath);
    Exception e = assertThrows(Exception.class, () -> invalid.selectExpr("sum(id)").collectAsList(), key);
    assertTrue(hasCause(e, NumberFormatException.class), "The Vortex reader must read " + key);
  }

  private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  /**
   * With positional merging, the reader gives the position of each base file row from the Vortex
   * row index.
   */
  @Test
  public void testPositionalMerging() {
    int numRows = 3000;
    String tablePath = basePath + "/positional";
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    write(tableType, tablePath, createRows(numRows, 0), SaveMode.Overwrite, "hoodie.write.record.positions", "true");

    Dataset<Row> updates = createRows(numRows, 0)
        .filter("id % 3 = 0")
        .withColumn("score", functions.lit(-1.0))
        .withColumn("ts", functions.lit(2L));
    write(tableType, tablePath, updates, SaveMode.Append, "hoodie.write.record.positions", "true");

    Dataset<Row> table = sparkSession.read().format("hudi")
        .option("hoodie.merge.use.record.positions", "true")
        .load(tablePath);
    assertEquals(numRows, table.count());
    assertEquals(numRows / 3, table.filter("score = -1.0").count());
    assertEquals(0, table.filter("score = -1.0 AND id % 3 != 0").count());
  }

  /**
   * A COW scan returns columnar batches. A file with an older, narrower type of a column also
   * returns batches: the Arrow views give the narrow values as the wider table type.
   */
  @Test
  public void testColumnarBatchReads() {
    String tablePath = basePath + "/batches";
    StructType intSchema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("value", DataTypes.IntegerType, false)
        .add("part", DataTypes.StringType, false)
        .add("ts", DataTypes.LongType, false);
    StructType longSchema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("value", DataTypes.LongType, false)
        .add("part", DataTypes.StringType, false)
        .add("ts", DataTypes.LongType, false);

    // The first commit writes "value" as int to partition p0. The second commit widens it to long
    // and writes partition p1, thus the file of p0 keeps the int type.
    List<Row> first = new ArrayList<>();
    List<Row> second = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      first.add(RowFactory.create((long) i, i, "p0", 1L));
      second.add(RowFactory.create(1000L + i, 5_000_000_000L + i, "p1", 1L));
    }
    write(HoodieTableType.COPY_ON_WRITE, tablePath, sparkSession.createDataFrame(first, intSchema), SaveMode.Overwrite);
    write(HoodieTableType.COPY_ON_WRITE, tablePath, sparkSession.createDataFrame(second, longSchema), SaveMode.Append);

    Dataset<Row> table = sparkSession.read().format("hudi").load(tablePath);
    String plan = table.select("id", "value", "part").queryExecution().executedPlan().toString();
    assertTrue(plan.contains("ColumnarToRow"), "the scan must return batches: " + plan);

    assertEquals(2000, table.count());
    assertEquals(2000, table.select("part").collectAsList().size());
    assertEquals(1000, table.filter("part = 'p1'").count());
    assertEquals(sumOfIds(1000), table.filter("part = 'p0'").selectExpr("sum(value)").first().getLong(0));
    assertEquals(1000 * 5_000_000_000L + sumOfIds(1000),
        table.filter("part = 'p1'").selectExpr("sum(value)").first().getLong(0));
    assertEquals(sumOfIds(2000), table.selectExpr("sum(id)").first().getLong(0));

    List<Row> rows = table.filter("id = 7 OR id = 1007").select("id", "value", "part").orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals(RowFactory.create(7L, 7L, "p0"), rows.get(0));
    assertEquals(RowFactory.create(1007L, 5_000_000_007L, "p1"), rows.get(1));
  }

  /**
   * Spark prunes the fields of a struct that a query does not read. The reader must give the
   * requested fields, not the first fields of the struct in the file.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testNestedFieldPruning(HoodieTableType tableType) {
    String tablePath = basePath + "/nested_" + tableType.name().toLowerCase();
    Dataset<Row> df = sparkSession.range(0, 3000).selectExpr(
        "id",
        "case when id % 100 = 5 then null"
            + " else named_struct('a', id, 'b', concat('b', cast(id as string)), 'c', named_struct('x', id * 2, 'y', id * 3))"
            + " end as nested",
        "array(named_struct('p', id, 'q', concat('q', cast(id as string))), named_struct('p', -id, 'q', 'last')) as items",
        "map('k', named_struct('u', id, 'v', concat('v', cast(id as string)))) as attrs",
        "concat('p', cast(id % 2 as string)) as part",
        "1L as ts");
    write(tableType, tablePath, df, SaveMode.Overwrite);
    write(tableType, tablePath, df.filter("id < 100").withColumn("ts", functions.lit(2L)), SaveMode.Append);

    Dataset<Row> table = sparkSession.read().format("hudi").load(tablePath);
    List<Row> rows = table.filter("id = 42").select("nested.b", "nested.c.y").collectAsList();
    assertEquals(1, rows.size());
    assertEquals("b42", rows.get(0).getString(0));
    assertEquals(126L, rows.get(0).getLong(1));

    // The struct is null in 30 rows: the ids 5, 105, ..., 2905.
    long sumOfNullIds = 30L * 5 + 100L * sumOfIds(30);
    assertEquals(1, table.filter("nested.b = 'b2042'").count());
    assertEquals(2970, table.filter("nested.b LIKE 'b%'").count());
    assertEquals(30, table.filter("nested IS NULL").count());
    assertEquals(30, table.select("nested.c").filter("c IS NULL").count());
    assertEquals(3L * (sumOfIds(3000) - sumOfNullIds), table.selectExpr("sum(nested.c.y)").first().getLong(0));
    assertEquals(990, table.filter("nested.c.x >= 4000").select("nested.b").count());

    List<Row> collections = table.filter("id = 7")
        .selectExpr("items.q", "attrs['k'].v", "element_at(items, 2).p").collectAsList();
    assertEquals(1, collections.size());
    assertEquals(java.util.Arrays.asList("q7", "last"), collections.get(0).getList(0));
    assertEquals("v7", collections.get(0).getString(1));
    assertEquals(-7L, collections.get(0).getLong(2));
    assertEquals(1, table.filter("attrs['k'].v = 'v2999'").count());
  }

  /**
   * Bulk insert writes the base files with the row writer. An upsert then finds the existing
   * records through the bloom filter and the record key range in the Vortex footer.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testBulkInsertThenUpsert(HoodieTableType tableType) {
    int numRows = 2000;
    String tablePath = basePath + "/bulk_" + tableType.name().toLowerCase();
    write(tableType, tablePath, createRows(numRows, 0), SaveMode.Overwrite,
        DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Dataset<Row> updates = createRows(200, 0).withColumn("score", functions.lit(-1.0)).withColumn("ts", functions.lit(2L));
    write(tableType, tablePath, updates, SaveMode.Append);

    Dataset<Row> table = sparkSession.read().format("hudi").load(tablePath);
    assertEquals(numRows, table.count());
    assertEquals(numRows, table.select("id").distinct().count());
    assertEquals(200, table.filter("score = -1.0").count());
    assertEquals(0, table.filter("score = -1.0 AND id >= 200").count());
  }

  /**
   * The column stats index has no Vortex statistics for some columns: nested columns, and
   * floating-point columns with NaN values. Data skipping must keep each file that can have
   * matching rows, also when only some files of a partition have statistics for a column.
   */
  @Test
  public void testDataSkippingWithColumnsWithoutStatistics() {
    String tablePath = basePath + "/skipping";
    StructType nestedType = new StructType().add("a", DataTypes.LongType, false);
    StructType schema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("score", DataTypes.DoubleType, false)
        .add("nested", nestedType, false)
        .add("part", DataTypes.StringType, false)
        .add("ts", DataTypes.LongType, false);

    // Two bulk inserts make two files in the same partition. Only the second file has a NaN score.
    for (int batch = 0; batch < 2; batch++) {
      List<Row> rows = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        long id = batch * 1000L + i;
        double score = batch == 0 ? i / 1000.0 : (i == 0 ? Double.NaN : 10.0 + i);
        rows.add(RowFactory.create(id, score, RowFactory.create(id), "p0", 1L));
      }
      write(HoodieTableType.COPY_ON_WRITE, tablePath, sparkSession.createDataFrame(rows, schema),
          batch == 0 ? SaveMode.Overwrite : SaveMode.Append,
          DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    }

    for (String skipping : new String[] {"true", "false"}) {
      Dataset<Row> table = sparkSession.read().format("hudi")
          .option("hoodie.enable.data.skipping", skipping)
          .option("hoodie.metadata.enable", "true")
          .load(tablePath);
      String message = "data skipping " + skipping;

      // In Spark, NaN is larger than all other values.
      assertEquals(1000, table.filter("score > 5.0").count(), message);
      assertEquals(1, table.filter("isnan(score)").count(), message);
      assertEquals(500, table.filter("score < 0.5").count(), message);
      assertEquals(500, table.filter("nested.a >= 1500").count(), message);
      assertEquals(1, table.filter("nested.a = 1999").count(), message);
      assertEquals(1000, table.filter("id >= 1000").count(), message);
    }
  }

  /**
   * A large file has many Arrow batches in one scan partition. Each batch must be read from its
   * own buffers. The results must be the same as for a Parquet table.
   */
  @Test
  public void testLargeFilesMatchParquet() {
    Dataset<Row> df = sparkSession.range(0, 1_000_000).selectExpr(
        "id",
        "concat('name-', cast(id * 7919 % 100003 as string)) as name",
        "rand(1) as score",
        "concat('p', cast(id % 3 as string)) as part",
        "1L as ts",
        "id % 1000 as bucket");

    String[] queries = new String[] {
        "select sum(id), round(sum(score), 6), max(name), min(name), count(distinct bucket) from %s",
        "select round(sum(score), 6), count(*) from %s where bucket = 7",
        "select part, count(*), max(name) from %s where name like 'name-9%%' group by part order by part"};

    List<List<Row>> results = new ArrayList<>();
    for (String format : new String[] {"VORTEX", "PARQUET"}) {
      String tablePath = basePath + "/large_" + format.toLowerCase();
      df.write().format("hudi")
          .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), format)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "part")
          .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL())
          .option(HoodieWriteConfig.TBL_NAME.key(), "large_" + format.toLowerCase())
          .option("hoodie.bulkinsert.shuffle.parallelism", "2")
          .mode(SaveMode.Overwrite)
          .save(tablePath);

      String view = "large_" + format.toLowerCase();
      sparkSession.read().format("hudi").load(tablePath).createOrReplaceTempView(view);
      List<Row> rows = new ArrayList<>();
      for (String query : queries) {
        rows.addAll(sparkSession.sql(String.format(query, view)).collectAsList());
      }
      results.add(rows);
    }

    assertEquals(results.get(1), results.get(0));
  }

  /**
   * The column stats index entries of a Vortex file are the same as those of a Parquet file with
   * the same rows, for all value types, and for columns with some or all values null. Data
   * skipping with these entries gives the same results as a scan without data skipping.
   */
  @Test
  public void testColumnStatsMatchParquet() {
    Dataset<Row> df = statsRows();
    List<List<Row>> entries = new ArrayList<>();
    for (String format : new String[] {"PARQUET", "VORTEX"}) {
      String tablePath = basePath + "/stats_" + format.toLowerCase();
      writeSingleFile(format, tablePath, df);

      // The commit time is different for each table.
      entries.add(sparkSession.read().format("hudi").load(tablePath + "/.hoodie/metadata")
          .filter("type = 3 AND ColumnStatsMetadata.columnName != '_hoodie_commit_time'")
          .selectExpr("ColumnStatsMetadata.columnName", "ColumnStatsMetadata.minValue", "ColumnStatsMetadata.maxValue",
              "ColumnStatsMetadata.nullCount", "ColumnStatsMetadata.valueCount")
          .orderBy("columnName")
          .collectAsList());
    }
    assertEquals(entries.get(0), entries.get(1));
    assertEquals(15, entries.get(1).size());

    String tablePath = basePath + "/stats_vortex";
    String[] filters = new String[] {
        "s IS NULL", "s IS NOT NULL", "allnull IS NULL", "allnull IS NOT NULL", "dbl IS NULL", "dbl > 500",
        "i < 10", "b = true", "d >= date'2026-01-01'", "t > timestamp_micros(1700000500000000)",
        "tntz < cast(timestamp_micros(1700000010000000) as timestamp_ntz)", "dec > 9.5", "bin = cast('b5' as binary)", "s LIKE 's%'", "s LIKE 's99%'",
        "s = 's2000'", "id > 5000"};
    for (String filter : filters) {
      long withSkipping = readStatsTable(tablePath, "true").filter(filter).count();
      long withoutSkipping = readStatsTable(tablePath, "false").filter(filter).count();
      assertEquals(withoutSkipping, withSkipping, filter);
    }
    assertEquals(100, readStatsTable(tablePath, "true").filter("s IS NULL").count());
    assertEquals(900, readStatsTable(tablePath, "true").filter("s LIKE 's%'").count());
    assertEquals(1, readStatsTable(tablePath, "true").filter("bin = cast('b5' as binary)").count());
    assertEquals(1000, readStatsTable(tablePath, "true").filter("allnull IS NULL").count());
  }

  private Dataset<Row> readStatsTable(String tablePath, String dataSkipping) {
    return sparkSession.read().format("hudi")
        .option("hoodie.enable.data.skipping", dataSkipping)
        .option("hoodie.metadata.enable", "true")
        .load(tablePath);
  }

  private Dataset<Row> statsRows() {
    return sparkSession.range(0, 1000).selectExpr(
        "id",
        "cast(id as int) as i",
        "id % 2 = 0 as b",
        "date_add(date'2024-01-01', cast(id as int)) as d",
        "timestamp_micros(1700000000000000 + id * 1000000) as t",
        "cast(timestamp_micros(1700000000000000 + id * 1000000) as timestamp_ntz) as tntz",
        "cast(id / 100.0 as decimal(10, 2)) as dec",
        "cast(concat('b', cast(id as string)) as binary) as bin",
        "case when id % 10 = 0 then null else concat('s', cast(id as string)) end as s",
        "cast(null as string) as allnull",
        "case when id % 7 = 0 then null else cast(id as double) end as dbl",
        "'p0' as part",
        "1L as ts");
  }

  private void writeSingleFile(String format, String tablePath, Dataset<Row> df) {
    df.coalesce(1).write().format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), format)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "ts")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "part")
        .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL())
        .option(HoodieWriteConfig.TBL_NAME.key(), "stats_" + format.toLowerCase())
        .option("hoodie.bulkinsert.shuffle.parallelism", "1")
        .mode(SaveMode.Overwrite)
        .save(tablePath);
  }

  private static long sumOfIds(int numRows) {
    return (long) numRows * (numRows - 1) / 2;
  }

  private Dataset<Row> createRows(int numRows, int seed) {
    Random random = new Random(seed);
    List<Row> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      String name = "name-" + Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
      rows.add(RowFactory.create((long) i, name, random.nextDouble(), "p" + (i % 3), 1L));
    }

    StructType schema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("name", DataTypes.StringType, false)
        .add("score", DataTypes.DoubleType, false)
        .add("part", DataTypes.StringType, false)
        .add("ts", DataTypes.LongType, false);
    return sparkSession.createDataFrame(rows, schema);
  }

  private void write(HoodieTableType tableType, String tablePath, Dataset<Row> df, SaveMode saveMode,
                     String... extraOptions) {
    DataFrameWriter<Row> writer = df.write();
    for (int i = 0; i + 1 < extraOptions.length; i += 2) {
      writer = writer.option(extraOptions[i], extraOptions[i + 1]);
    }

    writer
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "VORTEX")
        .option(DataSourceWriteOptions.TABLE_TYPE().key(), tableType.name())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "ts")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "part")
        .option(HoodieWriteConfig.TBL_NAME.key(), "vortex_table")
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), DefaultSparkRecordMerger.class.getName())
        .option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.bulkinsert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .mode(saveMode)
        .save(tablePath);
  }
}

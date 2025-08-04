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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHiveTableSchemaEvolution {

  private SparkSession spark = null;

  @TempDir
  java.nio.file.Path basePath;

  @BeforeEach
  public void setUp() {
    initSparkContexts("HiveSchemaEvolution");
  }

  @AfterEach
  public void clean() {
    if (spark != null) {
      spark.close();
    }
  }

  private void initSparkContexts(String appName) {
    SparkConf sparkConf = getSparkConfForTest(appName);

    spark = SparkSession.builder()
        .config("hoodie.support.write.lock", "false")
        .config("spark.sql.session.timeZone", "CTT")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .config(sparkConf)
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");
  }

  @Test
  public void testHiveReadTimestampColumnAsTimestampWritable() throws Exception {
    String tableName = "hudi_test" + new Date().getTime();
    String path = new Path(basePath.toAbsolutePath().toString()).toUri().toString();

    spark.sql("set hoodie.schema.on.read.enable=true");
    spark.sql("set hoodie.datasource.write.schema.allow.auto.evolution.column.drop=true");

    spark.sql(String.format("create table %s (col0 int, col1 float, col2 string, col3 timestamp) using hudi "
            + "tblproperties (type='mor', primaryKey='col0', preCombineField='col1', "
            + "hoodie.compaction.payload.class='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload') location '%s'",
        tableName, path));
    spark.sql(String.format("insert into %s values(1, 1.1, 'text', timestamp('2021-12-25 12:01:01'))", tableName));
    spark.sql(String.format("update %s set col2 = 'text2' where col0 = 1", tableName));
    spark.sql(String.format("alter table %s rename column col2 to col2_new", tableName));

    JobConf jobConf = new JobConf();
    jobConf.set(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
    jobConf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "col1,col2_new,col3");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "6,7,8");
    jobConf.set(serdeConstants.LIST_COLUMNS, "_hoodie_commit_time,_hoodie_commit_seqno,"
        + "_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,col0,col1,col2_new,col3");
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string,string,int,float,string,timestamp,string");
    FileInputFormat.setInputPaths(jobConf, path);

    HoodieParquetInputFormat inputFormat = new HoodieParquetRealtimeInputFormat();
    inputFormat.setConf(jobConf);

    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    int expectedSplits = 1;
    assertEquals(expectedSplits, splits.length);

    RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat.getRecordReader(splits[0], jobConf, null);
    List<List<Writable>> records = getWritableList(recordReader);
    assertEquals(1, records.size());
    List<Writable> record1 = records.get(0);
    // _hoodie_record_key,_hoodie_commit_time,_hoodie_partition_path, col1, col2, col3
    assertEquals(6, record1.size());

    Writable c3 = record1.get(5);
    assertTrue(c3 instanceof TimestampWritable);

    recordReader.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  public void testHiveReadSchemaEvolutionTable(String tableType) throws Exception {
    String tableName = "hudi_test" + new Date().getTime();
    String path = new Path(basePath.toAbsolutePath().toString()).toUri().toString();

    spark.sql("set hoodie.schema.on.read.enable=true");
    spark.sql("set hoodie.datasource.write.schema.allow.auto.evolution.column.drop=true");
    spark.sql(String.format("create table %s (col0 int, col1 float, col2 string) using hudi "
            + "tblproperties (type='%s', primaryKey='col0', preCombineField='col1', "
            + "hoodie.compaction.payload.class='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload') location '%s'",
        tableName, tableType, path));
    spark.sql(String.format("insert into %s values(1, 1.1, 'text')", tableName));
    spark.sql(String.format("update %s set col2 = 'text2' where col0 = 1", tableName));
    spark.sql(String.format("alter table %s alter column col1 type double", tableName));
    spark.sql(String.format("alter table %s rename column col2 to col2_new", tableName));

    JobConf jobConf = new JobConf();
    jobConf.set(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
    jobConf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "col1,col2_new");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "6,7");
    jobConf.set(serdeConstants.LIST_COLUMNS, "_hoodie_commit_time,_hoodie_commit_seqno,"
        + "_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,col0,col1,col2_new");
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string,string,int,double,string");
    FileInputFormat.setInputPaths(jobConf, path);

    HoodieParquetInputFormat inputFormat = "cow".equals(tableType) ? new HoodieParquetInputFormat()
        : new HoodieParquetRealtimeInputFormat();
    inputFormat.setConf(jobConf);

    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    assertEquals(1, splits.length);

    RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat.getRecordReader(splits[0], jobConf, null);
    List<List<Writable>> records = getWritableList(recordReader);
    assertEquals(1, records.size());
    List<Writable> record1 = records.get(0);
    if ("cow".equals(tableType)) {
      // col1, col2_new
      assertEquals(2, record1.size());

      Writable c1 = record1.get(0);
      assertTrue(c1 instanceof DoubleWritable);
      assertEquals("1.1", c1.toString().substring(0, 3));

      Writable c2 = record1.get(1);
      assertTrue(c2 instanceof Text);
      assertEquals("text2", c2.toString());
    } else {
      // _hoodie_record_key,_hoodie_commit_time,_hoodie_partition_path, col1, col2_new
      assertEquals(5, record1.size());

      Writable c1 = record1.get(3);
      assertTrue(c1 instanceof DoubleWritable);
      assertEquals("1.1", c1.toString().substring(0, 3));

      Writable c2 = record1.get(4);
      assertTrue(c2 instanceof Text);
      assertEquals("text2", c2.toString());
    }
    recordReader.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"mor","cow"})
  public void testHiveReadSchemaEvolutionWithAddingColumns(String tableType) throws Exception {
    String tableName = "hudi_test" + new Date().getTime();
    String path = new Path(basePath.toAbsolutePath().toString()).toUri().toString();

    spark.sql("set hoodie.schema.on.read.enable=true");

    spark.sql(String.format("create table %s (col0 int, col1 float, col2 string, col3 timestamp) using hudi "
                    + "tblproperties (type='%s', primaryKey='col0', preCombineField='col1', "
                    + "hoodie.compaction.payload.class='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload') location '%s'",
            tableName, tableType, path));
    spark.sql(String.format("insert into %s values(1, 1.1, 'text', timestamp('2021-12-25 12:01:01'))", tableName));
    spark.sql(String.format("update %s set col2 = 'text2' where col0 = 1", tableName));
    spark.sql(String.format("alter table %s add columns (col4 string)", tableName));

    JobConf jobConf = new JobConf();
    jobConf.set(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
    jobConf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "col1,col2,col3,col4");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "6,7,8,9");
    jobConf.set(serdeConstants.LIST_COLUMNS, "_hoodie_commit_time,_hoodie_commit_seqno,"
            + "_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,col0,col1,col2,col3,col4");
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string,string,int,float,string,timestamp,string,string");
    FileInputFormat.setInputPaths(jobConf, path);

    HoodieParquetInputFormat inputFormat =
            tableType.equals("cow") ? new HoodieParquetInputFormat() : new HoodieParquetRealtimeInputFormat();
    inputFormat.setConf(jobConf);

    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    int expectedSplits = 1;
    assertEquals(expectedSplits, splits.length);

    RecordReader<NullWritable, ArrayWritable> recordReader = inputFormat.getRecordReader(splits[0], jobConf, null);
    List<List<Writable>> records = getWritableList(recordReader, false);
    assertEquals(1, records.size());
    List<Writable> record1 = records.get(0);
    assertEquals(10, record1.size());
    assertEquals(new FloatWritable(1.1f), record1.get(6));
    assertEquals(new Text("text2"), record1.get(7));
    assertInstanceOf(TimestampWritable.class, record1.get(8));
    // field-9 is new added column without any inserts.
    assertNull(record1.get(9));
    recordReader.close();
  }

  private List<List<Writable>> getWritableList(RecordReader<NullWritable, ArrayWritable> recordReader) throws IOException {
    return getWritableList(recordReader, true);
  }

  private List<List<Writable>> getWritableList(RecordReader<NullWritable, ArrayWritable> recordReader, boolean filterNull) throws IOException {
    List<List<Writable>> records = new ArrayList<>();
    NullWritable key = recordReader.createKey();
    ArrayWritable writable = recordReader.createValue();
    while (writable != null && recordReader.next(key, writable)) {
      records.add(Arrays.stream(writable.get())
          .filter(f -> !filterNull || Objects.nonNull(f))
          .collect(Collectors.toList()));
    }
    return records;
  }
}

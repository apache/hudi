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

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
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

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  public void testHiveReadSchemaEvolutionTable(String tableType) throws Exception {
    if (HoodieSparkUtils.gteqSpark3_1()) {
      String tableName = "hudi_test" + new Date().getTime();
      String path = new Path(basePath.toAbsolutePath().toString()).toUri().toString();

      spark.sql("set hoodie.schema.on.read.enable=true");
      spark.sql(String.format("create table %s (col0 int, col1 float, col2 string) using hudi "
              + "tblproperties (type='%s', primaryKey='col0', preCombineField='col1') location '%s'",
          tableName, tableType, path));
      spark.sql(String.format("insert into %s values(1, 1.1, 'text')", tableName));
      spark.sql(String.format("update %s set col2 = 'text2' where col0 = 1", tableName));
      spark.sql(String.format("alter table %s alter column col1 type double", tableName));
      spark.sql(String.format("alter table %s rename column col2 to col2_new", tableName));

      JobConf jobConf = new JobConf();
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
  }

  private List<List<Writable>> getWritableList(RecordReader<NullWritable, ArrayWritable> recordReader) throws IOException {
    List<List<Writable>> records = new ArrayList<>();
    NullWritable key = recordReader.createKey();
    ArrayWritable writable = recordReader.createValue();
    while (writable != null && recordReader.next(key, writable)) {
      records.add(Arrays.stream(writable.get())
          .filter(Objects::nonNull)
          .collect(Collectors.toList()));
    }
    return records;
  }
}

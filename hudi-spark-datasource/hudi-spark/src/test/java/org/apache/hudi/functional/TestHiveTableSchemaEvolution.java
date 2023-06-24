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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.SchemaEvolutionContext;
import org.apache.hudi.hadoop.realtime.HoodieEmptyRecordReader;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader;
import org.apache.hudi.hadoop.realtime.RealtimeCompactedRecordReader;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;

import com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Date;

import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestHiveTableSchemaEvolution {

  private SparkSession sparkSession = null;

  @TempDir
  java.nio.file.Path basePath;

  @BeforeEach
  public void setUp() {
    initSparkContexts("HiveSchemaEvolution");
  }

  private void initSparkContexts(String appName) {
    SparkConf sparkConf = getSparkConfForTest(appName);

    sparkSession = SparkSession.builder()
        .config("hoodie.support.write.lock", "false")
        .config("spark.sql.session.timeZone", "CTT")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .config(sparkConf)
        .getOrCreate();

    sparkSession.sparkContext().setLogLevel("ERROR");
  }

  @Test
  public void testCopyOnWriteTableForHive() throws Exception {
    String tableName = "huditest" + new Date().getTime();
    if (HoodieSparkUtils.gteqSpark3_1()) {
      sparkSession.sql("set hoodie.schema.on.read.enable=true");
      String path = new Path(basePath.toAbsolutePath().toString()).toUri().toString();
      sparkSession.sql("create table " + tableName + "(col0 int, col1 float, col2 string) using hudi options(type='cow', primaryKey='col0', preCombineField='col1') location '" + path + "'");
      sparkSession.sql("insert into " + tableName + " values(1, 1.1, 'text')");
      sparkSession.sql("alter table " + tableName + " alter column col1 type double");
      sparkSession.sql("alter table " + tableName + " rename column col2 to aaa");

      HoodieParquetInputFormat inputFormat = new HoodieParquetInputFormat();
      JobConf jobConf = new JobConf();
      inputFormat.setConf(jobConf);
      FileInputFormat.setInputPaths(jobConf, path);
      InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
      assertEvolutionResult("cow", splits[0], jobConf);
    }
  }

  @Test
  public void testMergeOnReadTableForHive() throws Exception {
    String tableName = "huditest" + new Date().getTime();
    if (HoodieSparkUtils.gteqSpark3_1()) {
      sparkSession.sql("set hoodie.schema.on.read.enable=true");
      String path = new Path(basePath.toAbsolutePath().toString()).toUri().toString();
      sparkSession.sql("create table " + tableName + "(col0 int, col1 float, col2 string) using hudi options(type='cow', primaryKey='col0', preCombineField='col1') location '" + path + "'");
      sparkSession.sql("insert into " + tableName + " values(1, 1.1, 'text')");
      sparkSession.sql("insert into " + tableName + " values(2, 1.2, 'text2')");
      sparkSession.sql("alter table " + tableName + " alter column col1 type double");
      sparkSession.sql("alter table " + tableName + " rename column col2 to aaa");

      HoodieRealtimeInputFormat inputFormat = new HoodieRealtimeInputFormat();
      JobConf jobConf = new JobConf();
      inputFormat.setConf(jobConf);
      FileInputFormat.setInputPaths(jobConf, path);
      InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
      assertEvolutionResult("mor", splits[0], jobConf);
    }
  }

  private void assertEvolutionResult(String tableType, InputSplit split, JobConf jobConf) throws Exception {
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "col1,aaa");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "6,7");
    jobConf.set(serdeConstants.LIST_COLUMNS, "_hoodie_commit_time,_hoodie_commit_seqno,"
        + "_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,col0,col1,aaa");
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string,string,int,double,string");

    SchemaEvolutionContext schemaEvolutionContext = new SchemaEvolutionContext(split, jobConf);
    if ("cow".equals(tableType)) {
      schemaEvolutionContext.doEvolutionForParquetFormat();
    } else {
      // mot table
      RealtimeSplit realtimeSplit = (RealtimeSplit) split;
      RecordReader recordReader;
      // for log only split, set the parquet reader as empty.
      if (FSUtils.isLogFile(realtimeSplit.getPath())) {
        recordReader = new HoodieRealtimeRecordReader(realtimeSplit, jobConf, new HoodieEmptyRecordReader(realtimeSplit, jobConf));
      } else {
        // create a RecordReader to be used by HoodieRealtimeRecordReader
        recordReader = new MapredParquetInputFormat().getRecordReader(realtimeSplit, jobConf, null);
      }
      RealtimeCompactedRecordReader realtimeCompactedRecordReader = new RealtimeCompactedRecordReader(realtimeSplit, jobConf, recordReader);
      // mor table also run with doEvolutionForParquetFormat in HoodieParquetInputFormat
      schemaEvolutionContext.doEvolutionForParquetFormat();
      schemaEvolutionContext.doEvolutionForRealtimeInputFormat(realtimeCompactedRecordReader);
    }

    assertEquals(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR), "col1,col2");
    assertEquals(jobConf.get(serdeConstants.LIST_COLUMNS), "_hoodie_commit_time,_hoodie_commit_seqno,"
        + "_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,col0,col1,col2");
    assertEquals(jobConf.get(serdeConstants.LIST_COLUMN_TYPES), "string,string,string,string,string,int,double,string");
  }
}

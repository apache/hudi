/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.schemaevo;

import org.apache.flink.table.api.TableResult;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.ExecutionException;

import static org.apache.hudi.config.HoodieWriteConfig.SCHEMA_EVOLUTION_ENABLE;

/**
 * Tests of reading when schema evolution enabled.
 */
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlDialectInspection"})
public class ITTestReadWithSchemaEvo extends TestSchemaEvoBase {

  @TestWhenSparkGreaterThan31
  public void testReadSnapshotBatchCOW() throws ExecutionException, InterruptedException {
    String tablePath = tempFile.getAbsolutePath();
    try (SparkSession spark = spark()) {
      spark.sql(String.format("set %s=true", SCHEMA_EVOLUTION_ENABLE.key()));
      createAndPreparePartitionTable(spark, "t1", tablePath, "cow");
      spark.sql("alter table t1 add columns(newCol1 boolean after col4)");
      spark.sql("alter table t1 rename column col1 to renamedCol1");
      spark.sql("alter table t1 drop column col3");
      spark.sql("alter table t1 alter column col6 type string");
      spark.sql("alter table t1 add columns(newCol2 int after col8)");
      spark.sql("insert into t1 values (1,1,11,100001,101.01,100001.0001,true,'a000001','date->string','2021-12-25 12:01:01',true,10,'a01','2021-12-25')");
    }
    //language=SQL
    tEnv.executeSql(
            "create table t1 ("
              + "  id int,"
              + "  comb int,"
              + "  col0 int,"
              + "  renamedCol1 bigint,"
              + "  col2 float,"
              + "  col4 decimal(10,4),"
              + "  newCol1 boolean,"
              + "  col5 string,"
              + "  col6 string,"
              + "  col7 timestamp,"
              + "  col8 boolean,"
              + "  newCol2 int,"
              + "  col9 binary,"
              + "  par date"
              + " )"
              + " partitioned by (par) with ("
              + "  'connector' = 'hudi',"
              + "  'path' = '" + tablePath + "',"
              + "  'table.type' = 'COPY_ON_WRITE',"
              + "  'read.streaming.enabled' = 'false',"
              + "  'read.tasks' = '1',"
              + "  'hoodie.datasource.query.type' = 'snapshot',"
              + "  'hoodie.datasource.write.recordkey.field' = 'id',"
              + "  'hoodie.datasource.write.hive_style_partitioning' = 'true',"
              + "  'hoodie.datasource.write.keygenerator.type' = 'COMPLEX',"
              + "  'hoodie.schema.on.read.enable' = 'true'"
              + ")"
      ).await();
    TableResult tableResult = tEnv.executeSql("select par, newCol2, col6, newCol1, col4, renamedCol1, id from t1");
    checkAnswer(
            tableResult,
            "+I[2021-12-25, 10, date->string, true, 100001.0001, 100001, 1]",
            "+I[2021-12-25, null, 2021-12-25, null, 100002.0002, 100002, 2]",
            "+I[2021-12-25, null, 2021-12-25, null, 100003.0003, 100003, 3]",
            "+I[2021-12-26, null, 2021-12-26, null, 100004.0004, 100004, 4]",
            "+I[2021-12-26, null, 2021-12-26, null, 100005.0005, 100005, 5]"
    );
  }
}

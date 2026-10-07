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

package org.apache.spark.sql.hudi.dml

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

/**
 * Characterizes how the 0.x write/merge path handles an ordering (precombine) value that is
 * already persisted as null in a base file. A record with a null precombine value is first
 * seeded via bulk_insert (the row-writer path, which does not go through the payload constructor
 * that rejects a null ordering value), then a real-ordering record is merged onto the same key.
 *
 * On the 0.x payload-based merge path this resolves cleanly (the persisted null loses to the
 * incoming value). The 1.x engine-native merger instead compares a boxed default Integer against
 * the incoming Long and fails with a ClassCastException, so this suite is the 0.x control.
 */
class TestPersistedNullOrdering extends HoodieSparkSqlTestBase {

  private val schema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("ts", LongType, nullable = true)))

  private def rowDf(id: Int, name: String, ts: java.lang.Long): DataFrame =
    spark.createDataFrame(java.util.Collections.singletonList(Row(id, name, ts)), schema)

  private def write(path: String, table: String, op: String, mode: SaveMode, df: DataFrame): Unit = {
    df.write.format("hudi")
      .option("hoodie.table.name", table)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "")
      .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .option("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.DefaultHoodieRecordPayload")
      .option("hoodie.datasource.write.operation", op)
      .option("hoodie.datasource.write.row.writer.enable", "true")
      .option("hoodie.metadata.enable", "false")
      .mode(mode)
      .save(path)
  }

  test("persisted null ordering value merges cleanly on 0.x - Spark DataSource") {
    withTempDir { dir =>
      val path = new java.io.File(dir, "ds_persisted_null").getCanonicalPath

      // 1. Seed a persisted null ordering value. bulk_insert uses the row-writer path, which does
      //    not construct the payload that would otherwise reject a null precombine value.
      write(path, "ds_persisted_null", "bulk_insert", SaveMode.Overwrite, rowDf(1, "base", null))

      // Confirm the null really landed in the base file.
      val seeded = spark.read.format("hudi").load(path).select("id", "name", "ts").collect()
      assert(seeded.length == 1 && seeded(0).isNullAt(2),
        s"expected a persisted null ordering value, got ${seeded.map(_.toString).mkString(",")}")

      // 2. Upsert a real-ordering record on the same key. This merges the incoming Long ordering
      //    value against the persisted null, the exact path that ClassCastExceptions on 1.x.
      write(path, "ds_persisted_null", "upsert", SaveMode.Append, rowDf(1, "updated", 100L))

      // 3. The merge succeeds and the incoming record wins.
      checkAnswer(spark.read.format("hudi").load(path).select("id", "name", "ts").collect())(
        Seq(1, "updated", 100L))
    }
  }

  test("persisted null ordering value merges cleanly on 0.x - Spark SQL") {
    withTempDir { dir =>
      val tbl = generateTableName
      val path = new java.io.File(dir, "sql_persisted_null").getCanonicalPath
      spark.sql(
        s"""create table $tbl (id int, name string, ts long) using hudi
           | tblproperties (primaryKey = 'id', preCombineField = 'ts', type = 'cow')
           | location '$path'""".stripMargin)

      // 1. Seed a persisted null ordering value via bulk_insert mode (row-writer path).
      withSQLConf("hoodie.sql.bulk.insert.enable" -> "true", "hoodie.sql.insert.mode" -> "non-strict") {
        spark.sql(s"insert into $tbl values (1, 'base', cast(null as long))")
      }
      val seeded = spark.sql(s"select ts from $tbl").collect()
      assert(seeded.length == 1 && seeded(0).isNullAt(0),
        s"expected a persisted null ordering value via SQL, got ${seeded.map(_.toString).mkString(",")}")

      // 2. Merge a real-ordering record against the persisted null (upsert path).
      spark.sql(
        s"""merge into $tbl t using (select 1 as id, 'updated' as name, 100L as ts) s
           | on t.id = s.id
           | when matched then update set *
           | when not matched then insert *""".stripMargin)

      // 3. The merge succeeds and the incoming record wins.
      checkAnswer(s"select id, name, ts from $tbl")(Seq(1, "updated", 100L))
    }
  }
}

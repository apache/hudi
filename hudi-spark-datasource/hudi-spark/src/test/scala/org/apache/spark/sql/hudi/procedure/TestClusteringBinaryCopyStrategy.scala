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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.WriteClientTestUtils
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{Option => HOption}

import org.apache.spark.SparkConf

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate

class TestClusteringBinaryCopyStrategy extends HoodieSparkProcedureTestBase {

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.hadoop.parquet.avro.write-old-list-structure", "false")
      .set("spark.sql.defaultColumn.enabled", "false")
  }

  Seq("bulk_insert", "insert").foreach { operation =>
    test(s"Test run_clustering using binary stream copy, cow table prepared by $operation operation") {
      withTempDir { tmp =>
        val conf = operation match {
          case "bulk_insert" =>
            Map(
              "hoodie.sql.bulk.insert.enable" -> "true",
              "hoodie.sql.insert.mode" -> "non-strict",
              "hoodie.combine.before.insert" -> "false",
              "hoodie.parquet.small.file.limit" -> "-1",
              "hoodie.clustering.plan.strategy.binary.copy.schema.evolution.enable" -> "true"
            )
          case "insert" =>
            Map(
              "hoodie.datasource.write.operation" -> "insert",
              "hoodie.sql.insert.mode" -> "non-strict",
              "hoodie.combine.before.insert" -> "false",
              "spark.hadoop.parquet.avro.write-old-list-structure" -> "false",
              "hoodie.parquet.small.file.limit" -> "-1",
              "hoodie.clustering.plan.strategy.binary.copy.schema.evolution.enable" -> "true"
            )
        }
        withSQLConf(conf.toSeq: _*) {
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = 'cow',
               |  preCombineField = 'price'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(intType int)")
          spark.sql(
            s"""
               |insert into $tableName (id, name, price, intType, ts)
               |values (4, 'a2', 2, 1, 1000), (5, 'a3', 3, cast(NULL as int), 1001)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(longType long)")
          spark.sql(s"insert into $tableName (id, name, price, intType, longType, ts) values (6, 'a2', 2, 1, 22, 1000)")

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(floatType float)")
          spark.sql(
            s"""
               |insert into $tableName (id, name, price, intType, longType, floatType, ts)
               |values (7, 'a6', 6, 6, 22, cast(66 as float), 1000)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(doubleType double)")
          spark.sql(
            s"""
               |insert into $tableName (id, name, price, intType, longType, floatType, doubleType, ts)
               |values (8, 'a6', 5.5d, 5, 22, cast(66 as float), cast(77 as double), 1000)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(arrayType ARRAY<INT>)")
          spark.sql(
            s"""
               |insert into $tableName (id, name, price, intType, longType, floatType, doubleType, arrayType, ts)
               |values (9, 'a6', 6, 6, 22, cast(66 as float), cast(77 as double), array(1,2), 1000)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(mapType Map<String,String>)")
          spark.sql(
            s"""
               |insert into $tableName (
               |  id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, ts
               |) values (
               |  10, 'a6', 6, 6, 22, cast(66 as float), cast(77 as double), array(1,2), MAP('key1','value1'), 1001
               |)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(timestampType timestamp)")
          spark.sql(
            s"""
               |insert into $tableName (
               |  id, name, price, intType, longType, floatType, doubleType, arrayType, mapType,timestampType, ts
               |)
               |values (
               |  11, 'a6', 6.5d, 6, 22, cast(66 as float), cast(77 as double), array(1,2), MAP('key1','value1'),
               |cast('2024-10-11 11:18:01' as timestamp), 1001
               |)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(dateType Date)")
          spark.sql(
            s"""
               |insert into $tableName (
               |  id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, timestampType,
               |  dateType, ts
               |) values (
               |  12, 'a6', 6.5d, 6, 22, cast(66 as float), cast(77 as double), array(1,2), MAP('key1','value1'),
               |  cast('2024-10-11 11:18:01' as timestamp), cast('2024-10-12 11:18:02' as Date), 1001
               |)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(decimalType decimal(3,1))")
          spark.sql(
            s"""
               |insert into $tableName (
               |  id, name, price, intType, longType, floatType, doubleType, arrayType, mapType,timestampType, dateType,
               |  decimalType, ts
               |) values (
               |  13, 'a6', 6.5d, 6, 22, cast(66 as float), cast(77 as double), array(1,2), MAP('key1','value1'),
               |  cast('2024-10-11 11:18:01' as timestamp), cast('2024-10-12 11:18:02' as Date), cast(1 as decimal(3,1)),
               |  1001
               |)
               |""".stripMargin)

          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(booleanType boolean)")
          spark.sql(
            s"""
               |insert into $tableName (
               |  id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, timestampType, dateType,
               |  decimalType, booleanType, ts
               |) values(
               |  14, 'a6', 6.5d, 6, 22, cast(66 as float), cast(77 as double), array(1,2), MAP('key1','value1'),
               |  cast('2024-10-11 11:18:01' as timestamp), cast('2024-10-12 11:18:02' as Date), cast(1 as decimal(3,1)),
               |  true, 1001
               |)
               |""".stripMargin)

          spark.sql(
            s"""
               |ALTER TABLE $tableName
               |ADD COLUMNS(structType STRUCT<firstname:String, middlename:String,lastname:String >)
               |""".stripMargin)
          spark.sql(
            s"""
               |insert into $tableName (
               |  id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, timestampType, dateType,
               |  decimalType, booleanType, structType, ts
               |) values(
               |  15, 'a6', 6.5d, 6, 22, cast(66 as float), cast(77 as double), array(1,2), MAP('key1','value1'),
               |  cast('2024-10-11 11:18:01' as timestamp),cast('2024-10-12 11:18:02' as Date), cast(1 as decimal(3,1)),
               |  true, STRUCT('2','2','2'), 1001
               |)
               |""".stripMargin)

          // Generate the first clustering plan
          val firstScheduleInstant = WriteClientTestUtils.createNewInstantTime()
          client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())
          checkAnswer(
            s"""
               |call run_clustering(
               |  op => 'execute',
               |  table => '$tableName',
               |  order => 'ts',
               |  options => 'hoodie.clustering.execution.strategy.class=
               |    org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy'
               |)
               |""".stripMargin)(
            Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "*")
          )

          checkAnswer(
            s"""
               |select id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, timestampType,
               |  dateType, decimalType, booleanType, structType.firstname, ts
               |from $tableName
               |""".stripMargin)(
            Seq(1, "a1", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1000),
            Seq(10, "a6", 6.0, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), null, null, null, null, null, 1001),
            Seq(11, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              null, null, null, null, 1001),
            Seq(12, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), null, null, null, 1001),
            Seq(13, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), null, null, 1001),
            Seq(14, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, null, 1001),
            Seq(15, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, "2", 1001),
            Seq(2, "a2", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1001),
            Seq(3, "a3", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1002),
            Seq(4, "a2", 2.0, 1, null, null, null, null, null, null, null, null, null, null, 1000),
            Seq(5, "a3", 3.0, null, null, null, null, null, null, null, null, null, null, null, 1001),
            Seq(6, "a2", 2.0, 1, 22, null, null, null, null, null, null, null, null, null, 1000),
            Seq(7, "a6", 6.0, 6, 22, 66.0, null, null, null, null, null, null, null, null, 1000),
            Seq(8, "a6", 5.5, 5, 22, 66.0, 77.0, null, null, null, null, null, null, null, 1000),
            Seq(9, "a6", 6.0, 6, 22, 66.0, 77.0, Seq(1, 2), null, null, null, null, null, null, 1000)
          )
          spark.sql(
            s"""
               |insert into $tableName (
               |  id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, timestampType, dateType,
               |  decimalType, booleanType, structType, ts
               |) values (
               |  16, 'a6', 6.5d, 6, 22, cast(66 as float), cast(77 as double), array(1,2), MAP('key1','value1'),
               |  cast('2024-10-11 11:18:01' as timestamp), cast('2024-10-12 11:18:02' as Date), cast(1 as decimal(3,1)),
               |  true, STRUCT('4','4','4'), 1001
               |)""".stripMargin)
          checkAnswer(
            s"""
               |select id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, timestampType,
               |  dateType, decimalType, booleanType, structType.firstname, ts
               |from $tableName
               |""".stripMargin)(
            Seq(1, "a1", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1000),
            Seq(10, "a6", 6.0, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), null, null, null, null, null, 1001),
            Seq(11, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              null, null, null, null, 1001),
            Seq(12, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), null, null, null, 1001),
            Seq(13, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), null, null, 1001),
            Seq(14, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, null, 1001),
            Seq(15, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, "2", 1001),
            Seq(16, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, "4", 1001),
            Seq(2, "a2", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1001),
            Seq(3, "a3", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1002),
            Seq(4, "a2", 2.0, 1, null, null, null, null, null, null, null, null, null, null, 1000),
            Seq(5, "a3", 3.0, null, null, null, null, null, null, null, null, null, null, null, 1001),
            Seq(6, "a2", 2.0, 1, 22, null, null, null, null, null, null, null, null, null, 1000),
            Seq(7, "a6", 6.0, 6, 22, 66.0, null, null, null, null, null, null, null, null, 1000),
            Seq(8, "a6", 5.5, 5, 22, 66.0, 77.0, null, null, null, null, null, null, null, 1000),
            Seq(9, "a6", 6.0, 6, 22, 66.0, 77.0, Seq(1, 2), null, null, null, null, null, null, 1000)
          )

          // Generate the second clustering plan
          val secondScheduleInstant = WriteClientTestUtils.createNewInstantTime()
          client.scheduleClusteringAtInstant(secondScheduleInstant, HOption.empty())
          checkAnswer(
            s"""
               |call run_clustering(
               |  op => 'execute',
               |  table => '$tableName',
               |  order => 'ts',
               |  options => 'hoodie.clustering.execution.strategy.class=
               |    org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy'
               |)
               |""".stripMargin)(
            Seq(secondScheduleInstant, 1, HoodieInstant.State.COMPLETED.name(), "*")
          )
          checkAnswer(
            s"""
               |select id, name, price, intType, longType, floatType, doubleType, arrayType, mapType, timestampType,
               |  dateType, decimalType, booleanType, structType.firstname, ts
               |from $tableName
               |""".stripMargin)(
            Seq(1, "a1", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1000),
            Seq(10, "a6", 6.0, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), null, null, null, null, null, 1001),
            Seq(11, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              null, null, null, null, 1001),
            Seq(12, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), null, null, null, 1001),
            Seq(13, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), null, null, 1001),
            Seq(14, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, null, 1001),
            Seq(15, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, "2", 1001),
            Seq(16, "a6", 6.5, 6, 22, 66.0, 77.0, Seq(1, 2), Map("key1" -> "value1"), time("2024-10-11 11:18:01.0"),
              date("2024-10-12"), new BigDecimal("1.0"), true, "4", 1001),
            Seq(2, "a2", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1001),
            Seq(3, "a3", 10.0, null, null, null, null, null, null, null, null, null, null, null, 1002),
            Seq(4, "a2", 2.0, 1, null, null, null, null, null, null, null, null, null, null, 1000),
            Seq(5, "a3", 3.0, null, null, null, null, null, null, null, null, null, null, null, 1001),
            Seq(6, "a2", 2.0, 1, 22, null, null, null, null, null, null, null, null, null, 1000),
            Seq(7, "a6", 6.0, 6, 22, 66.0, null, null, null, null, null, null, null, null, 1000),
            Seq(8, "a6", 5.5, 5, 22, 66.0, 77.0, null, null, null, null, null, null, null, 1000),
            Seq(9, "a6", 6.0, 6, 22, 66.0, 77.0, Seq(1, 2), null, null, null, null, null, null, 1000)
          )
        }
      }
    }
  }

  def time(value: String): Timestamp = {
    Timestamp.valueOf(value)
  }

  def date(value: String): Date = {
    Date.valueOf(LocalDate.parse(value))
  }
}

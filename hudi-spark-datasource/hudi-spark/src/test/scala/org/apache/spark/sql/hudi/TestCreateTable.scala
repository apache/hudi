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

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class TestCreateTable extends TestHoodieSqlBase {

  test("Test Create Managed Hoodie Table") {
    val tableName = generateTableName
    // Create a managed table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | tblproperties (
         |   primaryKey = 'id',
         |   preCombineField = 'ts'
         | )
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    assertResult(tableName)(table.identifier.table)
    assertResult("hudi")(table.provider.get)
    assertResult(CatalogTableType.MANAGED)(table.tableType)
    assertResult(
      HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
        ++ Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("price", DoubleType),
        StructField("ts", LongType))
    )(table.schema.fields)
  }

  test("Test Create Hoodie Table With Options") {
    val tableName = generateTableName
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  dt string
         | ) using hudi
         | partitioned by (dt)
         | options (
         |   primaryKey = 'id',
         |   preCombineField = 'ts'
         | )
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    assertResult(table.properties("type"))("cow")
    assertResult(table.properties("primaryKey"))("id")
    assertResult(table.properties("preCombineField"))("ts")
    assertResult(tableName)(table.identifier.table)
    assertResult("hudi")(table.provider.get)
    assertResult(CatalogTableType.MANAGED)(table.tableType)
    assertResult(
      HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
      ++ Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("price", DoubleType),
        StructField("ts", LongType),
        StructField("dt", StringType))
    )(table.schema.fields)

    val tablePath = table.storage.properties("path")
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(tablePath)
      .setConf(spark.sessionState.newHadoopConf())
      .build()
    val tableConfig = metaClient.getTableConfig.getProps.asScala.toMap
    assertResult(true)(tableConfig.contains(HoodieTableConfig.CREATE_SCHEMA.key))
    assertResult("dt")(tableConfig(HoodieTableConfig.PARTITION_FIELDS.key))
    assertResult("id")(tableConfig(HoodieTableConfig.RECORDKEY_FIELDS.key))
    assertResult("ts")(tableConfig(HoodieTableConfig.PRECOMBINE_FIELD.key))
    assertResult(classOf[ComplexKeyGenerator].getCanonicalName)(tableConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key))
  }

  test("Test Create External Hoodie Table") {
    withTempDir { tmp =>
      // Test create cow table.
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id,name',
           |  type = 'cow'
           | )
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))

      assertResult(tableName)(table.identifier.table)
      assertResult("hudi")(table.provider.get)
      assertResult(CatalogTableType.EXTERNAL)(table.tableType)
      assertResult(
        HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
          ++ Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("price", DoubleType),
          StructField("ts", LongType))
      )(table.schema.fields)
      assertResult(table.properties("type"))("cow")
      assertResult(table.properties("primaryKey"))("id,name")

      spark.sql(s"drop table $tableName")
      // Test create mor partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | partitioned by (dt)
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'mor'
           | )
           | location '${tmp.getCanonicalPath}/h0'
       """.stripMargin)
      val table2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assertResult(table2.properties("type"))("mor")
      assertResult(table2.properties("primaryKey"))("id")
      assertResult(Seq("dt"))(table2.partitionColumnNames)
      assertResult(classOf[HoodieParquetRealtimeInputFormat].getCanonicalName)(table2.storage.inputFormat.get)

      // Test create a external table with an exist table in the path
      val tableName3 = generateTableName
      spark.sql(
        s"""
           |create table $tableName3
           |using hudi
           |location '${tmp.getCanonicalPath}/h0'
         """.stripMargin)
      val table3 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName3))
      assertResult(table3.properties("type"))("mor")
      assertResult(table3.properties("primaryKey"))("id")
      assertResult(
        HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
          ++ Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("price", DoubleType),
          StructField("ts", LongType),
          StructField("dt", StringType)
        )
      )(table3.schema.fields)
    }
  }

  test("Test Table Column Validate") {
    withTempDir {tmp =>
      val tableName = generateTableName
      assertThrows[IllegalArgumentException] {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id1',
             |  type = 'cow'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      }

      assertThrows[IllegalArgumentException] {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  preCombineField = 'ts1',
             |  type = 'cow'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      }

      assertThrows[IllegalArgumentException] {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  type = 'cow1'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      }
    }
  }

  test("Test Create Table As Select") {
    withTempDir { tmp =>
      // Create Non-Partitioned table
      val tableName1 = generateTableName
      spark.sql(
        s"""
           | create table $tableName1 using hudi
           | tblproperties(primaryKey = 'id')
           | location '${tmp.getCanonicalPath}/$tableName1'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
       """.stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName1")(
        Seq(1, "a1", 10.0, 1000)
      )

      // Create Partitioned table
      val tableName2 = generateTableName
      spark.sql(
        s"""
           | create table $tableName2 using hudi
           | partitioned by (dt)
           | tblproperties(primaryKey = 'id')
           | location '${tmp.getCanonicalPath}/$tableName2'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, '2021-04-01' as dt
         """.stripMargin
      )
      checkAnswer(s"select id, name, price, dt from $tableName2") (
        Seq(1, "a1", 10, "2021-04-01")
      )

      // Create Partitioned table with timestamp data type
      val tableName3 = generateTableName
      // CTAS failed with null primaryKey
      assertThrows[Exception] {
      spark.sql(
        s"""
           | create table $tableName3 using hudi
           | partitioned by (dt)
           | tblproperties(primaryKey = 'id')
           | location '${tmp.getCanonicalPath}/$tableName3'
           | AS
           | select null as id, 'a1' as name, 10 as price, '2021-05-07' as dt
           |
         """.stripMargin
      )}
      // Create table with timestamp type partition
      spark.sql(
        s"""
           | create table $tableName3 using hudi
           | partitioned by (dt)
           | tblproperties(primaryKey = 'id')
           | location '${tmp.getCanonicalPath}/$tableName3'
           | AS
           | select cast('2021-05-06 00:00:00' as timestamp) as dt, 1 as id, 'a1' as name, 10 as
           | price
         """.stripMargin
      )
      checkAnswer(s"select id, name, price, cast(dt as string) from $tableName3")(
        Seq(1, "a1", 10, "2021-05-06 00:00:00")
      )
      // Create table with date type partition
      val tableName4 = generateTableName
      spark.sql(
        s"""
           | create table $tableName4 using hudi
           | partitioned by (dt)
           | tblproperties(primaryKey = 'id')
           | location '${tmp.getCanonicalPath}/$tableName4'
           | AS
           | select cast('2021-05-06' as date) as dt, 1 as id, 'a1' as name, 10 as
           | price
         """.stripMargin
      )
      checkAnswer(s"select id, name, price, cast(dt as string) from $tableName4")(
        Seq(1, "a1", 10, "2021-05-06")
      )
    }
  }

  test("Test Create Table From Exist Hoodie Table") {
    withTempDir { tmp =>
      Seq("2021-08-02", "2021/08/02").foreach { partitionValue =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        import spark.implicits._
        val df = Seq((1, "a1", 10, 1000, partitionValue)).toDF("id", "name", "value", "ts", "dt")
        // Write a table by spark dataframe.
        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // Create a table over the exist old table.
        spark.sql(
          s"""
             |create table $tableName using hudi
             |tblproperties (
             | primaryKey = 'id',
             | preCombineField = 'ts'
             |)
             |partitioned by (dt)
             |location '$tablePath'
             |""".stripMargin)
        checkAnswer(s"select id, name, value, ts, dt from $tableName")(
          Seq(1, "a1", 10, 1000, partitionValue)
        )
        // Check the missing properties for spark sql
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(spark.sessionState.newHadoopConf())
          .build()
        val properties = metaClient.getTableConfig.getProps.asScala.toMap
        assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
        assertResult("dt")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
        assertResult("ts")(properties(HoodieTableConfig.PRECOMBINE_FIELD.key))

        // Test insert into
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, '$partitionValue')")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("1", partitionValue, 1, "a1", 10, 1000, partitionValue),
          Seq("2", partitionValue, 2, "a2", 10, 1000, partitionValue)
        )
        // Test merge into
        spark.sql(
          s"""
             |merge into $tableName h0
             |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts, '$partitionValue' as dt) s0
             |on h0.id = s0.id
             |when matched then update set *
             |""".stripMargin)
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("1", partitionValue, 1, "a1", 11, 1001, partitionValue),
          Seq("2", partitionValue, 2, "a2", 10, 1000, partitionValue)
        )
        // Test update
        spark.sql(s"update $tableName set value = value + 1 where id = 2")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("1", partitionValue, 1, "a1", 11, 1001, partitionValue),
          Seq("2", partitionValue, 2, "a2", 11, 1000, partitionValue)
        )
        // Test delete
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("2", partitionValue, 2, "a2", 11, 1000, partitionValue)
        )
      }
    }
  }

  test("Test Create Table From Exist Hoodie Table For Multi-Level Partitioned Table") {
    withTempDir { tmp =>
      Seq("2021-08-02", "2021/08/02").foreach { day =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        import spark.implicits._
        val df = Seq((1, "a1", 10, 1000, day, 12)).toDF("id", "name", "value", "ts", "day", "hh")
        // Write a table by spark dataframe.
        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "day,hh")
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // Create a table over the exist old table.
        spark.sql(
          s"""
             |create table $tableName using hudi
             |tblproperties (
             | primaryKey = 'id',
             | preCombineField = 'ts'
             |)
             |partitioned by (day, hh)
             |location '$tablePath'
             |""".stripMargin)
        checkAnswer(s"select id, name, value, ts, day, hh from $tableName")(
          Seq(1, "a1", 10, 1000, day, 12)
        )
        // Check the missing properties for spark sql
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(spark.sessionState.newHadoopConf())
          .build()
        val properties = metaClient.getTableConfig.getProps.asScala.toMap
        assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
        assertResult("day,hh")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
        assertResult("ts")(properties(HoodieTableConfig.PRECOMBINE_FIELD.key))

        // Test insert into
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, '$day', 12)")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, day, hh from $tableName order by id")(
          Seq("id:1", s"$day/12", 1, "a1", 10, 1000, day, 12),
          Seq("id:2", s"$day/12", 2, "a2", 10, 1000, day, 12)
        )
        // Test merge into
        spark.sql(
          s"""
             |merge into $tableName h0
             |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts, '$day' as day, 12 as hh) s0
             |on h0.id = s0.id
             |when matched then update set *
             |""".stripMargin)
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, day, hh from $tableName order by id")(
          Seq("id:1", s"$day/12", 1, "a1", 11, 1001, day, 12),
          Seq("id:2", s"$day/12", 2, "a2", 10, 1000, day, 12)
        )
        // Test update
        spark.sql(s"update $tableName set value = value + 1 where id = 2")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, day, hh from $tableName order by id")(
          Seq("id:1", s"$day/12", 1, "a1", 11, 1001, day, 12),
          Seq("id:2", s"$day/12", 2, "a2", 11, 1000, day, 12)
        )
        // Test delete
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, day, hh from $tableName order by id")(
          Seq("id:2", s"$day/12", 2, "a2", 11, 1000, day, 12)
        )
      }
    }
  }

  test("Test Create Table From Exist Hoodie Table For None Partitioned Table") {
    withTempDir{tmp =>
      // Write a table by spark dataframe.
      val tableName = generateTableName
      import spark.implicits._
      val df = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "ts")
      df.write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, tableName)
        .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
        .option(RECORDKEY_FIELD.key, "id")
        .option(PRECOMBINE_FIELD.key, "ts")
        .option(PARTITIONPATH_FIELD.key, "")
        .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
        .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
        .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
        .mode(SaveMode.Overwrite)
        .save(tmp.getCanonicalPath)

      // Create a table over the exist old table.
      spark.sql(
        s"""
           |create table $tableName using hudi
           |tblproperties (
           | primaryKey = 'id',
           | preCombineField = 'ts'
           |)
           |location '${tmp.getCanonicalPath}'
           |""".stripMargin)
      checkAnswer(s"select id, name, value, ts from $tableName")(
        Seq(1, "a1", 10, 1000)
      )
      // Check the missing properties for spark sql
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tmp.getCanonicalPath)
        .setConf(spark.sessionState.newHadoopConf())
        .build()
      val properties = metaClient.getTableConfig.getProps.asScala.toMap
      assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
      assertResult("ts")(properties(HoodieTableConfig.PRECOMBINE_FIELD.key))

      // Test insert into
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts from $tableName order by id")(
        Seq("1", "", 1, "a1", 10, 1000),
        Seq("2", "", 2, "a2", 10, 1000)
      )
      // Test merge into
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts) s0
           |on h0.id = s0.id
           |when matched then update set *
           |""".stripMargin)
      checkAnswer(s"select id, name, value, ts from $tableName order by id")(
        Seq(1, "a1", 11, 1001),
        Seq(2, "a2", 10, 1000)
      )
      // Test update
      spark.sql(s"update $tableName set value = value + 1 where id = 2")
      checkAnswer(s"select id, name, value, ts from $tableName order by id")(
        Seq(1, "a1", 11, 1001),
        Seq(2, "a2", 11, 1000)
      )
      // Test delete
      spark.sql(s"delete from $tableName where id = 1")
      checkAnswer(s"select id, name, value, ts from $tableName order by id")(
        Seq(2, "a2", 11, 1000)
      )
    }
  }

  test("Test Create Table Exists In Catalog") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double
         |) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin
    )

    spark.sql(s"alter table $tableName add columns(ts bigint)")

    // Check "create table if not exist" works after schema evolution.
    spark.sql(
      s"""
         |create table if not exists $tableName (
         | id int,
         | name string,
         | price double
         |) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin
    )
  }
}

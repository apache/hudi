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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.functional.TestSparkSqlWithCustomKeyGenerator._
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient
import org.apache.hudi.util.SparkKeyGenUtils

import org.apache.avro.Schema
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.slf4j.LoggerFactory

/**
 * Tests Spark SQL DML with custom key generator and write configs.
 */
class TestSparkSqlWithCustomKeyGenerator extends HoodieSparkSqlTestBase {
  private val LOG = LoggerFactory.getLogger(getClass)

  test("Test Spark SQL DML with custom key generator") {
    for (extractPartition <- Seq(false)) {
      withSQLConf(EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key() -> extractPartition.toString) {
        withTempDir { tmp =>
          Seq(
            Seq("COPY_ON_WRITE", "ts:timestamp,segment:simple",
              "(ts=202401, segment='cat2')", "202401/cat2",
              Seq("202312/cat2", "202312/cat4", "202401/cat1", "202401/cat3", "202402/cat1", "202402/cat3", "202402/cat5"),
              (ts: Integer, segment: String) => TS_FORMATTER_FUNC.apply(ts) + "/" + segment, false),
            Seq("MERGE_ON_READ", "segment:simple",
              "(segment='cat3')", "cat3",
              Seq("cat1", "cat2", "cat4", "cat5"),
              (_: Integer, segment: String) => segment, false),
            Seq("MERGE_ON_READ", "ts:timestamp",
              "(ts=202312)", "202312",
              Seq("202401", "202402"),
              (ts: Integer, _: String) => TS_FORMATTER_FUNC.apply(ts), false),
            Seq("MERGE_ON_READ", "ts:timestamp,segment:simple",
              "(ts=202401, segment='cat2')", "202401/cat2",
              Seq("202312/cat2", "202312/cat4", "202401/cat1", "202401/cat3", "202402/cat1", "202402/cat3", "202402/cat5"),
              (ts: Integer, segment: String) => TS_FORMATTER_FUNC.apply(ts) + "/" + segment, false),
            Seq("MERGE_ON_READ", "ts:timestamp,segment:simple",
              "(ts=202401, segment='cat2')", "202401/cat2",
              Seq("202312/cat2", "202312/cat4", "202401/cat1", "202401/cat3", "202402/cat1", "202402/cat3", "202402/cat5"),
              (ts: Integer, segment: String) => TS_FORMATTER_FUNC.apply(ts) + "/" + segment, true)
          ).foreach { testParams =>
            withTable(generateTableName) { tableName =>
              LOG.warn("Testing with parameters: " + testParams)
              val tableType = testParams(0).asInstanceOf[String]
              val writePartitionFields = testParams(1).asInstanceOf[String]
              val dropPartitionStatement = testParams(2).asInstanceOf[String]
              val droppedPartition = testParams(3).asInstanceOf[String]
              val expectedPartitions = testParams(4).asInstanceOf[Seq[String]]
              val partitionGenFunc = testParams(5).asInstanceOf[(Integer, String) => String]
              val tablePath = tmp.getCanonicalPath + "/" + tableName
              val timestampKeyGeneratorConfig = if (writePartitionFields.contains("timestamp")) {
                TS_KEY_GEN_CONFIGS
              } else {
                Map[String, String]()
              }
              val timestampKeyGenProps = if (timestampKeyGeneratorConfig.nonEmpty) {
                ", " + timestampKeyGeneratorConfig.map(e => e._1 + " = '" + e._2 + "'").mkString(", ")
              } else {
                ""
              }
              val useOlderPartitionFieldFormat = testParams(6).asInstanceOf[Boolean]

              prepareTableWithKeyGenerator(
                tableName, tablePath, tableType,
                CUSTOM_KEY_GEN_CLASS_NAME, writePartitionFields, timestampKeyGeneratorConfig)

              if (useOlderPartitionFieldFormat) {
                var metaClient = createMetaClient(spark, tablePath)
                val props = new TypedProperties()
                props.put(HoodieTableConfig.PARTITION_FIELDS.key(), metaClient.getTableConfig.getPartitionFieldProp)
                HoodieTableConfig.update(metaClient.getStorage, metaClient.getMetaPath, props)
                metaClient = createMetaClient(spark, tablePath)
                assertEquals(metaClient.getTableConfig.getPartitionFieldProp, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).orElse(""))
              }

              // SQL CTAS with table properties containing key generator write configs
              createTableWithSql(tableName, tablePath,
                s"hoodie.datasource.write.partitionpath.field = '$writePartitionFields'" + timestampKeyGenProps)

              // Prepare source and test SQL INSERT INTO
              val sourceTableName = tableName + "_source"
              prepareParquetSource(sourceTableName, Seq(
                "(7, 'a7', 1399.0, 1706800227, 'cat1')",
                "(8, 'a8', 26.9, 1706800227, 'cat3')",
                "(9, 'a9', 299.0, 1701443427, 'cat4')"))
              spark.sql(
                s"""
                   | INSERT INTO $tableName
                   | SELECT * from ${tableName}_source
                   | """.stripMargin)
              val sqlStr = s"SELECT id, name, cast(price as string), ts, segment from $tableName"
              validateResults(
                tableName,
                sqlStr,
                partitionGenFunc,
                Seq(),
                Seq(1, "a1", "1.6", 1704121827, "cat1"),
                Seq(2, "a2", "10.8", 1704121827, "cat1"),
                Seq(3, "a3", "30.0", 1706800227, "cat1"),
                Seq(4, "a4", "103.4", 1701443427, "cat2"),
                Seq(5, "a5", "1999.0", 1704121827, "cat2"),
                Seq(6, "a6", "80.0", 1704121827, "cat3"),
                Seq(7, "a7", "1399.0", 1706800227, "cat1"),
                Seq(8, "a8", "26.9", 1706800227, "cat3"),
                Seq(9, "a9", "299.0", 1701443427, "cat4")
              )

              // Test SQL UPDATE
              spark.sql(
                s"""
                   | UPDATE $tableName
                   | SET price = price + 10.0
                   | WHERE id between 4 and 7
                   | """.stripMargin)
              validateResults(
                tableName,
                sqlStr,
                partitionGenFunc,
                Seq(),
                Seq(1, "a1", "1.6", 1704121827, "cat1"),
                Seq(2, "a2", "10.8", 1704121827, "cat1"),
                Seq(3, "a3", "30.0", 1706800227, "cat1"),
                Seq(4, "a4", "113.4", 1701443427, "cat2"),
                Seq(5, "a5", "2009.0", 1704121827, "cat2"),
                Seq(6, "a6", "90.0", 1704121827, "cat3"),
                Seq(7, "a7", "1409.0", 1706800227, "cat1"),
                Seq(8, "a8", "26.9", 1706800227, "cat3"),
                Seq(9, "a9", "299.0", 1701443427, "cat4")
              )

              // Test SQL MERGE INTO
              spark.sql(
                s"""
                   | MERGE INTO $tableName as target
                   | USING (
                   |   SELECT 1 as id, 'a1' as name, 1.6 as price, 1704121827 as ts, 'cat1' as segment, 'delete' as flag
                   |   UNION
                   |   SELECT 2 as id, 'a2' as name, 11.9 as price, 1704121827 as ts, 'cat1' as segment, '' as flag
                   |   UNION
                   |   SELECT 6 as id, 'a6' as name, 99.0 as price, 1704121827 as ts, 'cat3' as segment, '' as flag
                   |   UNION
                   |   SELECT 8 as id, 'a8' as name, 24.9 as price, 1706800227 as ts, 'cat3' as segment, '' as flag
                   |   UNION
                   |   SELECT 10 as id, 'a10' as name, 888.8 as price, 1706800227 as ts, 'cat5' as segment, '' as flag
                   | ) source
                   | on target.id = source.id
                   | WHEN MATCHED AND flag != 'delete' THEN UPDATE SET
                   |   id = source.id, name = source.name, price = source.price, ts = source.ts, segment = source.segment
                   | WHEN MATCHED AND flag = 'delete' THEN DELETE
                   | WHEN NOT MATCHED THEN INSERT (id, name, price, ts, segment)
                   |   values (source.id, source.name, source.price, source.ts, source.segment)
                   | """.stripMargin)
              validateResults(
                tableName,
                sqlStr,
                partitionGenFunc,
                Seq(),
                Seq(2, "a2", "11.9", 1704121827, "cat1"),
                Seq(3, "a3", "30.0", 1706800227, "cat1"),
                Seq(4, "a4", "113.4", 1701443427, "cat2"),
                Seq(5, "a5", "2009.0", 1704121827, "cat2"),
                Seq(6, "a6", "99.0", 1704121827, "cat3"),
                Seq(7, "a7", "1409.0", 1706800227, "cat1"),
                Seq(8, "a8", "24.9", 1706800227, "cat3"),
                Seq(9, "a9", "299.0", 1701443427, "cat4"),
                Seq(10, "a10", "888.8", 1706800227, "cat5")
              )

              // Test SQL DELETE
              spark.sql(
                s"""
                   | DELETE FROM $tableName
                   | WHERE id = 7
                   | """.stripMargin)
              validateResults(
                tableName,
                sqlStr,
                partitionGenFunc,
                Seq(),
                Seq(2, "a2", "11.9", 1704121827, "cat1"),
                Seq(3, "a3", "30.0", 1706800227, "cat1"),
                Seq(4, "a4", "113.4", 1701443427, "cat2"),
                Seq(5, "a5", "2009.0", 1704121827, "cat2"),
                Seq(6, "a6", "99.0", 1704121827, "cat3"),
                Seq(8, "a8", "24.9", 1706800227, "cat3"),
                Seq(9, "a9", "299.0", 1701443427, "cat4"),
                Seq(10, "a10", "888.8", 1706800227, "cat5")
              )

              // Test DROP PARTITION
              assertTrue(getSortedTablePartitions(tableName).contains(droppedPartition))
              spark.sql(
                s"""
                   | ALTER TABLE $tableName DROP PARTITION $dropPartitionStatement
                   |""".stripMargin)
              validatePartitions(tableName, Seq(droppedPartition), expectedPartitions)

              spark.sql(
                s"""
                   | INSERT OVERWRITE $tableName
                   | SELECT 100 as id, 'a100' as name, 299.0 as price, 1706800227 as ts, 'cat10' as segment
                   | """.stripMargin)
              validateResults(
                tableName,
                sqlStr,
                partitionGenFunc,
                Seq(),
                Seq(100, "a100", "299.0", 1706800227, "cat10")
              )

              // Validate ts field is still of type int in the table
              validateTsFieldSchema(tablePath, "ts", Schema.Type.INT)
              if (useOlderPartitionFieldFormat) {
                val metaClient = createMetaClient(spark, tablePath)
                assertEquals(metaClient.getTableConfig.getPartitionFieldProp, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).orElse(""))
              }
            }
          }
        }
      }
    }
  }

  test("Test table property isolation for partition path field config with custom key generator") {
    for (extractPartition <- Seq(false)) {
      withSQLConf(EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key() -> extractPartition.toString) {
        withTempDir { tmp => {
          val tableNameNonPartitioned = generateTableName
          val tableNameSimpleKey = generateTableName
          val tableNameCustom1 = generateTableName
          val tableNameCustom2 = generateTableName

          val tablePathNonPartitioned = tmp.getCanonicalPath + "/" + tableNameNonPartitioned
          val tablePathSimpleKey = tmp.getCanonicalPath + "/" + tableNameSimpleKey
          val tablePathCustom1 = tmp.getCanonicalPath + "/" + tableNameCustom1
          val tablePathCustom2 = tmp.getCanonicalPath + "/" + tableNameCustom2

          val tableType = "MERGE_ON_READ"
          val writePartitionFields1 = "segment:simple"
          val writePartitionFields2 = "ts:timestamp,segment:simple"

          prepareTableWithKeyGenerator(
            tableNameNonPartitioned, tablePathNonPartitioned, tableType,
            NONPARTITIONED_KEY_GEN_CLASS_NAME, "", Map())
          prepareTableWithKeyGenerator(
            tableNameSimpleKey, tablePathSimpleKey, tableType,
            SIMPLE_KEY_GEN_CLASS_NAME, "segment", Map())
          prepareTableWithKeyGenerator(
            tableNameCustom1, tablePathCustom1, tableType,
            CUSTOM_KEY_GEN_CLASS_NAME, writePartitionFields1, Map())
          prepareTableWithKeyGenerator(
            tableNameCustom2, tablePathCustom2, tableType,
            CUSTOM_KEY_GEN_CLASS_NAME, writePartitionFields2, TS_KEY_GEN_CONFIGS)

          // Non-partitioned table does not require additional partition path field write config
          createTableWithSql(tableNameNonPartitioned, tablePathNonPartitioned, "")
          // Partitioned table with simple key generator does not require additional partition path field write config
          createTableWithSql(tableNameSimpleKey, tablePathSimpleKey, "")
          // Partitioned table with custom key generator requires additional partition path field write config
          // Without that, right now the SQL DML fails
          createTableWithSql(tableNameCustom1, tablePathCustom1, "")
          createTableWithSql(tableNameCustom2, tablePathCustom2,
            s"hoodie.datasource.write.partitionpath.field = '$writePartitionFields2', "
              + TS_KEY_GEN_CONFIGS.map(e => e._1 + " = '" + e._2 + "'").mkString(", "))

          val segmentPartitionFunc = (_: Integer, segment: String) => segment
          val customPartitionFunc = (ts: Integer, segment: String) => TS_FORMATTER_FUNC.apply(ts) + "/" + segment

          testFirstRoundInserts(tableNameNonPartitioned, (_, _) => "")
          testFirstRoundInserts(tableNameSimpleKey, segmentPartitionFunc)
          testFirstRoundInserts(tableNameCustom2, customPartitionFunc)

          // INSERT INTO should succeed for tableNameCustom1 even if write partition path field config is not set
          // It should pick up the partition fields from table config
          val sourceTableName = tableNameCustom1 + "_source"
          prepareParquetSource(sourceTableName, Seq("(7, 'a7', 1399.0, 1706800227, 'cat1')"))
          testFirstRoundInserts(tableNameCustom1, segmentPartitionFunc)

          // All tables should be able to do INSERT INTO without any problem,
          // since the scope of the added write config is at the catalog table level
          testSecondRoundInserts(tableNameNonPartitioned, (_, _) => "")
          testSecondRoundInserts(tableNameSimpleKey, segmentPartitionFunc)
          testSecondRoundInserts(tableNameCustom2, customPartitionFunc)

          // Validate ts field is still of type int in the table
          validateTsFieldSchema(tablePathCustom1, "ts", Schema.Type.INT)
          validateTsFieldSchema(tablePathCustom2, "ts", Schema.Type.INT)
        }
        }
      }
    }
  }

  test("Test wrong partition path field write config with custom key generator") {
    for (extractPartition <- Seq(false)) {
      withSQLConf(EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key() -> extractPartition.toString) {
        withTempDir { tmp => {
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath + "/" + tableName
          val tableType = "MERGE_ON_READ"
          val writePartitionFields = "segment:simple,ts:timestamp"
          val wrongWritePartitionFields = "segment:simple"
          val customPartitionFunc = (ts: Integer, segment: String) => segment + "/" + TS_FORMATTER_FUNC.apply(ts)

          prepareTableWithKeyGenerator(
            tableName, tablePath, "MERGE_ON_READ",
            CUSTOM_KEY_GEN_CLASS_NAME, writePartitionFields, TS_KEY_GEN_CONFIGS)

          // CREATE TABLE should fail due to config conflict
          assertThrows[HoodieException] {
            createTableWithSql(tableName, tablePath,
              s"hoodie.datasource.write.partitionpath.field = '$wrongWritePartitionFields', "
                + TS_KEY_GEN_CONFIGS.map(e => e._1 + " = '" + e._2 + "'").mkString(", "))
          }

          createTableWithSql(tableName, tablePath,
            s"hoodie.datasource.write.partitionpath.field = '$writePartitionFields', "
              + TS_KEY_GEN_CONFIGS.map(e => e._1 + " = '" + e._2 + "'").mkString(", "))
          // Set wrong write config
          spark.sql(
            s"""ALTER TABLE $tableName
               | SET TBLPROPERTIES (hoodie.datasource.write.partitionpath.field = '$wrongWritePartitionFields')
               | """.stripMargin)

          // INSERT INTO should fail due to conflict between write and table config of partition path fields
          val sourceTableName = tableName + "_source"
          prepareParquetSource(sourceTableName, Seq("(7, 'a7', 1399.0, 1706800227, 'cat1')"))
          assertThrows[HoodieException] {
            spark.sql(
              s"""
                 | INSERT INTO $tableName
                 | SELECT * from $sourceTableName
                 | """.stripMargin)
          }

          // Now fix the partition path field write config for tableName
          spark.sql(
            s"""ALTER TABLE $tableName
               | SET TBLPROPERTIES (hoodie.datasource.write.partitionpath.field = '$writePartitionFields')
               | """.stripMargin)

          // INSERT INTO should succeed now
          testFirstRoundInserts(tableName, customPartitionFunc)

          // Validate ts field is still of type int in the table
          validateTsFieldSchema(tablePath, "ts", Schema.Type.INT)
        }
        }
      }
    }
  }

  test("Test query with custom key generator") {
    for (extractPartition <- Seq(false)) {
      withSQLConf(EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key() -> extractPartition.toString) {
        withTempDir { tmp => {
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath + "/" + tableName
          val writePartitionFields = "ts:timestamp"
          val dateFormat = "yyyy/MM/dd"
          val tsGenFunc = (ts: Integer) => TS_FORMATTER_FUNC_WITH_FORMAT.apply(ts, dateFormat)
          val customPartitionFunc = (ts: Integer, _: String) => tsGenFunc.apply(ts)
          val keyGenConfigs = TS_KEY_GEN_CONFIGS + ("hoodie.keygen.timebased.output.dateformat" -> dateFormat)

          prepareTableWithKeyGenerator(
            tableName, tablePath, "MERGE_ON_READ",
            CUSTOM_KEY_GEN_CLASS_NAME, writePartitionFields, keyGenConfigs)

          createTableWithSql(tableName, tablePath,
            s"hoodie.datasource.write.partitionpath.field = '$writePartitionFields', "
              + keyGenConfigs.map(e => e._1 + " = '" + e._2 + "'").mkString(", "))
          testFirstRoundInserts(tableName, customPartitionFunc)
          assertEquals(7, spark.sql(
            s"""
               | SELECT * from $tableName
               | """.stripMargin).count())
          val incrementalDF = spark.read.format("hudi").
            option("hoodie.datasource.query.type", "incremental").
            option("hoodie.datasource.read.begin.instanttime", 0).
            load(tablePath)
          incrementalDF.createOrReplaceTempView("tbl_incremental")
          assertEquals(7, spark.sql(
            s"""
               | SELECT * from tbl_incremental
               | """.stripMargin).count())

          // Validate ts field is still of type int in the table
          validateTsFieldSchema(tablePath, "ts", Schema.Type.INT)
        }
        }
      }
    }
  }

  test("Test query with custom key generator without partition path field config") {
    for (extractPartition <- Seq(false)) {
      withSQLConf(EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key() -> extractPartition.toString) {
        withTempDir { tmp => {
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath + "/" + tableName
          val writePartitionFields = "ts:timestamp"
          val dateFormat = "yyyy/MM/dd"
          val tsGenFunc = (ts: Integer) => TS_FORMATTER_FUNC_WITH_FORMAT.apply(ts, dateFormat)
          val customPartitionFunc = (ts: Integer, _: String) => tsGenFunc.apply(ts)
          val keyGenConfigs = TS_KEY_GEN_CONFIGS + ("hoodie.keygen.timebased.output.dateformat" -> dateFormat)

          prepareTableWithKeyGenerator(
            tableName, tablePath, "MERGE_ON_READ",
            CUSTOM_KEY_GEN_CLASS_NAME, writePartitionFields, keyGenConfigs)

          // We are not specifying config hoodie.datasource.write.partitionpath.field while creating
          // table
          createTableWithSql(tableName, tablePath,
            keyGenConfigs.map(e => e._1 + " = '" + e._2 + "'").mkString(", "))
          testFirstRoundInserts(tableName, customPartitionFunc)
          assertEquals(7, spark.sql(
            s"""
               | SELECT * from $tableName
               | """.stripMargin).count())
          val incrementalDF = spark.read.format("hudi").
            option("hoodie.datasource.query.type", "incremental").
            option("hoodie.datasource.read.begin.instanttime", 0).
            load(tablePath)
          incrementalDF.createOrReplaceTempView("tbl_incremental")
          assertEquals(7, spark.sql(
            s"""
               | SELECT * from tbl_incremental
               | """.stripMargin).count())

          // Validate ts field is still of type int in the table
          validateTsFieldSchema(tablePath, "ts", Schema.Type.INT)
        }
        }
      }
    }
  }

  test("Test create table with custom key generator") {
    withTempDir { tmp => {
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val writePartitionFields = "ts:timestamp"
      val dateFormat = "yyyy/MM/dd"
      val tsGenFunc = (ts: Integer) => TS_FORMATTER_FUNC_WITH_FORMAT.apply(ts, dateFormat)
      val customPartitionFunc = (ts: Integer, _: String) => "ts=" + tsGenFunc.apply(ts)

      spark.sql(
        s"""
           |create table ${tableName} (
           |  `id` INT,
           |  `name` STRING,
           |  `price` DECIMAL(5, 1),
           |  `ts` INT,
           |  `segment` STRING
           |) using hudi
           |tblproperties (
           |  'primaryKey' = 'id,name',
           |  'type' = 'mor',
           |  'preCombineField'='name',
           |  'hoodie.datasource.write.keygenerator.class' = '$CUSTOM_KEY_GEN_CLASS_NAME',
           |  'hoodie.datasource.write.partitionpath.field' = '$writePartitionFields',
           |  'hoodie.insert.shuffle.parallelism' = '1',
           |  'hoodie.upsert.shuffle.parallelism' = '1',
           |  'hoodie.bulkinsert.shuffle.parallelism' = '1',
           |  'hoodie.keygen.timebased.timestamp.type' = 'SCALAR',
           |  'hoodie.keygen.timebased.output.dateformat' = '$dateFormat',
           |  'hoodie.keygen.timebased.timestamp.scalar.time.unit' = 'seconds'
           |)
           location '${tablePath}'
           """.stripMargin)

      testInserts(tableName, tsGenFunc, customPartitionFunc)

      // Validate ts field is still of type int in the table
      validateTsFieldSchema(tablePath, "ts", Schema.Type.INT)

      val metaClient = createMetaClient(spark, tablePath)
      assertEquals(KeyGeneratorType.CUSTOM.getClassName, metaClient.getTableConfig.getKeyGeneratorClassName)
    }
    }
  }

  private def validateTsFieldSchema(tablePath: String, fieldName: String, expectedType: Schema.Type): Unit = {
    val metaClient = createMetaClient(spark, tablePath)
    val schemaResolver = new TableSchemaResolver(metaClient)
    val nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(expectedType))
    assertEquals(nullableSchema, schemaResolver.getTableAvroSchema(true).getField(fieldName).schema())
  }

  private def testInserts(tableName: String,
                          tsGenFunc: Integer => String,
                          partitionGenFunc: (Integer, String) => String): Unit = {
    val sourceTableName = tableName + "_source1"
    prepareParquetSource(sourceTableName, Seq(
      "(1, 'a1', 1.6, 1704121827, 'cat1')",
      "(2, 'a2', 10.8, 1704121827, 'cat1')",
      "(3, 'a3', 30.0, 1706800227, 'cat1')",
      "(4, 'a4', 103.4, 1701443427, 'cat2')",
      "(5, 'a5', 1999.0, 1704121827, 'cat2')",
      "(6, 'a6', 80.0, 1704121827, 'cat3')",
      "(7, 'a7', 1399.0, 1706800227, 'cat1')"))
    spark.sql(
      s"""
         | INSERT INTO $tableName
         | SELECT * from $sourceTableName
         | """.stripMargin)
    validateResults(
      tableName,
      s"SELECT id, name, cast(price as string), ts, segment from $tableName",
      partitionGenFunc,
      Seq(),
      Seq(1, "a1", "1.6", 1704121827, "cat1"),
      Seq(2, "a2", "10.8", 1704121827, "cat1"),
      Seq(3, "a3", "30.0", 1706800227, "cat1"),
      Seq(4, "a4", "103.4", 1701443427, "cat2"),
      Seq(5, "a5", "1999.0", 1704121827, "cat2"),
      Seq(6, "a6", "80.0", 1704121827, "cat3"),
      Seq(7, "a7", "1399.0", 1706800227, "cat1")
    )
  }

  private def testFirstRoundInserts(tableName: String,
                                    partitionGenFunc: (Integer, String) => String): Unit = {
    val sourceTableName = tableName + "_source1"
    prepareParquetSource(sourceTableName, Seq("(7, 'a7', 1399.0, 1706800227, 'cat1')"))
    spark.sql(
      s"""
         | INSERT INTO $tableName
         | SELECT * from $sourceTableName
         | """.stripMargin)
    val sqlStr = s"SELECT id, name, cast(price as string), ts, segment from $tableName"
    validateResults(
      tableName,
      sqlStr,
      partitionGenFunc,
      Seq(),
      Seq(1, "a1", "1.6", 1704121827, "cat1"),
      Seq(2, "a2", "10.8", 1704121827, "cat1"),
      Seq(3, "a3", "30.0", 1706800227, "cat1"),
      Seq(4, "a4", "103.4", 1701443427, "cat2"),
      Seq(5, "a5", "1999.0", 1704121827, "cat2"),
      Seq(6, "a6", "80.0", 1704121827, "cat3"),
      Seq(7, "a7", "1399.0", 1706800227, "cat1")
    )
  }

  private def testSecondRoundInserts(tableName: String,
                                     partitionGenFunc: (Integer, String) => String): Unit = {
    val sourceTableName = tableName + "_source2"
    prepareParquetSource(sourceTableName, Seq("(8, 'a8', 26.9, 1706800227, 'cat3')"))
    spark.sql(
      s"""
         | INSERT INTO $tableName
         | SELECT * from $sourceTableName
         | """.stripMargin)
    val sqlStr = s"SELECT id, name, cast(price as string), ts, segment from $tableName"
    validateResults(
      tableName,
      sqlStr,
      partitionGenFunc,
      Seq(),
      Seq(1, "a1", "1.6", 1704121827, "cat1"),
      Seq(2, "a2", "10.8", 1704121827, "cat1"),
      Seq(3, "a3", "30.0", 1706800227, "cat1"),
      Seq(4, "a4", "103.4", 1701443427, "cat2"),
      Seq(5, "a5", "1999.0", 1704121827, "cat2"),
      Seq(6, "a6", "80.0", 1704121827, "cat3"),
      Seq(7, "a7", "1399.0", 1706800227, "cat1"),
      Seq(8, "a8", "26.9", 1706800227, "cat3")
    )
  }

  private def prepareTableWithKeyGenerator(tableName: String,
                                           tablePath: String,
                                           tableType: String,
                                           keyGenClassName: String,
                                           writePartitionFields: String,
                                           timestampKeyGeneratorConfig: Map[String, String]): Unit = {
    val df = spark.sql(
      s"""SELECT 1 as id, 'a1' as name, 1.6 as price, 1704121827 as ts, 'cat1' as segment
         | UNION
         | SELECT 2 as id, 'a2' as name, 10.8 as price, 1704121827 as ts, 'cat1' as segment
         | UNION
         | SELECT 3 as id, 'a3' as name, 30.0 as price, 1706800227 as ts, 'cat1' as segment
         | UNION
         | SELECT 4 as id, 'a4' as name, 103.4 as price, 1701443427 as ts, 'cat2' as segment
         | UNION
         | SELECT 5 as id, 'a5' as name, 1999.0 as price, 1704121827 as ts, 'cat2' as segment
         | UNION
         | SELECT 6 as id, 'a6' as name, 80.0 as price, 1704121827 as ts, 'cat3' as segment
         |""".stripMargin)

    df.write.format("hudi")
      .option("hoodie.datasource.write.table.type", tableType)
      .option("hoodie.datasource.write.keygenerator.class", keyGenClassName)
      .option("hoodie.datasource.write.partitionpath.field", writePartitionFields)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option(HoodieTableConfig.ORDERING_FIELDS.key(), "name")
      .option("hoodie.table.name", tableName)
      .option("hoodie.insert.shuffle.parallelism", "1")
      .option("hoodie.upsert.shuffle.parallelism", "1")
      .option("hoodie.bulkinsert.shuffle.parallelism", "1")
      .options(timestampKeyGeneratorConfig)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Validate that the generated table has expected table configs of key generator and partition path fields
    val metaClient = createMetaClient(spark, tablePath)
    assertEquals(keyGenClassName, metaClient.getTableConfig.getKeyGeneratorClassName)
    // Validate that that partition path fields in the table config should always
    // contain the field names only (no key generator type like "segment:simple")
    if (CUSTOM_KEY_GEN_CLASS_NAME.equals(keyGenClassName)) {
      val props = new TypedProperties()
      props.put("hoodie.datasource.write.partitionpath.field", writePartitionFields)
      timestampKeyGeneratorConfig.foreach(e => {
        props.put(e._1, e._2)
      })
      // For custom key generator, the "hoodie.datasource.write.partitionpath.field"
      // contains the key generator type, like "ts:timestamp,segment:simple",
      // whereas the partition path fields in table config is "ts,segment"
      assertEquals(
        SparkKeyGenUtils.getPartitionColumns(Option(CUSTOM_KEY_GEN_CLASS_NAME), props),
        metaClient.getTableConfig.getPartitionFieldProp)
    } else {
      assertEquals(writePartitionFields, metaClient.getTableConfig.getPartitionFieldProp)
    }
  }

  private def createTableWithSql(tableName: String,
                                 tablePath: String,
                                 tblProps: String): Unit = {
    val tblPropsStatement = if (StringUtils.isNullOrEmpty(tblProps)) {
      ""
    } else {
      "TBLPROPERTIES (\n" + tblProps + "\n)"
    }
    spark.sql(
      s"""
         | CREATE TABLE $tableName USING HUDI
         | location '$tablePath'
         | $tblPropsStatement
         | """.stripMargin)
  }

  private def prepareParquetSource(sourceTableName: String,
                                   rows: Seq[String]): Unit = {
    spark.sql(
      s"""CREATE TABLE $sourceTableName
         | (id int, name string, price decimal(5, 1), ts int, segment string)
         | USING PARQUET
         |""".stripMargin)
    spark.sql(
      s"""
         | INSERT INTO $sourceTableName values
         | ${rows.mkString(", ")}
         | """.stripMargin)
  }

  private def validateResults(tableName: String,
                              sql: String,
                              partitionGenFunc: (Integer, String) => String,
                              droppedPartitions: Seq[String],
                              expects: Seq[Any]*): Unit = {
    checkAnswer(sql)(
      expects.map(e => Seq(e(0), e(1), e(2), e(3), e(4))): _*
    )
    val expectedPartitions: Seq[String] = expects
      .map(e => partitionGenFunc.apply(e(3).asInstanceOf[Integer], e(4).asInstanceOf[String]))
      .distinct.sorted
    validatePartitions(tableName, droppedPartitions, expectedPartitions)
  }

  private def getSortedTablePartitions(tableName: String): Seq[String] = {
    spark.sql(s"SHOW PARTITIONS $tableName").collect()
      .map(row => row.getString(0))
      .sorted.toSeq
  }

  private def validatePartitions(tableName: String,
                                 droppedPartitions: Seq[String],
                                 expectedPartitions: Seq[String]): Unit = {
    val actualPartitions: Seq[String] = getSortedTablePartitions(tableName)
    if (expectedPartitions.size == 1 && expectedPartitions.head.isEmpty) {
      assertTrue(actualPartitions.isEmpty)
    } else {
      assertEquals(expectedPartitions, actualPartitions)
    }
    droppedPartitions.foreach(dropped => assertFalse(actualPartitions.contains(dropped)))
  }
}

object TestSparkSqlWithCustomKeyGenerator {
  val SIMPLE_KEY_GEN_CLASS_NAME = "org.apache.hudi.keygen.SimpleKeyGenerator"
  val NONPARTITIONED_KEY_GEN_CLASS_NAME = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
  val CUSTOM_KEY_GEN_CLASS_NAME = "org.apache.hudi.keygen.CustomKeyGenerator"
  val DATE_FORMAT_PATTERN = "yyyyMM"
  val TS_KEY_GEN_CONFIGS = Map(
    "hoodie.keygen.timebased.timestamp.type" -> "SCALAR",
    "hoodie.keygen.timebased.output.dateformat" -> DATE_FORMAT_PATTERN,
    "hoodie.keygen.timebased.timestamp.scalar.time.unit" -> "seconds"
  )
  val TS_TO_STRING_FUNC = (tsSeconds: Integer) => tsSeconds.toString
  val TS_FORMATTER_FUNC = (tsSeconds: Integer) => {
    new DateTime(tsSeconds * 1000L).toString(DateTimeFormat.forPattern(DATE_FORMAT_PATTERN))
  }
  val TS_FORMATTER_FUNC_WITH_FORMAT = (tsSeconds: Integer, dateFormat: String) => {
    new DateTime(tsSeconds * 1000L).toString(DateTimeFormat.forPattern(dateFormat))
  }

  def getTimestampKeyGenConfigs: Map[String, String] = {
    Map(
      "hoodie.keygen.timebased.timestamp.type" -> "SCALAR",
      "hoodie.keygen.timebased.output.dateformat" -> DATE_FORMAT_PATTERN,
      "hoodie.keygen.timebased.timestamp.scalar.time.unit" -> "seconds"
    )
  }
}

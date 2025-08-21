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

package org.apache.hudi.functional

import org.apache.hudi.exception.HoodieException
import org.apache.hudi.functional.TestSparkSqlWithTimestampKeyGenerator._

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.slf4j.LoggerFactory

/**
 * Tests of timestamp key generator using Spark SQL
 */
class TestSparkSqlWithTimestampKeyGenerator extends HoodieSparkSqlTestBase {
  private val LOG = LoggerFactory.getLogger(getClass)

  test("Test Spark SQL with timestamp key generator") {
    withTempDir { tmp =>
      Seq(
        Seq("COPY_ON_WRITE", "true"),
        Seq("COPY_ON_WRITE", "false"),
        Seq("MERGE_ON_READ", "true"),
        Seq("MERGE_ON_READ", "false")
      ).foreach { testParams =>
        val tableType = testParams(0)
        // enables use of engine agnostic file group reader
        val shouldUseFileGroupReader = testParams(1)

        timestampKeyGeneratorSettings.foreach { keyGeneratorSettings =>
          withTable(generateTableName) { tableName =>
            // Warning level is used due to CI run with warn-log profile for quick failed cases identification
            LOG.warn(s"Table '$tableName' with parameters: $testParams. Timestamp key generator settings: $keyGeneratorSettings")
            val tablePath = tmp.getCanonicalPath + "/" + tableName
            val tsType = if (keyGeneratorSettings.contains("DATE_STRING")) "string" else "long"
            spark.sql(
              s"""
                 | CREATE TABLE $tableName (
                 |   id int,
                 |   name string,
                 |   precomb long,
                 |   ts $tsType
                 | ) USING HUDI
                 | PARTITIONED BY (ts)
                 | LOCATION '$tablePath'
                 | TBLPROPERTIES (
                 |   type = '$tableType',
                 |   primaryKey = 'id',
                 |   orderingFields = 'precomb',
                 |   hoodie.datasource.write.partitionpath.field = 'ts',
                 |   hoodie.datasource.write.hive_style_partitioning = 'false',
                 |   hoodie.file.group.reader.enabled = '$shouldUseFileGroupReader',
                 |   hoodie.table.keygenerator.class = 'org.apache.hudi.keygen.TimestampBasedKeyGenerator',
                 |   $keyGeneratorSettings
                 | )
                 |""".stripMargin)
            // TODO: couldn't set `TIMESTAMP` for `hoodie.table.keygenerator.type`, it's overwritten by `SIMPLE`, only `hoodie.table.keygenerator.class` works

            val (dataBatches, expectedQueryResult) = if (keyGeneratorSettings.contains("DATE_STRING"))
              (dataBatchesWithString, queryResultWithString)
            else if (keyGeneratorSettings.contains("EPOCHMILLISECONDS"))
              (dataBatchesWithLongOfMilliseconds, queryResultWithLongOfMilliseconds)
            else // UNIX_TIMESTAMP, and SCALAR with SECONDS
              (dataBatchesWithLongOfSeconds, queryResultWithLongOfSeconds)

            withSQLConf("hoodie.file.group.reader.enabled" -> s"$shouldUseFileGroupReader",
              "hoodie.datasource.query.type" -> "snapshot") {
              // two partitions, one contains parquet file only, the second one contains parquet and log files for MOR, and two parquets for COW
              spark.sql(s"INSERT INTO $tableName VALUES ${dataBatches(0)}")
              spark.sql(s"INSERT INTO $tableName VALUES ${dataBatches(1)}")

              val queryResult = spark.sql(s"SELECT id, name, precomb, ts FROM $tableName ORDER BY id").collect().mkString("; ")
              LOG.warn(s"Query result: $queryResult")
              // TODO: use `shouldExtractPartitionValuesFromPartitionPath` uniformly, and get `expectedQueryResult` for all cases instead of `expectedQueryResultWithLossyString` for some cases
              //   After it we could properly process filters like "WHERE ts BETWEEN 1078016000 and 1718953003" and add tests with partition pruning.
              //   COW: Fix for [HUDI-3896] overwrites `shouldExtractPartitionValuesFromPartitionPath` in `BaseFileOnlyRelation`, therefore for COW we extracting from partition paths and get nulls
              //   shouldUseFileGroupReader: [HUDI-7925] Currently there is no logic for `shouldExtractPartitionValuesFromPartitionPath` in `HoodieBaseHadoopFsRelationFactory`
              //   UPDATE: with [HUDI-5807] we now have fg reader support. However partition pruning is still not fixed.
              if (tableType == "COPY_ON_WRITE" && !shouldUseFileGroupReader.toBoolean)
                assertResult(expectedQueryResultWithLossyString)(queryResult)
              else
                assertResult(expectedQueryResult)(queryResult)
            }
          }
        }
      }
    }
  }
  test("Test Spark SQL with default partition path for timestamp key generator") {
    withTempDir { tmp =>
      val keyGeneratorSettings = timestampKeyGeneratorSettings(3)
      val tsType = if (keyGeneratorSettings.contains("DATE_STRING")) "string" else "long"
      spark.sql(
        s"""
           | CREATE TABLE test_default_path_ts (
           |   id int,
           |   name string,
           |   precomb long,
           |   ts TIMESTAMP
           | ) USING HUDI
           | LOCATION '${tmp.getCanonicalPath + "/test_default_path_ts"}'
           | PARTITIONED BY (ts)
           | TBLPROPERTIES (
           |   type = 'COPY_ON_WRITE',
           |   primaryKey = 'id',
           |   orderingFields = 'precomb'
           | )
           |""".stripMargin)
      val dataBatches =   Array(
        "(1, 'a1', 1,TIMESTAMP '2025-01-15 01:02:03')",
        "(2, 'a3', 1, null)"
      )
      val expectedQueryResult: String = "[1,a1,1,2025-01-15 01:02:03.0]; [2,a3,1,null]"
      spark.sql(s"INSERT INTO test_default_path_ts VALUES ${dataBatches(0)}")
      // inserting value with partition_timestamp value as null
      spark.sql(s"INSERT INTO test_default_path_ts VALUES ${dataBatches(1)}")

      val queryResult = spark.sql(s"SELECT id, name, precomb, ts FROM test_default_path_ts ORDER BY id").collect().mkString("; ")
      LOG.warn(s"Query result: $queryResult")
      assertResult(expectedQueryResult)(queryResult)

    }
  }

  test("Test mandatory partitioning for timestamp key generator") {
    withTempDir { tmp =>
      spark.sql(
        s"""
           | CREATE TABLE should_fail (
           |   id int,
           |   name string,
           |   precomb long,
           |   ts long
           | ) USING HUDI
           | LOCATION '${tmp.getCanonicalPath + "/should_fail"}'
           | TBLPROPERTIES (
           |   type = 'COPY_ON_WRITE',
           |   primaryKey = 'id',
           |   orderingFields = 'precomb',
           |   hoodie.table.keygenerator.class = 'org.apache.hudi.keygen.TimestampBasedKeyGenerator',
           |   ${timestampKeyGeneratorSettings.head}
           | )
           |""".stripMargin)
      // should fail due to absent partitioning
      assertThrows[HoodieException] {
        spark.sql(s"INSERT INTO should_fail VALUES ${dataBatchesWithLongOfSeconds(0)}")
      }

    }
  }
}

object TestSparkSqlWithTimestampKeyGenerator {
  val outputDateformat = "yyyy-MM-dd HH"
  val timestampKeyGeneratorSettings: Array[String] = Array(
    s"""
       |   hoodie.keygen.timebased.timestamp.type = 'UNIX_TIMESTAMP',
       |   hoodie.keygen.timebased.output.dateformat = '$outputDateformat'""",
    s"""
       |   hoodie.keygen.timebased.timestamp.type = 'EPOCHMILLISECONDS',
       |   hoodie.keygen.timebased.output.dateformat = '$outputDateformat'""",
    s"""
       |   hoodie.keygen.timebased.timestamp.type = 'SCALAR',
       |   hoodie.keygen.timebased.timestamp.scalar.time.unit = 'SECONDS',
       |   hoodie.keygen.timebased.output.dateformat = '$outputDateformat'""",
    s"""
       |   hoodie.keygen.timebased.timestamp.type = 'DATE_STRING',
       |   hoodie.keygen.timebased.input.dateformat = 'yyyy-MM-dd HH:mm:ss',
       |   hoodie.keygen.timebased.output.dateformat = '$outputDateformat'"""
  )

  // All data batches should correspond to 2004-02-29 01:02:03 and 2024-06-21 06:50:03
  val dataBatchesWithLongOfSeconds: Array[String] = Array(
    "(1, 'a1', 1, 1078016523), (2, 'a2', 1, 1718952603)",
    "(2, 'a3', 1, 1718952603)"
  )
  val dataBatchesWithLongOfMilliseconds: Array[String] = Array(
    "(1, 'a1', 1, 1078016523000), (2, 'a2', 1, 1718952603000)",
    "(2, 'a3', 1, 1718952603000)"
  )
  val dataBatchesWithString: Array[String] = Array(
    "(1, 'a1', 1, '2004-02-29 01:02:03'), (2, 'a2', 1, '2024-06-21 06:50:03')",
    "(2, 'a3', 1, '2024-06-21 06:50:03')"
  )
  val queryResultWithLongOfSeconds: String = "[1,a1,1,1078016523]; [2,a3,1,1718952603]"
  val queryResultWithLongOfMilliseconds: String = "[1,a1,1,1078016523000]; [2,a3,1,1718952603000]"
  val queryResultWithString: String = "[1,a1,1,2004-02-29 01:02:03]; [2,a3,1,2024-06-21 06:50:03]"
  val expectedQueryResultWithLossyString: String = "[1,a1,1,2004-02-29 01]; [2,a3,1,2024-06-21 06]"
}

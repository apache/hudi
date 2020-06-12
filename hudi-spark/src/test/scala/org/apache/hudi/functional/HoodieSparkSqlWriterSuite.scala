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

package org.apache.hudi.functional

import java.util.{Date, UUID}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hive.jdbc.HiveDriver
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkSqlWriter
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.testutils.minicluster.MiniClusterUtil
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hive.HoodieHiveSyncException
import org.apache.hudi.hive.testutils.HiveTestService
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class HoodieSparkSqlWriterSuite extends FunSuite with Matchers {

  test("Parameters With Write Defaults") {
    val originals = HoodieSparkSqlWriter.parametersWithWriteDefaults(Map.empty)
    val rhsKey = "hoodie.right.hand.side.key"
    val rhsVal = "hoodie.right.hand.side.val"
    val modifier = Map(OPERATION_OPT_KEY -> INSERT_OPERATION_OPT_VAL, TABLE_TYPE_OPT_KEY -> MOR_TABLE_TYPE_OPT_VAL, rhsKey -> rhsVal)
    val modified = HoodieSparkSqlWriter.parametersWithWriteDefaults(modifier)
    val matcher = (k: String, v: String) => modified(k) should be(v)

    originals foreach {
      case (OPERATION_OPT_KEY, _) => matcher(OPERATION_OPT_KEY, INSERT_OPERATION_OPT_VAL)
      case (TABLE_TYPE_OPT_KEY, _) => matcher(TABLE_TYPE_OPT_KEY, MOR_TABLE_TYPE_OPT_VAL)
      case (`rhsKey`, _) => matcher(rhsKey, rhsVal)
      case (k, v) => matcher(k, v)
    }
  }

  test("throw hoodie exception when invalid serializer") {
    val session = SparkSession.builder().appName("hoodie_test").master("local").getOrCreate()
    try {
      val sqlContext = session.sqlContext
      val options = Map("path" -> "hoodie/test/path", HoodieWriteConfig.TABLE_NAME -> "hoodie_test_tbl")
      val e = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.ErrorIfExists, options, session.emptyDataFrame))
      assert(e.getMessage.contains("spark.serializer"))
    } finally {
      session.stop()
    }
  }


  test("throw hoodie exception when there already exist a table with different name with Append Save mode") {

    val session = SparkSession.builder()
      .appName("test_append_mode")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val sqlContext = session.sqlContext
      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME -> hoodieFooTableName,
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4")
      val fooTableParams = HoodieSparkSqlWriter.parametersWithWriteDefaults(fooTableModifier)
      val dataFrame = session.createDataFrame(Seq(Test(UUID.randomUUID().toString, new Date().getTime)))
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, dataFrame)

      //on same path try append with different("hoodie_bar_tbl") table name which should throw an exception
      val barTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME -> "hoodie_bar_tbl",
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4")
      val barTableParams = HoodieSparkSqlWriter.parametersWithWriteDefaults(barTableModifier)
      val dataFrame2 = session.createDataFrame(Seq(Test(UUID.randomUUID().toString, new Date().getTime)))
      val tableAlreadyExistException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, barTableParams, dataFrame2))
      assert(tableAlreadyExistException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))

      //on same path try append with delete operation and different("hoodie_bar_tbl") table name which should throw an exception
      val deleteTableParams = barTableParams ++ Map(OPERATION_OPT_KEY -> "delete")
      val deleteCmdException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, deleteTableParams, dataFrame2))
      assert(deleteCmdException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))
    } finally {
      session.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  case class Test(uuid: String, ts: Long)

  test("Table can be created normally without partition keys, when sync non-parititioned table to hive") {
    // MiniCluster
    MiniClusterUtil.setUp()

    val hadoopConf = MiniClusterUtil.configuration

    // Hive
    val hiveTestService = new HiveTestService(hadoopConf)
    val hiveServer = hiveTestService.start()

    val session = SparkSession.builder()
      .appName("test_hoodie_trips_hive_non_partitioned")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {

      val sqlContext = session.sqlContext
      val tableName = "hoodie_trips_hive_non_partitioned"
      val path = hadoopConf.getRaw("fs.defaultFS") + "/hoodie_test_path"

      import session.implicits._
      val df = Seq(
        (100, "name_1", "2020-01-01 13:51:39", 1.32, "type1"),
        (101, "name_2", "2020-01-01 12:14:58", 2.57, "type2"),
        (102, "name_3", "2020-01-01 12:15:00", 3.45, "type1"),
        (103, "name_4", "2020-01-01 13:51:42", 6.78, "type2")
      )
        .toDF("id", "name", "ts", "value", "type")

      val tableOptions = Map("path" -> path,
        HoodieWriteConfig.TABLE_NAME -> tableName,
        RECORDKEY_FIELD_OPT_KEY -> "id",
        PRECOMBINE_FIELD_OPT_KEY -> "ts",
        PARTITIONPATH_FIELD_OPT_KEY -> "type",
        // enable Hive Sync
        HIVE_SYNC_ENABLED_OPT_KEY -> "true",
        HIVE_TABLE_OPT_KEY -> tableName,
        // HIVE_PARTITION_FIELDS_OPT_KEY should be ignored when sync non-parititioned table to hive
        HIVE_PARTITION_FIELDS_OPT_KEY -> "type",
        HIVE_URL_OPT_KEY -> "jdbc:hive2://127.0.0.1:9999",
        // Set partition value extractor to NonPartitionedExtractor
        HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> "org.apache.hudi.hive.NonPartitionedExtractor",
        KEYGENERATOR_CLASS_OPT_KEY -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
      )
      val tableParams = HoodieSparkSqlWriter.parametersWithWriteDefaults(tableOptions)

      // do the write and sync
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, tableParams, df)

      // validate
      val hiveMSClient = getHiveMetaStoreClient(hiveServer.getHiveConf)

      // table should exist and have no partitions
      assert(hiveMSClient.tableExists(tableParams(HIVE_DATABASE_OPT_KEY), tableName), s"Table $tableName should exist after sync complete")
      assert(hiveMSClient.listPartitions(tableParams(HIVE_DATABASE_OPT_KEY), tableName, -1).size() == 0,
        "Table should not have partitions because of NonPartitionedExtractor")

      // table create DDL should not contain 'PARTITIONED BY'
      assert(hiveMSClient.getTable(tableParams(HIVE_DATABASE_OPT_KEY), tableName).getPartitionKeys.size() == 0,
        "Table create DDL should not contain 'PARTITIONED BY'")

      // finally the total number of fields should be equal
      assert(hiveMSClient.getTable(tableParams(HIVE_DATABASE_OPT_KEY), tableName)
        .getSd().getCols().size() == (df.columns.length + HoodieRecord.HOODIE_META_COLUMNS.size()),
        "Table schema should contain all the fields")

    } finally {
      session.stop()
      hiveServer.stop()
      hiveTestService.stop()
      MiniClusterUtil.shutdown()
    }
  }

  def getHiveMetaStoreClient(hiveConf: HiveConf): IMetaStoreClient ={
    try {
      Class.forName(classOf[HiveDriver].getName)
      Hive.get(hiveConf).getMSC()
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalStateException("Could not find HiveDriver in classpath. ", e)
      case _ =>
        throw new HoodieHiveSyncException("Failed to create HiveMetaStoreClient")
    }
  }
}

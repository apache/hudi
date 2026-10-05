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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.{DefaultSparkRecordMerger, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.model.{HoodieAvroIndexedRecord, HoodieKey, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieIndex

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{FileSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import java.io.File
import java.sql.Timestamp

import scala.collection.JavaConverters._

/**
 * Spark caches the resolved relation of a table per session, so every statement on the table shares one
 * HoodieFileGroupReaderBasedFileFormat instance, and it asks that instance about batch support only for scans
 * narrower than spark.sql.codegen.maxFields. A wider scan therefore has to decide about vectorized reading for
 * its own schema when its reader is built, and no scan may change spark.sql.parquet.enableVectorizedReader,
 * which Spark reads to decide whether the scan output still needs an UnsafeRow conversion.
 */
class TestVectorizedReadDecisionPerScan extends HoodieSparkSqlTestBase with AdaptiveSparkPlanHelper {

  private val vectorizedReaderConf = "spark.sql.parquet.enableVectorizedReader"
  // Leaf fields of the payload struct; with them the table exceeds spark.sql.codegen.maxFields, so a
  // select * scan is planned row-based without a supportBatch call
  private val payloadFieldCount = 120

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test wide scan after narrow scan decides vectorized read per scan for $tableType table") {
      withSQLConf(vectorizedReaderConf -> "true", "spark.sql.codegen.maxFields" -> "100") {
        withTempDir { tmp =>
          val tableName = generateTableName
          createWideTable(tableName, tableType, tmp)

          // The narrow scan is asked supportBatch and may read vectorized
          checkAnswer(s"select count(*) from $tableName")(Seq(3))
          assertResult("true")(spark.conf.get(vectorizedReaderConf))

          // The wide scan is never asked, and must not inherit the narrow scan's answer
          assert(!scanIsBatched(s"select * from $tableName"))
          checkWideScan(tableName)
          assertResult("true")(spark.conf.get(vectorizedReaderConf))

          // With the vectorized reader disabled Spark skips the UnsafeRow conversion of the scan output, so a
          // wide scan handed a leftover vectorized reader fails with
          // ClassCastException: ColumnarBatchRow cannot be cast to UnsafeRow
          withSQLConf(vectorizedReaderConf -> "false") {
            checkWideScan(tableName)
            assertResult("false")(spark.conf.get(vectorizedReaderConf))
          }
        }
      }
    }

    test(s"Test wide scan keeps vectorized reads enabled for later scans for $tableType table") {
      withSQLConf(vectorizedReaderConf -> "true", "spark.sql.codegen.maxFields" -> "100") {
        withTempDir { tmp =>
          val tableName = generateTableName
          createWideTable(tableName, tableType, tmp)

          checkWideScan(tableName)
          assertResult("true")(spark.conf.get(vectorizedReaderConf))

          // Only COW scans return batches; MOR merges rows, so its scans are never columnar
          if (tableType == "cow") {
            assert(scanIsBatched(s"select count(*) from $tableName"))
          }
          checkAnswer(s"select count(*) from $tableName")(Seq(3))
          assertResult("true")(spark.conf.get(vectorizedReaderConf))
        }
      }
    }

    // Every scan now asks canReadVectorized, so a format it does not know fails every read, not only narrow ones
    test(s"Test narrow and wide scans of a Vortex table read row-based for $tableType table") {
      assume(SparkAdapterSupport.sparkAdapter.createVortexFileReader(vectorized = false, spark.sessionState.conf,
        Map.empty, new Configuration()).isDefined, "Vortex reads need a Spark 4.x adapter")
      // Vortex files are written from Spark records only
      withSQLConf(vectorizedReaderConf -> "true", "spark.sql.codegen.maxFields" -> "100",
        HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName) {
        withTempDir { tmp =>
          val tableName = generateTableName
          createWideTable(tableName, tableType, tmp, Map(HoodieTableConfig.BASE_FILE_FORMAT.key -> "VORTEX"))

          val narrowScan = s"select id, name from $tableName"
          assert(!scanIsBatched(narrowScan))
          checkAnswer(s"$narrowScan order by id")(Seq(1, "a1"), Seq(2, "a2"), Seq(3, "a3"))
          assert(!scanIsBatched(s"select * from $tableName"))
          checkWideScan(tableName)
          assertResult("true")(spark.conf.get(vectorizedReaderConf))
        }
      }
    }
  }

  // Before Spark 3.5, Hudi never reads a table with a timestamp-millis field vectorized (#14161), so these scans are
  // row-based while spark.sql.parquet.enableVectorizedReader stays true. Spark then converts the scan output to
  // UnsafeRow, which the row reader already produces, so the conversion only copies each row.
  Seq(HoodieTableType.COPY_ON_WRITE -> "cow", HoodieTableType.MERGE_ON_READ -> "mor").foreach { case (tableType, label) =>
    test(s"Test narrow and wide scans of a timestamp-millis table with the vectorized reader enabled for $label table") {
      withSQLConf(vectorizedReaderConf -> "true", "spark.sql.codegen.maxFields" -> "100") {
        withTempDir { tmp =>
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          writeTimestampMillisTable(tableName, basePath, tableType)
          // Read by path: without a Hive catalog, a catalog table hands Hudi its Spark schema, which has no
          // timestamp-millis type. One view keeps one file format instance for every scan below.
          spark.read.format("hudi").load(basePath).createOrReplaceTempView(tableName)
          val expected = timestampMillisRows.map { case (id, name, eventTime, payloadBase) =>
            (id, name, eventTime, payloadBase + 1, payloadBase + payloadFieldCount)
          }

          val narrowScan = s"select id, name, event_time from $tableName"
          // Only COW scans return batches, and only from Spark 3.5 on for this table
          assertResult(HoodieSparkUtils.gteqSpark3_5 && tableType == HoodieTableType.COPY_ON_WRITE)(scanIsBatched(narrowScan))
          val narrow = spark.sql(narrowScan).collect().map { row =>
            (row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Timestamp]("event_time").getTime)
          }.sorted.toSeq
          assertResult(expected.map(r => (r._1, r._2, r._3)))(narrow)
          assertResult("true")(spark.conf.get(vectorizedReaderConf))

          val wideScan = s"select * from $tableName"
          assert(WholeStageCodegenExec.isTooManyFields(spark.sessionState.conf, spark.table(tableName).schema),
            "select * must be too wide for whole-stage codegen, otherwise Spark asks supportBatch for it as well")
          assert(!scanIsBatched(wideScan))
          val wide = spark.sql(wideScan).collect().map { row =>
            val payload = row.getAs[Row]("payload")
            (row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Timestamp]("event_time").getTime,
              payload.getAs[Int]("f1"), payload.getAs[Int](s"f$payloadFieldCount"))
          }.sorted.toSeq
          assertResult(expected)(wide)
          assertResult("true")(spark.conf.get(vectorizedReaderConf))
          spark.catalog.dropTempView(tableName)
        }
      }
    }
  }

  // Rows are (id, name, event_time in epoch millis, payload base); the second commit updates id 2
  private val insertedTimestampMillisRows = Seq(
    (1, "a1", 1672567200124L, 1000), (2, "a2", 1672567200125L, 2000), (3, "a3", 1672567200126L, 3000))
  private val updatedTimestampMillisRow = (2, "a2_updated", 1672653600456L, 20000)
  private val timestampMillisRows = insertedTimestampMillisRows.map { row =>
    if (row._1 == updatedTimestampMillisRow._1) updatedTimestampMillisRow else row
  }

  // Spark has no timestamp-millis type, so SQL and DataFrame writes cannot produce this schema
  private lazy val timestampMillisSchema: Schema = {
    val payload = (1 to payloadFieldCount)
      .foldLeft(SchemaBuilder.record("payload").fields())((fields, i) => fields.requiredInt(s"f$i"))
      .endRecord()
    SchemaBuilder.record("ts_millis_record").fields()
      .requiredInt("id")
      .requiredString("name")
      .name("event_time").`type`(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
      .name("payload").`type`(payload).noDefault()
      .endRecord()
  }

  private def timestampMillisRecord(row: (Int, String, Long, Int)): HoodieRecord[IndexedRecord] = {
    val (id, name, eventTime, payloadBase) = row
    val payload = new GenericData.Record(timestampMillisSchema.getField("payload").schema())
    (1 to payloadFieldCount).foreach(i => payload.put(s"f$i", payloadBase + i))
    val record = new GenericData.Record(timestampMillisSchema)
    record.put("id", id)
    record.put("name", name)
    record.put("event_time", eventTime)
    record.put("payload", payload)
    new HoodieAvroIndexedRecord(new HoodieKey(id.toString, ""), record)
  }

  /** Writes three rows, then updates one, which lands in a log file on a MOR table. */
  private def writeTimestampMillisTable(tableName: String, basePath: String, tableType: HoodieTableType): Unit = {
    HoodieTableMetaClient.newTableBuilder()
      .setTableName(tableName)
      .setTableType(tableType)
      .setRecordKeyFields("id")
      .setPartitionFields("")
      .setRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING)
      .initTable(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration), basePath)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .forTable(tableName)
      .withSchema(timestampMillisSchema.toString)
      .withParallelism(1, 1)
      .withRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING)
      .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
      .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
      .withEmbeddedTimelineServerEnabled(false)
      .build()
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)
    val client = new SparkRDDWriteClient[IndexedRecord](new HoodieSparkEngineContext(jsc), writeConfig)
    try {
      Seq(insertedTimestampMillisRows -> true, Seq(updatedTimestampMillisRow) -> false).foreach { case (rows, isInsert) =>
        val records = jsc.parallelize(rows.map(timestampMillisRecord).asJava, 1)
        val instant = client.startCommit()
        val statuses = if (isInsert) client.insert(records, instant) else client.upsert(records, instant)
        assert(client.commit(instant, jsc.parallelize(statuses.collect(), 1)))
      }
    } finally {
      client.close()
    }
  }

  private def createWideTable(tableName: String, tableType: String, tmp: File,
                              extraProps: Map[String, String] = Map.empty): Unit = {
    val payloadType = (1 to payloadFieldCount).map(i => s"f$i int").mkString("struct<", ", ", ">")
    val extraTblProps = extraProps.map { case (k, v) => s",\n  '$k' = '$v'" }.mkString
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  payload $payloadType,
         |  ts long
         |) using hudi
         | location '${tmp.getCanonicalPath}/$tableName'
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'$extraTblProps
         | )
       """.stripMargin)
    val rows = (1 to 3).map { id =>
      val payload = (1 to payloadFieldCount).map(i => s"'f$i', ${id * 1000 + i}").mkString("named_struct(", ", ", ")")
      s"select $id as id, 'a$id' as name, $payload as payload, ${id * 1000}L as ts"
    }
    spark.sql(s"insert into $tableName ${rows.mkString(" union all ")}")
    assert(WholeStageCodegenExec.isTooManyFields(spark.sessionState.conf, spark.table(tableName).schema),
      "select * must be too wide for whole-stage codegen, otherwise Spark asks supportBatch for it as well")
  }

  /** Runs select * so that the scan stays wide, and checks the rows that came back. */
  private def checkWideScan(tableName: String): Unit = {
    val actual = spark.sql(s"select * from $tableName").collect().map { row =>
      val payload = row.getAs[Row]("payload")
      (row.getAs[Int]("id"), row.getAs[String]("name"), payload.getAs[Int]("f1"), payload.getAs[Int](s"f$payloadFieldCount"))
    }.sorted.toSeq
    assertResult(Seq((1, "a1", 1001, 1120), (2, "a2", 2001, 2120), (3, "a3", 3001, 3120)))(actual)
  }

  private def scanIsBatched(sql: String): Boolean = {
    val plan = spark.sql(sql).queryExecution.executedPlan
    collectFirst(plan) { case scan: FileSourceScanExec => scan.supportsColumnar }
      .getOrElse(fail(s"No FileSourceScanExec in the plan of [$sql]:\n$plan"))
  }
}

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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.common.model.PartialUpdateAvroPayload
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertTrue

import scala.collection.JavaConverters._

/**
 * Event-time merges onto records whose nullable `long` ordering value is stored as null. The row-writer
 * bulk_insert stores the nulls; a stored null ranks below any incoming value, so updates and deletes win.
 */
class TestPersistedNullOrdering extends HoodieSparkSqlTestBase {

  private val schema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("ts", LongType, nullable = true),
    StructField("_hoodie_is_deleted", BooleanType, nullable = false)))

  Seq(
    ("COPY_ON_WRITE", "SIMPLE", None),
    ("MERGE_ON_READ", "SIMPLE", None),
    ("MERGE_ON_READ", "GLOBAL_SIMPLE", None),
    ("MERGE_ON_READ", "SIMPLE", Some(classOf[PartialUpdateAvroPayload].getName))
  ).foreach { case (tableType, indexType, payloadClass) =>
    test(s"Merge onto stored null ordering values ($tableType, $indexType, payload=${payloadClass.getOrElse("none")})") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        def write(op: String, mode: SaveMode, rows: Row*): Unit = {
          val writer = spark.createDataFrame(rows.asJava, schema).write.format("hudi")
            .option("hoodie.table.name", "persisted_null_ordering")
            .option("hoodie.datasource.write.table.type", tableType)
            .option("hoodie.datasource.write.recordkey.field", "id")
            .option("hoodie.datasource.write.precombine.field", "ts")
            .option("hoodie.datasource.write.partitionpath.field", "")
            .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
            .option("hoodie.index.type", indexType)
            .option("hoodie.datasource.write.operation", op)
            .option("hoodie.datasource.write.row.writer.enable", "true")
            .option("hoodie.metadata.enable", "false")
          payloadClass.fold(writer.option("hoodie.record.merge.mode", "EVENT_TIME_ORDERING"))(
            writer.option("hoodie.datasource.write.payload.class", _)).mode(mode).save(path)
        }
        def read(): Array[Row] =
          spark.read.format("hudi").load(path).select("id", "name", "ts").orderBy("id").collect()

        write("bulk_insert", SaveMode.Overwrite, Row(1, "a", null, false), Row(2, "b", null, false), Row(3, "c", null, false))
        checkAnswer(read())(Seq(1, "a", null), Seq(2, "b", null), Seq(3, "c", null))

        write("upsert", SaveMode.Append, Row(1, "a1", 100L, false), Row(2, "b", 50L, true))
        val expected = Seq(Seq(1, "a1", 100L), Seq(3, "c", null))
        checkAnswer(read())(expected: _*)

        if (tableType == "MERGE_ON_READ") {
          spark.sql(s"call run_compaction(op => 'scheduleAndExecute', path => '$path', options => 'hoodie.metadata.enable=false,hoodie.compact.inline.max.delta.commits=1')")
          assertTrue(createMetaClient(spark, path).getActiveTimeline.filterCompletedInstants().getInstants.asScala
            .exists(_.getAction == HoodieTimeline.COMMIT_ACTION), "expected a completed compaction")
          checkAnswer(read())(expected: _*)
        }
      }
    }
  }
}

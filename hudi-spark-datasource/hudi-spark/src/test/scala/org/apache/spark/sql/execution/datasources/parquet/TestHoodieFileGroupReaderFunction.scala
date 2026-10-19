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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.io.{ByteArrayOutputStream, ObjectOutputStream, OutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Spark Java-serializes the function returned by [[HoodieFileGroupReaderBasedFileFormat.buildReaderWithPartitionValues]]
 * into every task. It must hold only broadcast handles, not the file format, a meta client or a Hadoop configuration.
 */
class TestHoodieFileGroupReaderFunction extends HoodieSparkClientTestBase {

  private val maxSerializedFunctionBytes = 16 * 1024

  private val classesNotSerializedWithFunction: Seq[Class[_]] = Seq(
    classOf[HoodieTableMetaClient],
    classOf[HoodieFileGroupReaderBasedFileFormat],
    classOf[HadoopStorageConfiguration],
    classOf[org.apache.hadoop.conf.Configuration])

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testReaderFunctionHoldsNoDriverState(tableType: HoodieTableType): Unit = {
    val opts = Map(
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.upsert.shuffle.parallelism" -> "2")
    val inserts = recordsToStrings(dataGen.generateInserts("001", 20)).asScala.toSeq
    sparkSession.read.json(sparkSession.sparkContext.parallelize(inserts, 2))
      .write.format("hudi").options(opts).mode(SaveMode.Overwrite).save(basePath)
    val updates = recordsToStrings(dataGen.generateUpdates("002", 10)).asScala.toSeq
    sparkSession.read.json(sparkSession.sparkContext.parallelize(updates, 2))
      .write.format("hudi").options(opts).mode(SaveMode.Append).save(basePath)

    val df = sparkSession.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath)
    assertEquals(20, df.count())
    val relation = df.queryExecution.optimizedPlan.collectFirst {
      case l: LogicalRelation => l.relation.asInstanceOf[HadoopFsRelation]
    }.get
    val format = relation.fileFormat.asInstanceOf[HoodieFileGroupReaderBasedFileFormat]
    // Spark's scan passes the batch decision it planned with ("returning_batch" is FileFormat.OPTION_RETURNING_BATCH).
    val returningBatch = format.supportBatch(sparkSession, relation.schema)
    val options = relation.options + ("returning_batch" -> returningBatch.toString)
    val readerFunction = format.buildReaderWithPartitionValues(sparkSession, relation.dataSchema, relation.partitionSchema,
      relation.dataSchema, Seq.empty, options, sparkSession.sessionState.newHadoopConfWithOptions(options))
    assertTrue(readerFunction.isInstanceOf[HoodieFileGroupReaderFunction],
      s"Unexpected reader function ${readerFunction.getClass.getName}")

    val bytes = new ByteArrayOutputStream()
    val out = new ClassRecordingObjectOutputStream(bytes)
    out.writeObject(readerFunction)
    out.close()
    val captured = classesNotSerializedWithFunction.flatMap(forbidden =>
      out.classes.filter(c => forbidden.isAssignableFrom(c)).map(_.getName))
    assertTrue(captured.isEmpty, s"The serialized reader function must not contain $captured")
    assertTrue(bytes.size() < maxSerializedFunctionBytes,
      s"The serialized reader function takes ${bytes.size()} bytes, over the budget of $maxSerializedFunctionBytes")
  }

  /**
   * Records the class of every object written to the stream.
   */
  private class ClassRecordingObjectOutputStream(out: OutputStream) extends ObjectOutputStream(out) {
    val classes: mutable.Set[Class[_]] = mutable.LinkedHashSet[Class[_]]()
    enableReplaceObject(true)

    override def replaceObject(obj: AnyRef): AnyRef = {
      if (obj != null) {
        classes += obj.getClass
      }
      obj
    }
  }
}

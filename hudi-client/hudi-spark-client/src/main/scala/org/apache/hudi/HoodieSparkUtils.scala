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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.SPARK_VERSION
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLConfInjectingRDD
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[hudi] trait SparkVersionsSupport {
  def getSparkVersion: String

  def isSpark2: Boolean = getSparkVersion.startsWith("2.")
  def isSpark3: Boolean = getSparkVersion.startsWith("3.")
  def isSpark3_0: Boolean = getSparkVersion.startsWith("3.0")
  def isSpark3_1: Boolean = getSparkVersion.startsWith("3.1")
  def isSpark3_2: Boolean = getSparkVersion.startsWith("3.2")
  def isSpark3_3: Boolean = getSparkVersion.startsWith("3.3")

  def gteqSpark3_0: Boolean = getSparkVersion >= "3.0"
  def gteqSpark3_1: Boolean = getSparkVersion >= "3.1"
  def gteqSpark3_1_3: Boolean = getSparkVersion >= "3.1.3"
  def gteqSpark3_2: Boolean = getSparkVersion >= "3.2"
  def gteqSpark3_2_1: Boolean = getSparkVersion >= "3.2.1"
  def gteqSpark3_3: Boolean = getSparkVersion >= "3.3"
}

object HoodieSparkUtils extends SparkAdapterSupport with SparkVersionsSupport {

  override def getSparkVersion: String = SPARK_VERSION

  def getMetaSchema: StructType = {
    StructType(HoodieRecord.HOODIE_META_COLUMNS.asScala.map(col => {
      StructField(col, StringType, nullable = true)
    }))
  }

  /**
   * @deprecated please use other overload [[createRdd]]
   */
  def createRdd(df: DataFrame, structName: String, recordNamespace: String, reconcileToLatestSchema: Boolean,
                latestTableSchema: org.apache.hudi.common.util.Option[Schema] = org.apache.hudi.common.util.Option.empty()): RDD[GenericRecord] = {
    var latestTableSchemaConverted : Option[Schema] = None

    if (latestTableSchema.isPresent && reconcileToLatestSchema) {
      latestTableSchemaConverted = Some(latestTableSchema.get())
    } else {
      // cases when users want to use latestTableSchema but have not turned on reconcileToLatestSchema explicitly
      // for example, when using a Transformer implementation to transform source RDD to target RDD
      latestTableSchemaConverted = if (latestTableSchema.isPresent) Some(latestTableSchema.get()) else None
    }
    createRdd(df, structName, recordNamespace, latestTableSchemaConverted)
  }

  def createRdd(df: DataFrame, structName: String, recordNamespace: String, readerAvroSchemaOpt: Option[Schema]): RDD[GenericRecord] = {
    val writerSchema = df.schema
    val writerAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(writerSchema, structName, recordNamespace)
    val readerAvroSchema = readerAvroSchemaOpt.getOrElse(writerAvroSchema)
    // We check whether passed in reader schema is identical to writer schema to avoid costly serde loop of
    // making Spark deserialize its internal representation [[InternalRow]] into [[Row]] for subsequent conversion
    // (and back)
    val sameSchema = writerAvroSchema.equals(readerAvroSchema)
    val (nullable, _) = AvroConversionUtils.resolveAvroTypeNullability(writerAvroSchema)

    // NOTE: We have to serialize Avro schema, and then subsequently parse it on the executor node, since Spark
    //       serializer is not able to digest it
    val readerAvroSchemaStr = readerAvroSchema.toString
    val writerAvroSchemaStr = writerAvroSchema.toString

    // NOTE: We're accessing toRdd here directly to avoid [[InternalRow]] to [[Row]] conversion
    //       Additionally, we have to explicitly wrap around resulting [[RDD]] into the one
    //       injecting [[SQLConf]], which by default isn't propgated by Spark to the executor(s).
    //       [[SQLConf]] is required by [[AvroSerializer]]
    injectSQLConf(df.queryExecution.toRdd.mapPartitions { rows =>
      if (rows.isEmpty) {
        Iterator.empty
      } else {
        val readerAvroSchema = new Schema.Parser().parse(readerAvroSchemaStr)
        val transform: GenericRecord => GenericRecord =
          if (sameSchema) identity
          else {
            HoodieAvroUtils.rewriteRecordDeep(_, readerAvroSchema)
          }

        // Since caller might request to get records in a different ("evolved") schema, we will be rewriting from
        // existing Writer's schema into Reader's (avro) schema
        val writerAvroSchema = new Schema.Parser().parse(writerAvroSchemaStr)
        val convert = AvroConversionUtils.createInternalRowToAvroConverter(writerSchema, writerAvroSchema, nullable = nullable)

        rows.map { ir => transform(convert(ir)) }
      }
    }, SQLConf.get)
  }

  def getCatalystRowSerDe(structType: StructType) : SparkRowSerDe = {
    sparkAdapter.createSparkRowSerDe(structType)
  }

  private def injectSQLConf[T: ClassTag](rdd: RDD[T], conf: SQLConf): RDD[T] =
    new SQLConfInjectingRDD(rdd, conf)
}

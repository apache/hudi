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

import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.avro.{AvroSchemaUtils, HoodieAvroUtils}
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.util.ExceptionWrappingIterator

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getTimeZone
import org.apache.spark.sql.execution.SQLConfInjectingRDD
import org.apache.spark.sql.execution.datasources.SparkParsePartitionUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, HoodieUnsafeUtils}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[hudi] trait SparkVersionsSupport {
  def getSparkVersion: String

  def isSpark3: Boolean = getSparkVersion.startsWith("3.")
  def isSpark4: Boolean = getSparkVersion.startsWith("4.")
  def isSpark3_3: Boolean = getSparkVersion.startsWith("3.3")
  def isSpark3_4: Boolean = getSparkVersion.startsWith("3.4")
  def isSpark3_5: Boolean = getSparkVersion.startsWith("3.5")
  def isSpark4_0: Boolean = getSparkVersion.startsWith("4.0")

  def gteqSpark3_3_2: Boolean = getSparkVersion >= "3.3.2"
  def gteqSpark3_4: Boolean = getSparkVersion >= "3.4"
  def gteqSpark3_5: Boolean = getSparkVersion >= "3.5"
  def gteqSpark4_0: Boolean = getSparkVersion >= "4.0"
}

object HoodieSparkUtils extends SparkAdapterSupport with SparkVersionsSupport with Logging {

  override def getSparkVersion: String = SPARK_VERSION

  def getMetaSchema: StructType = {
    StructType(HoodieRecord.HOODIE_META_COLUMNS.asScala.map(col => {
      StructField(col, StringType, nullable = true)
    }).toSeq)
  }

  /**
   * @deprecated please use other overload [[createRdd]]
   */
  @Deprecated
  def createRdd(df: DataFrame, structName: String, recordNamespace: String, reconcileToLatestSchema: Boolean,
                latestTableSchema: org.apache.hudi.common.util.Option[Schema] = org.apache.hudi.common.util.Option.empty()): RDD[GenericRecord] = {
    createRdd(df, structName, recordNamespace, toScalaOption(latestTableSchema))
  }

  def createRdd(df: DataFrame, structName: String, recordNamespace: String): RDD[GenericRecord] =
    createRdd(df, structName, recordNamespace, None)

  def createRdd(df: DataFrame, structName: String, recordNamespace: String, readerAvroSchemaOpt: Option[Schema]): RDD[GenericRecord] = {
    val writerSchema = df.schema
    val writerAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(writerSchema, structName, recordNamespace)
    val readerAvroSchema = readerAvroSchemaOpt.getOrElse(writerAvroSchema)
    // We check whether passed in reader schema is identical to writer schema to avoid costly serde loop of
    // making Spark deserialize its internal representation [[InternalRow]] into [[Row]] for subsequent conversion
    // (and back)
    val sameSchema = writerAvroSchema.equals(readerAvroSchema)
    val nullable = AvroSchemaUtils.getNonNullTypeFromUnion(writerAvroSchema) != writerAvroSchema

    // NOTE: We have to serialize Avro schema, and then subsequently parse it on the executor node, since Spark
    //       serializer is not able to digest it
    val readerAvroSchemaStr = readerAvroSchema.toString
    val writerAvroSchemaStr = writerAvroSchema.toString

    // NOTE: We're accessing toRdd here directly to avoid [[InternalRow]] to [[Row]] conversion
    //       Additionally, we have to explicitly wrap around resulting [[RDD]] into the one
    //       injecting [[SQLConf]], which by default isn't propagated by Spark to the executor(s).
    //       [[SQLConf]] is required by [[AvroSerializer]]
    injectSQLConf(df.queryExecution.toRdd.mapPartitions (rows => {
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
    }, preservesPartitioning = true), SQLConf.get)
  }

  def injectSQLConf[T: ClassTag](rdd: RDD[T], conf: SQLConf): RDD[T] =
    new SQLConfInjectingRDD(rdd, conf)

  def maybeWrapDataFrameWithException(df: DataFrame, exceptionClass: String, msg: String, shouldWrap: Boolean): DataFrame = {
    if (shouldWrap) {
      sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(df.sparkSession, injectSQLConf(df.queryExecution.toRdd.mapPartitions {
        rows => new ExceptionWrappingIterator[InternalRow](rows, exceptionClass, msg)
      }, SQLConf.get), df.schema)
    } else {
      df
    }
  }

  def safeCreateRDD(df: DataFrame, structName: String, recordNamespace: String, reconcileToLatestSchema: Boolean,
                    latestTableSchema: org.apache.hudi.common.util.Option[Schema] = org.apache.hudi.common.util.Option.empty()):
  Tuple2[RDD[GenericRecord], RDD[String]] = {
    var latestTableSchemaConverted: Option[Schema] = None

    if (latestTableSchema.isPresent && reconcileToLatestSchema) {
      latestTableSchemaConverted = Some(latestTableSchema.get())
    } else {
      // cases when users want to use latestTableSchema but have not turned on reconcileToLatestSchema explicitly
      // for example, when using a Transformer implementation to transform source RDD to target RDD
      latestTableSchemaConverted = if (latestTableSchema.isPresent) Some(latestTableSchema.get()) else None
    }
    safeCreateRDD(df, structName, recordNamespace, latestTableSchemaConverted);
  }

  def safeCreateRDD(df: DataFrame, structName: String, recordNamespace: String, readerAvroSchemaOpt: Option[Schema]):
  Tuple2[RDD[GenericRecord], RDD[String]] = {
    val writerSchema = df.schema
    val writerAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(writerSchema, structName, recordNamespace)
    val readerAvroSchema = readerAvroSchemaOpt.getOrElse(writerAvroSchema)
    // We check whether passed in reader schema is identical to writer schema to avoid costly serde loop of
    // making Spark deserialize its internal representation [[InternalRow]] into [[Row]] for subsequent conversion
    // (and back)
    val sameSchema = writerAvroSchema.equals(readerAvroSchema)
    val nullable = AvroSchemaUtils.getNonNullTypeFromUnion(writerAvroSchema) != writerAvroSchema

    // NOTE: We have to serialize Avro schema, and then subsequently parse it on the executor node, since Spark
    //       serializer is not able to digest it
    val writerAvroSchemaStr = writerAvroSchema.toString
    val readerAvroSchemaStr = readerAvroSchema.toString
    // NOTE: We're accessing toRdd here directly to avoid [[InternalRow]] to [[Row]] conversion

    if (!sameSchema) {
      val rdds: RDD[Either[GenericRecord, InternalRow]] = df.queryExecution.toRdd.mapPartitions { rows =>
        if (rows.isEmpty) {
          Iterator.empty
        } else {
          val writerAvroSchema = new Schema.Parser().parse(writerAvroSchemaStr)
          val readerAvroSchema = new Schema.Parser().parse(readerAvroSchemaStr)
          val convert = AvroConversionUtils.createInternalRowToAvroConverter(writerSchema, writerAvroSchema, nullable = nullable)
          val transform: InternalRow => Either[GenericRecord, InternalRow] = internalRow => try {
            Left(HoodieAvroUtils.rewriteRecordDeep(convert(internalRow), readerAvroSchema, true))
          } catch {
            case _: Throwable =>
              Right(internalRow)
          }
          rows.map(transform)
        }
      }

      val rowDeserializer = getCatalystRowSerDe(writerSchema)
      val errorRDD = df.sparkSession.createDataFrame(
        rdds.filter(_.isRight).map(_.right.get).map(ir => rowDeserializer.deserializeRow(ir)), writerSchema)

      // going to follow up on improving performance of separating out events
      (rdds.filter(_.isLeft).map(_.left.get), errorRDD.toJSON.rdd)
    } else {
      val rdd = df.queryExecution.toRdd.mapPartitions { rows =>
        if (rows.isEmpty) {
          Iterator.empty
        } else {
          val convert = AvroConversionUtils.createInternalRowToAvroConverter(writerSchema, writerAvroSchema, nullable = nullable)
          rows.map(convert)
        }
      }
      (rdd, df.sparkSession.sparkContext.emptyRDD[String])
    }
  }

  /**
   * Rerwite the record into the target schema.
   * Return tuple of rewritten records and records that could not be converted
   */
  def safeRewriteRDD(df: RDD[GenericRecord], serializedTargetSchema: String): Tuple2[RDD[GenericRecord], RDD[String]] = {
    val rdds: RDD[Either[GenericRecord, String]] = df.mapPartitions { recs =>
      if (recs.isEmpty) {
        Iterator.empty
      } else {
        val schema = new Schema.Parser().parse(serializedTargetSchema)
        val transform: GenericRecord => Either[GenericRecord, String] = record => try {
          Left(HoodieAvroUtils.rewriteRecordDeep(record, schema, true))
        } catch {
          case _: Throwable => Right(HoodieAvroUtils.safeAvroToJsonString(record))
        }
        recs.map(transform)
      }
    }
    (rdds.filter(_.isLeft).map(_.left.get), rdds.filter(_.isRight).map(_.right.get))
  }

  def getCatalystRowSerDe(structType: StructType): SparkRowSerDe = {
    new SparkRowSerDe(sparkAdapter.getCatalystExpressionUtils.getEncoder(structType))
  }

  def parsePartitionColumnValues(partitionColumns: Array[String],
                                   partitionPath: String,
                                   tableBasePath: StoragePath,
                                   tableSchema: StructType,
                                   tableConfig: java.util.Map[String, String],
                                   timeZoneId: String,
                                   shouldValidatePartitionColumns: Boolean): Array[Object] = {
    val keyGeneratorClass = KeyGeneratorType.getKeyGeneratorClassName(tableConfig)
    val timestampKeyGeneratorType = tableConfig.get(TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD.key())

    if (null != keyGeneratorClass
      && null != timestampKeyGeneratorType
      && keyGeneratorClass.equals(KeyGeneratorType.TIMESTAMP.getClassName)
      && !timestampKeyGeneratorType.matches(TimestampBasedAvroKeyGenerator.TimestampType.DATE_STRING.toString)) {
      // For TIMESTAMP key generator when TYPE is not DATE_STRING (like SCALAR, UNIX_TIMESTAMP, EPOCHMILLISECONDS, etc.),
      // we couldn't reconstruct initial partition column values from partition paths due to lost data after formatting.
      // But the output for these cases is in a string format, so we can pass partitionPath as UTF8String
      Array.fill(partitionColumns.length)(UTF8String.fromString(partitionPath))
    } else {
      doParsePartitionColumnValues(partitionColumns, partitionPath, tableBasePath, tableSchema, timeZoneId,
        shouldValidatePartitionColumns)
    }
  }

  def doParsePartitionColumnValues(partitionColumns: Array[String],
                                 partitionPath: String,
                                 basePath: StoragePath,
                                 schema: StructType,
                                 timeZoneId: String,
                                 shouldValidatePartitionCols: Boolean): Array[Object] = {
    if (partitionColumns.length == 0) {
      // This is a non-partitioned table
      Array.empty
    } else {
      val partitionFragments = partitionPath.split(StoragePath.SEPARATOR)
      if (partitionFragments.length != partitionColumns.length) {
        if (partitionColumns.length == 1) {
          // If the partition column size is not equal to the partition fragment size
          // and the partition column size is 1, we map the whole partition path
          // to the partition column which can benefit from the partition prune.
          val prefix = s"${partitionColumns.head}="
          val partitionValue = if (partitionPath.startsWith(prefix)) {
            // support hive style partition path
            partitionPath.substring(prefix.length)
          } else {
            partitionPath
          }
          val partitionSchema = buildPartitionSchemaForNestedFields(schema, partitionColumns)
          val typedValue = if (partitionSchema.fields.nonEmpty) {
            castStringToType(partitionValue, partitionSchema.fields.head.dataType)
          } else {
            UTF8String.fromString(partitionValue)
          }
          Array(typedValue.asInstanceOf[Object])
        } else {
          val prefix = s"${partitionColumns.head}="
          if (partitionPath.startsWith(prefix)) {
            val partitionValues = splitHiveSlashPartitions(partitionFragments, partitionColumns.length)
            val partitionSchema = buildPartitionSchemaForNestedFields(schema, partitionColumns)
            val typedValues = partitionValues.zip(partitionSchema.fields).map { case (stringValue, field) =>
              castStringToType(stringValue, field.dataType)
            }
            typedValues.map(_.asInstanceOf[Object])
          } else {
            // If the partition column size is not equal to the partition fragments size
            // and the partition column size > 1, we do not know how to map the partition
            // fragments to the partition columns and therefore return an empty tuple. We don't
            // fail outright so that in some cases we can fallback to reading the table as non-partitioned
            // one
            logWarning(s"Failed to parse partition values: found partition fragments" +
              s" (${partitionFragments.mkString(",")}) are not aligned with expected partition columns" +
              s" (${partitionColumns.mkString(",")})")
            Array.empty
          }
        }
      } else {
        // If partitionSeqs.length == partitionSchema.fields.length
        // Append partition name to the partition value if the
        // HIVE_STYLE_PARTITIONING is disable.
        // e.g. convert "/xx/xx/2021/02" to "/xx/xx/year=2021/month=02"
        val partitionWithName =
        partitionFragments.zip(partitionColumns).map {
          case (partition, columnName) =>
            if (partition.indexOf("=") == -1) {
              s"${columnName}=$partition"
            } else {
              partition
            }
        }.mkString(StoragePath.SEPARATOR)

        val pathWithPartitionName = new StoragePath(basePath, partitionWithName)
        val partitionSchema = buildPartitionSchemaForNestedFields(schema, partitionColumns)
        val partitionValues = parsePartitionPath(pathWithPartitionName, partitionSchema, timeZoneId,
          basePath, shouldValidatePartitionCols)
        partitionValues.map(_.asInstanceOf[Object]).toArray
      }
    }
  }

  private def parsePartitionPath(partitionPath: StoragePath, partitionSchema: StructType, timeZoneId: String,
                                 basePath: StoragePath, shouldValidatePartitionCols: Boolean): Seq[Any] = {
    val partitionDataTypes = partitionSchema.map(f => f.name -> f.dataType).toMap
    SparkParsePartitionUtil.parsePartition(
      new Path(partitionPath.toUri),
      typeInference = false,
      Set(new Path(basePath.toUri)),
      partitionDataTypes,
      getTimeZone(timeZoneId),
      validatePartitionValues = shouldValidatePartitionCols
    ).toSeq(partitionSchema)
  }

  private def buildPartitionSchemaForNestedFields(schema: StructType, partitionColumns: Array[String]): StructType = {
    val partitionFields = partitionColumns.flatMap { partitionCol =>
      extractNestedField(schema, partitionCol)
    }
    StructType(partitionFields)
  }

  private def extractNestedField(schema: StructType, fieldPath: String): Option[StructField] = {
    val pathParts = fieldPath.split("\\.")

    def traverseSchema(currentSchema: StructType, remainingPath: List[String], originalFieldName: String): Option[StructField] = {
      remainingPath match {
        case Nil => None
        case head :: Nil =>
          currentSchema.fields.find(_.name == head).map { field =>
            StructField(originalFieldName, field.dataType, field.nullable, field.metadata)
          }
        case head :: tail =>
          currentSchema.fields.find(_.name == head) match {
            case Some(StructField(_, structType: StructType, _, _)) =>
              traverseSchema(structType, tail, originalFieldName)
            case _ => None
          }
      }
    }
    traverseSchema(schema, pathParts.toList, fieldPath)
  }

  private def castStringToType(value: String, dataType: org.apache.spark.sql.types.DataType): Any = {
    import org.apache.spark.sql.types._

    // handling cases where the value contains path separators or is complex
    if (value.contains("/") || value.contains("=")) {
      // For complex paths, falling back to string representation
      logWarning(s"Cannot convert complex partition path '$value' to $dataType, keeping as string")
      return UTF8String.fromString(value)
    }

    try {
      dataType match {
        case LongType => value.toLong
        case IntegerType => value.toInt
        case ShortType => value.toShort
        case ByteType => value.toByte
        case FloatType => value.toFloat
        case DoubleType => value.toDouble
        case BooleanType => value.toBoolean
        case _: DecimalType => new java.math.BigDecimal(value)
        case StringType => UTF8String.fromString(value)
        case _: TimestampType =>
          UTF8String.fromString(value)
        case _: DateType =>
          UTF8String.fromString(value)
        case _ => UTF8String.fromString(value)
      }
    } catch {
      case _: NumberFormatException =>
        logWarning(s"Failed to convert '$value' to $dataType, keeping as string")
        UTF8String.fromString(value)
      case _: Exception =>
        logWarning(s"Error converting '$value' to $dataType, keeping as string")
        UTF8String.fromString(value)
    }
  }

  def splitHiveSlashPartitions(partitionFragments: Array[String], nPartitions: Int): Array[String] = {
    val partitionVals = new Array[String](nPartitions)
    var index = 0
    var first = true
    for (fragment <- partitionFragments) {
      if (fragment.contains("=")) {
        if (first) {
          first = false
        } else {
          index += 1
        }
        partitionVals(index) = fragment.substring(fragment.indexOf("=") + 1)

      } else {
        partitionVals(index) += StoragePath.SEPARATOR + fragment
      }
    }
    return partitionVals
  }
}

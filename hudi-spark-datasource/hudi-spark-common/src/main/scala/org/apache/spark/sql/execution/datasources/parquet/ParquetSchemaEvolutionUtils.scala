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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.timeline.TimelineLayout
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.util
import org.apache.hudi.common.util.InternalSchemaCache
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.action.InternalSchemaMerger
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaEvolutionUtils.pruneInternalSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, DataType, StructType}

import java.time.ZoneId

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ParquetSchemaEvolutionUtils(sharedConf: Configuration,
                                  filePath: Path,
                                  requiredSchema: StructType,
                                  partitionSchema: StructType,
                                  internalSchemaOpt: util.Option[InternalSchema],
                                  tableSchemaOpt: util.Option[org.apache.parquet.schema.MessageType] = util.Option.empty()) extends SparkAdapterSupport {
  // Fetch internal schema
  private lazy val querySchemaOption: util.Option[InternalSchema] = pruneInternalSchema(internalSchemaOpt, requiredSchema)

  var shouldUseInternalSchema: Boolean = querySchemaOption.isPresent && tablePath != null

  private lazy val schemaUtils: HoodieSchemaUtils = sparkAdapter.getSchemaUtils

  private lazy val tablePath: String = sharedConf.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH)
  private lazy val fileSchema: InternalSchema = if (shouldUseInternalSchema) {
    val commitInstantTime = FSUtils.getCommitTime(filePath.getName).toLong
    //TODO: HARDCODED TIMELINE OBJECT
    val validCommits = sharedConf.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST)
    val layout = TimelineLayout.fromVersion(TimelineLayoutVersion.CURR_LAYOUT_VERSION)
    InternalSchemaCache.getInternalSchemaByVersionId(commitInstantTime, tablePath,
      new HoodieHadoopStorage(tablePath, sharedConf), if (validCommits == null) "" else validCommits, layout)
  } else {
    null
  }

  // Columns that need timestamp correction (micros labeled as millis)
  private var columnsToMultiply: Set[String] = Set.empty

  def rebuildFilterFromParquet(filter: Filter): Filter = {
    rebuildFilterFromParquetHelper(filter, fileSchema, querySchemaOption.orElse(null))
  }

  /**
   * Adjusts timestamp filter values to account for precision mismatches in Parquet schema.
   * When file schema has timestamps in micros but labeled as millis, filter values need
   * to be divided by 1000 so they match the mis-labeled data correctly.
   *
   * @param filter The filter to adjust
   * @return Adjusted filter
   */
  def adjustFilterForTimestampCorrection(filter: Filter): Filter = {
    adjustFilterHelper(filter, columnsToMultiply)
  }

  private def adjustTimestampValue(value: Any): Any = value match {
    case ts: java.sql.Timestamp => divideBy1000(ts)
    case l: Long => l / 1000
    case other => other
  }

  private def divideBy1000(ts: java.sql.Timestamp): java.sql.Timestamp = {
    val millis = ts.getTime
    val nanos = ts.getNanos % 1000000

    val totalNanos: Long = millis * 1000000L + nanos
    val scaledNanos = totalNanos / 1000
    val result = new java.sql.Timestamp(scaledNanos / 1000000L)
    result.setNanos((scaledNanos % 1000000000L).toInt)
    result
  }

  private def adjustFilterHelper(filter: Filter, columnsToMultiply: Set[String]): Filter = {
    if (columnsToMultiply.isEmpty) {
      filter
    } else {
      filter match {
        case eq: EqualTo if columnsToMultiply.contains(eq.attribute) =>
          eq.copy(value = adjustTimestampValue(eq.value))
        case eqs: EqualNullSafe if columnsToMultiply.contains(eqs.attribute) =>
          eqs.copy(value = adjustTimestampValue(eqs.value))
        case gt: GreaterThan if columnsToMultiply.contains(gt.attribute) =>
          gt.copy(value = adjustTimestampValue(gt.value))
        case gtr: GreaterThanOrEqual if columnsToMultiply.contains(gtr.attribute) =>
          gtr.copy(value = adjustTimestampValue(gtr.value))
        case lt: LessThan if columnsToMultiply.contains(lt.attribute) =>
          lt.copy(value = adjustTimestampValue(lt.value))
        case lte: LessThanOrEqual if columnsToMultiply.contains(lte.attribute) =>
          lte.copy(value = adjustTimestampValue(lte.value))
        case i: In if columnsToMultiply.contains(i.attribute) =>
          i.copy(values = i.values.map(adjustTimestampValue))
        case And(left, right) =>
          And(adjustFilterHelper(left, columnsToMultiply),
            adjustFilterHelper(right, columnsToMultiply))
        case Or(left, right) =>
          Or(adjustFilterHelper(left, columnsToMultiply),
            adjustFilterHelper(right, columnsToMultiply))
        case Not(child) =>
          Not(adjustFilterHelper(child, columnsToMultiply))
        case _ =>
          filter
      }
    }
  }

  private def rebuildFilterFromParquetHelper(oldFilter: Filter, fileSchema: InternalSchema, querySchema: InternalSchema): Filter = {
    if (fileSchema == null || querySchema == null) {
      oldFilter
    } else {
      oldFilter match {
        case eq: EqualTo =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eq.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eq.copy(attribute = newAttribute)
        case eqs: EqualNullSafe =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eqs.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eqs.copy(attribute = newAttribute)
        case gt: GreaterThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gt.copy(attribute = newAttribute)
        case gtr: GreaterThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gtr.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gtr.copy(attribute = newAttribute)
        case lt: LessThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lt.copy(attribute = newAttribute)
        case lte: LessThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lte.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lte.copy(attribute = newAttribute)
        case i: In =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(i.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else i.copy(attribute = newAttribute)
        case isn: IsNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isn.copy(attribute = newAttribute)
        case isnn: IsNotNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isnn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isnn.copy(attribute = newAttribute)
        case And(left, right) =>
          And(rebuildFilterFromParquetHelper(left, fileSchema, querySchema), rebuildFilterFromParquetHelper(right, fileSchema, querySchema))
        case Or(left, right) =>
          Or(rebuildFilterFromParquetHelper(left, fileSchema, querySchema), rebuildFilterFromParquetHelper(right, fileSchema, querySchema))
        case Not(child) =>
          Not(rebuildFilterFromParquetHelper(child, fileSchema, querySchema))
        case ssw: StringStartsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ssw.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ssw.copy(attribute = newAttribute)
        case ses: StringEndsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ses.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ses.copy(attribute = newAttribute)
        case sc: StringContains =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(sc.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else sc.copy(attribute = newAttribute)
        case AlwaysTrue =>
          AlwaysTrue
        case AlwaysFalse =>
          AlwaysFalse
        case _ =>
          AlwaysTrue
      }
    }
  }

  protected var typeChangeInfos: java.util.Map[Integer, Pair[DataType, DataType]] = null

  def setFooterSchema(fileSchemaMessageType: org.apache.parquet.schema.MessageType): Unit = {
    columnsToMultiply = if (tableSchemaOpt.isPresent) {
      HoodieParquetFileFormatHelper.findColumnsToMultiply(fileSchemaMessageType, tableSchemaOpt.get())
    } else {
      Set.empty[String]
    }
  }

  def getColumnsToMultiply: Set[String] = columnsToMultiply

  def getHadoopConfClone(footerFileMetaData: FileMetaData, enableVectorizedReader: Boolean): Configuration = {
    // Clone new conf
    val hadoopAttemptConf = new Configuration(sharedConf)
    typeChangeInfos = if (shouldUseInternalSchema) {
      val mergedInternalSchema = new InternalSchemaMerger(fileSchema, querySchemaOption.get(), true, true).mergeSchema()
      val mergedSchema = SparkInternalSchemaConverter.constructSparkSchemaFromInternalSchema(mergedInternalSchema)

      hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, mergedSchema.json)

      SparkInternalSchemaConverter.collectTypeChangedCols(querySchemaOption.get(), mergedInternalSchema)
    } else {
      val (implicitTypeChangeInfo, sparkRequestSchema) = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(hadoopAttemptConf, footerFileMetaData, requiredSchema)
      if (!implicitTypeChangeInfo.isEmpty) {
        shouldUseInternalSchema = true
        hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, sparkRequestSchema.json)
      }
      implicitTypeChangeInfo
    }

    if (enableVectorizedReader && shouldUseInternalSchema &&
      !typeChangeInfos.values().forall(_.getLeft.isInstanceOf[AtomicType])) {
      throw new IllegalArgumentException(
        "Nested types with type changes(implicit or explicit) cannot be read in vectorized mode. " +
          "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false.")
    }

    hadoopAttemptConf
  }

  def generateUnsafeProjection(fullSchema: Seq[AttributeReference], timeZoneId: Option[String]): UnsafeProjection = {
    HoodieParquetFileFormatHelper.generateUnsafeProjection(fullSchema, timeZoneId, typeChangeInfos, requiredSchema, partitionSchema, schemaUtils, columnsToMultiply)
  }

  def buildVectorizedReader(convertTz: ZoneId,
                            datetimeRebaseMode: String,
                            datetimeRebaseTz: String,
                            int96RebaseMode: String,
                            int96RebaseTz: String,
                            useOffHeap: Boolean,
                            capacity: Int): VectorizedParquetRecordReader = {
    if (shouldUseInternalSchema) {
      new HoodieVectorizedParquetRecordReader(
        convertTz,
        datetimeRebaseMode,
        datetimeRebaseTz,
        int96RebaseMode,
        int96RebaseTz,
        useOffHeap,
        capacity,
        typeChangeInfos)
    } else {
      new VectorizedParquetRecordReader(
        convertTz,
        datetimeRebaseMode,
        datetimeRebaseTz,
        int96RebaseMode,
        int96RebaseTz,
        useOffHeap,
        capacity)
    }
  }
}

object ParquetSchemaEvolutionUtils {
  def pruneInternalSchema(internalSchemaOpt:  util.Option[InternalSchema], requiredSchema: StructType): util.Option[InternalSchema] = {
    if (internalSchemaOpt.isPresent && requiredSchema.nonEmpty) {
      util.Option.of(SparkInternalSchemaConverter.convertAndPruneStructTypeToInternalSchema(requiredSchema, internalSchemaOpt.get()))
    } else {
      internalSchemaOpt
    }
  }
}

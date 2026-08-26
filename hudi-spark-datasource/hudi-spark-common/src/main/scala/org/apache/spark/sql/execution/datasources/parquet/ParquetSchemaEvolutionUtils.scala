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
import org.apache.hudi.common.schema.internal.{InternalSchema, Type => InternalType}
import org.apache.hudi.common.schema.internal.Types
import org.apache.hudi.common.schema.internal.action.InternalSchemaMerger
import org.apache.hudi.common.schema.internal.utils.InternalSchemaUtils
import org.apache.hudi.common.table.timeline.TimelineLayout
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.util
import org.apache.hudi.common.util.HoodieStorageUtils
import org.apache.hudi.common.util.InternalSchemaCache
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.{Type => ParquetType}
import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaEvolutionUtils.pruneInternalSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructType}

import java.time.ZoneId

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ParquetSchemaEvolutionUtils(sharedConf: Configuration,
                                  filePath: Path,
                                  requiredSchema: StructType,
                                  partitionSchema: StructType,
                                  internalSchemaOpt: util.Option[InternalSchema]) extends SparkAdapterSupport {
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
      HoodieStorageUtils.getStorage(tablePath, HadoopFSUtils.getStorageConf(sharedConf)), if (validCommits == null) "" else validCommits, layout)
  } else {
    null
  }

  def rebuildFilterFromParquet(filter: Filter): Filter = {
    rebuildFilterFromParquetHelper(filter, fileSchema, querySchemaOption.orElse(null))
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

  def getHadoopConfClone(footerFileMetaData: FileMetaData, enableVectorizedReader: Boolean): Configuration = {
    // Clone new conf
    val hadoopAttemptConf = new Configuration(sharedConf)
    typeChangeInfos = if (shouldUseInternalSchema) {
      // Empty projections (count(*), select 1) read no column data, so there is nothing to
      // reconstruct - and querySchemaOption is the UNPRUNED table schema in that case (see
      // pruneInternalSchema), so running the guard would fail queries that work fine.
      if (requiredSchema.nonEmpty) {
        ParquetSchemaEvolutionUtils.validateNoShreddedVariants(requiredSchema, querySchemaOption.get(), footerFileMetaData)
      }
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
    SparkSchemaTransformUtils.generateUnsafeProjection(fullSchema, timeZoneId, typeChangeInfos, requiredSchema, partitionSchema, schemaUtils)
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

  /**
   * Fails fast when schema-on-read meets a shredded variant file. The internal schema models a
   * variant as a two-field {metadata, value} record (with sentinel negative field ids, see
   * InternalSchemaConverter), so the merged request clips the file's typed_value away and the
   * typed rows would read back with a null value residual - silent data loss. Reconstruction
   * under schema-on-read is tracked by #18285; until then the read must fail loudly. The check
   * anchors on the sentinel ids, which no real user field can carry, so plain user structs of
   * the same shape are left alone. The walk recurses through structs, arrays and maps because
   * the row writer shreds nested variants too (see VariantSchemaUtils).
   *
   * Footer columns are resolved by the query-schema name. A column renamed under schema-on-read
   * still carries its old name in the file and is not matched here; such reads are left to
   * #18285 with reconstruction itself.
   *
   * A request in the full-variant projection shape fails fast regardless of the file's layout:
   * the merged internal-schema request materializes the variant as {metadata, value} while the
   * consumer expects the ordinal-named extraction struct, so the read cannot be served either
   * way (pruning treats the rewritten struct as the variant column itself, see
   * SparkInternalSchemaConverter.isVariantRewriteStruct). Two producers ask for that shape: a
   * query rewritten by Spark's PushVariantIntoScan (4.x), and Hudi's own base-file reads on
   * 4.1+ (SparkFileFormatInternalRowReaderContext, via SparkAdapter.buildFullVariantReadSchema)
   * whenever their reader context carries the table's internal schema - SparkReaderContextFactory
   * puts the table path and valid commits on the conf once one is committed, so inline compaction
   * and clustering under a schema-on-read write, and CDC reads, land here on an unshredded
   * variant column. Upserts do not - the merge handle's base-file read never enters this
   * schema-on-read branch (probed against a committed internal schema) - nor do run_compaction /
   * run_clustering, whose clients carry no internal schema. Before this guard the same reads
   * died inside pruning ("cannot prune col: v.0"), so nothing that worked is lost; the error
   * names both routes rather than blaming pushVariantIntoScan on a read that never set it.
   * Real support is #18285.
   *
   * Shared by [[ParquetSchemaEvolutionUtils.getHadoopConfClone]] and the per-version legacy
   * file formats, which carry a copy of the same schema-merge block. Callers gate on a
   * non-empty projection: empty-projection queries (count(*), select 1) read no column data
   * and must keep working, and the query schema is unpruned in that case.
   */
  def validateNoShreddedVariants(requiredSchema: StructType, querySchema: InternalSchema, footerFileMetaData: FileMetaData): Unit = {
    findVariantRewritePath(requiredSchema).foreach { path =>
      throw new HoodieException(String.format(
        "Column '%s' is a variant requested in Spark's full-variant projection shape - by the "
          + "PushVariantIntoScan rewrite (spark.sql.variant.pushVariantIntoScan) on a query, or by "
          + "Hudi's own base-file reads for compaction, clustering and CDC - and the table is read "
          + "with schema-on-read (hoodie.schema.on.read.enable), which cannot reconstruct variants "
          + "(see issue #18285). Read without schema-on-read, and run compaction and clustering from "
          + "a client without it (run_compaction / run_clustering).", path))
    }
    val fileParquetSchema = footerFileMetaData.getSchema
    querySchema.getRecord.fields().foreach { field =>
      if (fileParquetSchema.containsField(field.name())) {
        validateNoShreddedVariant(
          field.`type`(), fileParquetSchema.getType(fileParquetSchema.getFieldIndex(field.name())), field.name())
      }
    }
  }

  /**
   * The dotted path of the first PushVariantIntoScan rewrite struct in the schema, if any (see
   * SparkInternalSchemaConverter.isVariantRewriteStruct for the marker).
   */
  private def findVariantRewritePath(dataType: DataType, path: String = ""): Option[String] = dataType match {
    case struct: StructType if SparkInternalSchemaConverter.isVariantRewriteStruct(struct) =>
      Some(path)
    case struct: StructType =>
      struct.fields.foldLeft(Option.empty[String]) { (found, field) =>
        found.orElse(findVariantRewritePath(field.dataType, concatPath(path, field.name)))
      }
    case array: ArrayType => findVariantRewritePath(array.elementType, concatPath(path, "element"))
    case map: MapType => findVariantRewritePath(map.valueType, concatPath(path, "value"))
    case _ => None
  }

  private def concatPath(path: String, name: String): String =
    if (path.isEmpty) name else path + "." + name

  private def validateNoShreddedVariant(internalType: InternalType, parquetType: ParquetType, path: String): Unit = {
    internalType match {
      // A variant: two fields, both carrying the sentinel negative ids (BLOB's sentinel record
      // has three). The parquet side decides shredded-ness.
      case record: Types.RecordType if record.fields().size() == 2 && record.fields().forall(_.fieldId() < 0) =>
        if (!parquetType.isPrimitive && parquetType.asGroupType().containsField("typed_value")) {
          throw new HoodieException(String.format(
            "Column '%s' is a shredded variant (typed_value present) and the table is read "
              + "with schema-on-read (hoodie.schema.on.read.enable), which cannot reconstruct "
              + "shredded variants (see issue #18285). Read without schema-on-read, or rewrite "
              + "the table unshredded (e.g. cluster with "
              + "hoodie.parquet.variant.write.shredding.enabled=false).", path))
        }
      case record: Types.RecordType if !parquetType.isPrimitive =>
        val group = parquetType.asGroupType()
        record.fields().foreach { field =>
          if (group.containsField(field.name())) {
            validateNoShreddedVariant(field.`type`(), group.getType(field.name()), path + "." + field.name())
          }
        }
      case array: Types.ArrayType =>
        parquetListElement(parquetType).foreach(validateNoShreddedVariant(array.elementType(), _, path + ".element"))
      case map: Types.MapType =>
        parquetMapValue(parquetType).foreach(validateNoShreddedVariant(map.valueType(), _, path + ".value"))
      case _ =>
    }
  }

  /**
   * Resolves the element type of a parquet LIST group, covering both the 3-level layout the
   * Spark writer produces (group -> repeated "list" -> element) and the 2-level layout
   * parquet-avro produces (group -> repeated element). The 3-level test mirrors Spark's
   * ParquetSchemaConverter.isElementType. An unrecognized shape returns None, which stops the
   * walk without failing the read.
   */
  private[parquet] def parquetListElement(parquetType: ParquetType): Option[ParquetType] = {
    if (parquetType.isPrimitive || parquetType.asGroupType().getFieldCount != 1) {
      None
    } else {
      val repeated = parquetType.asGroupType().getType(0)
      if (!repeated.isRepetition(ParquetType.Repetition.REPEATED)) {
        None
      } else if (!repeated.isPrimitive && repeated.asGroupType().getFieldCount == 1
        && repeated.getName != "array" && repeated.getName != parquetType.getName + "_tuple") {
        Some(repeated.asGroupType().getType(0))
      } else {
        Some(repeated)
      }
    }
  }

  /** Resolves the value type of a parquet MAP group; an unrecognized shape returns None. */
  private[parquet] def parquetMapValue(parquetType: ParquetType): Option[ParquetType] = {
    if (parquetType.isPrimitive || parquetType.asGroupType().getFieldCount != 1) {
      None
    } else {
      val keyValue = parquetType.asGroupType().getType(0)
      if (keyValue.isPrimitive || !keyValue.asGroupType().containsField("value")) {
        None
      } else {
        Some(keyValue.asGroupType().getType("value"))
      }
    }
  }
}

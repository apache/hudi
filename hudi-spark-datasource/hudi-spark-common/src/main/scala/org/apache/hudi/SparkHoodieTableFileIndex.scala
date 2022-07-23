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

package org.apache.hudi

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.DataSourceReadOptions.{PARTITION_FIELD_IS_STRING, PARTITION_FIELD_NAMES, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.SparkHoodieTableFileIndex.{deduceQueryType, generateFieldMap, toJavaOption}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.{FileSlice, HoodieTableQueryType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.keygen.{TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, NoopCache}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Implementation of the [[BaseHoodieTableFileIndex]] for Spark
 *
 * @param spark spark session
 * @param metaClient Hudi table's meta-client
 * @param schemaSpec optional table's schema
 * @param configProperties unifying configuration (in the form of generic properties)
 * @param specifiedQueryInstant instant as of which table is being queried
 * @param fileStatusCache transient cache of fetched [[FileStatus]]es
 */
class SparkHoodieTableFileIndex(spark: SparkSession,
                                metaClient: HoodieTableMetaClient,
                                schemaSpec: Option[StructType],
                                configProperties: TypedProperties,
                                queryPaths: Seq[Path],
                                specifiedQueryInstant: Option[String] = None,
                                @transient fileStatusCache: FileStatusCache = NoopCache)
  extends BaseHoodieTableFileIndex(
    new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)),
    metaClient,
    configProperties,
    deduceQueryType(configProperties),
    queryPaths.asJava,
    toJavaOption(specifiedQueryInstant),
    false,
    false,
    SparkHoodieTableFileIndex.adapt(fileStatusCache)
  )
    with SparkAdapterSupport
    with Logging {

  /**
   * Get the schema of the table.
   */
  lazy val schema: StructType = schemaSpec.getOrElse({
    val schemaUtil = new TableSchemaResolver(metaClient)
    AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
  })

  private lazy val sparkParsePartitionUtil = sparkAdapter.createSparkParsePartitionUtil(spark.sessionState.conf)

  /**
   * Get the partition schema from the hoodie.properties.
   */
  private lazy val _partitionSchemaFromProperties: StructType = {
    val tableConfig = metaClient.getTableConfig
    val partitionColumns = tableConfig.getPartitionFields
    val nameFieldMap = generateFieldMap(schema)
    val isStringType = configProperties.getBoolean(PARTITION_FIELD_IS_STRING.key())
    val customPartitionFieldNames = configProperties.getString(PARTITION_FIELD_NAMES.key())
      .split(",")
      .filter(p => p.nonEmpty)
      .map(field => field.trim)
    if (partitionColumns.isPresent) {
      var partitionColumnsValue = partitionColumns.get()
      if (customPartitionFieldNames.length > 0) {
        if (partitionColumns.get().length != customPartitionFieldNames.length) {
          throw new IllegalArgumentException(s"Number of custom partition field names " +
            s"([${customPartitionFieldNames.mkString(",")})] doesn't match the number of partition columns")
        }
        partitionColumnsValue = customPartitionFieldNames
      }

      // Note that key generator class name could be null
      val keyGeneratorClassName = tableConfig.getKeyGeneratorClassName
      if (isStringType
        || classOf[TimestampBasedKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)
        || classOf[TimestampBasedAvroKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)) {
        val partitionFields = partitionColumns.get().map(column => StructField(column, StringType))
        StructType(partitionFields)
      } else {
        if (customPartitionFieldNames.length == 0) {
          val partitionFields = partitionColumns.get().map(column =>
            nameFieldMap.getOrElse(column, throw new IllegalArgumentException(s"Cannot find column: '" +
              s"$column' in the schema[${schema.fields.mkString(",")}]")))
          StructType(partitionFields)
        } else {
          val fields = ListBuffer[StructField]()
          for (idx <- customPartitionFieldNames.indices) {
            val customName = customPartitionFieldNames(idx)
            val orig = partitionColumns.get()(idx)
            val structField = nameFieldMap.getOrElse(orig, throw new IllegalArgumentException(s"Cannot find column: '" +
              s"$orig' in the schema[${schema.fields.mkString(",")}]"))
            fields += StructField(customName, structField.dataType, structField.nullable, structField.metadata)
          }
          StructType(fields)
        }

      }
    } else {
      // If the partition columns have not stored in hoodie.properties(the table that was
      // created earlier), we trait it as a non-partitioned table.
      logWarning("No partition columns available from hoodie.properties." +
        " Partition pruning will not work")
      new StructType()
    }
  }

  /**
   * Get the data schema of the table.
   *
   * @return
   */
  def dataSchema: StructType = {
    val partitionColumns = partitionSchema.fields.map(_.name).toSet
    StructType(schema.fields.filterNot(f => partitionColumns.contains(f.name)))
  }

  /**
   * @VisibleForTesting
   */
  def partitionSchema: StructType = {
    if (queryAsNonePartitionedTable) {
      // If we read it as Non-Partitioned table, we should not
      // return the partition schema.
      new StructType()
    } else {
      _partitionSchemaFromProperties
    }
  }

  /**
   * Fetch list of latest base files w/ corresponding log files, after performing
   * partition pruning
   *
   * TODO unify w/ HoodieFileIndex#listFiles
   *
   * @param partitionFilters partition column filters
   * @return mapping from string partition paths to its base/log files
   */
  def listFileSlices(partitionFilters: Seq[Expression]): Map[String, Seq[FileSlice]] = {
    // Prune the partition path by the partition filters
    val prunedPartitions = prunePartition(cachedAllInputFileSlices.keySet().asScala.toSeq, partitionFilters)
    prunedPartitions.map(partition => {
      (partition.path, cachedAllInputFileSlices.get(partition).asScala)
    }).toMap
  }

  /**
   * Get all the cached partition paths pruned by the filter.
   *
   * @param predicates The filter condition
   * @return The pruned partition paths
   */
  def getPartitionPaths(predicates: Seq[Expression]): Seq[PartitionPath] = {
    prunePartition(cachedAllInputFileSlices.keySet().asScala.toSeq, predicates)
  }

  /**
   * Prune the partition by the filter.This implementation is fork from
   * org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex#prunePartitions.
   *
   * @param partitionPaths All the partition paths.
   * @param predicates     The filter condition.
   * @return The pruned partition paths.
   */
  protected def prunePartition(partitionPaths: Seq[PartitionPath], predicates: Seq[Expression]): Seq[PartitionPath] = {
    val partitionColumnNames = partitionSchema.fields.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate(predicate.transform {
        case a: AttributeReference =>
          val index = partitionSchema.indexWhere(a.name == _.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
      })

      val prunedPartitionPaths = partitionPaths.filter {
        partitionPath => boundPredicate.eval(InternalRow.fromSeq(partitionPath.values))
      }

      logInfo(s"Total partition size is: ${partitionPaths.size}," +
        s" after partition prune size is: ${prunedPartitionPaths.size}")
      prunedPartitionPaths
    } else {
      partitionPaths
    }
  }

  protected def parsePartitionColumnValues(partitionColumns: Array[String], partitionPath: String): Array[Object] = {
    if (partitionColumns.length == 0) {
      // This is a non-partitioned table
      Array.empty
    } else {
      val partitionFragments = partitionPath.split("/")

      if (partitionFragments.length != partitionColumns.length &&
        partitionColumns.length == 1) {
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
        Array(UTF8String.fromString(partitionValue))
      } else if (partitionFragments.length != partitionColumns.length &&
        partitionColumns.length > 1) {
        // If the partition column size is not equal to the partition fragments size
        // and the partition column size > 1, we do not know how to map the partition
        // fragments to the partition columns. So we trait it as a Non-Partitioned Table
        // for the query which do not benefit from the partition prune.
        logWarning(s"Cannot do the partition prune for table $basePath." +
          s"The partitionFragments size (${partitionFragments.mkString(",")})" +
          s" is not equal to the partition columns size(${partitionColumns.mkString(",")})")
        Array.empty
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
        }.mkString("/")

        val pathWithPartitionName = new Path(basePath, partitionWithName)
        val partitionValues = parsePartitionPath(pathWithPartitionName, partitionSchema)

        partitionValues.map(_.asInstanceOf[Object]).toArray
      }
    }
  }

  private def parsePartitionPath(partitionPath: Path, partitionSchema: StructType): Seq[Any] = {
    val timeZoneId = configProperties.getString(DateTimeUtils.TIMEZONE_OPTION, SQLConf.get.sessionLocalTimeZone)
    val partitionDataTypes = partitionSchema.map(f => f.name -> f.dataType).toMap

    sparkParsePartitionUtil.parsePartition(
      partitionPath,
      typeInference = false,
      Set(new Path(basePath)),
      partitionDataTypes,
      DateTimeUtils.getTimeZone(timeZoneId)
    )
      .toSeq(partitionSchema)
  }
}

object SparkHoodieTableFileIndex {

  implicit def toJavaOption[T](opt: Option[T]): org.apache.hudi.common.util.Option[T] =
    if (opt.isDefined) {
      org.apache.hudi.common.util.Option.of(opt.get)
    } else {
      org.apache.hudi.common.util.Option.empty()
    }

  /**
   * This method unravels [[StructType]] into a [[Map]] of pairs of dot-path notation with corresponding
   * [[StructField]] object for every field of the provided [[StructType]], recursively.
   *
   * For example, following struct
   * <pre>
   *   StructType(
   *     StructField("a",
   *       StructType(
   *          StructField("b", StringType),
   *          StructField("c", IntType)
   *       )
   *     )
   *   )
   * </pre>
   *
   * will be converted into following mapping:
   *
   * <pre>
   *   "a.b" -> StructField("b", StringType),
   *   "a.c" -> StructField("c", IntType),
   * </pre>
   */
  private def generateFieldMap(structType: StructType) : Map[String, StructField] = {
    def traverse(structField: Either[StructField, StructType]) : Map[String, StructField] = {
      structField match {
        case Right(struct) => struct.fields.flatMap(f => traverse(Left(f))).toMap
        case Left(field) => field.dataType match {
          case struct: StructType => traverse(Right(struct)).map {
            case (key, structField)  => (s"${field.name}.$key", structField)
          }
          case _ => Map(field.name -> field)
        }
      }
    }

    traverse(Right(structType))
  }

  private def deduceQueryType(configProperties: TypedProperties): HoodieTableQueryType = {
    configProperties.asScala.getOrElse(QUERY_TYPE.key, QUERY_TYPE.defaultValue) match {
      case QUERY_TYPE_SNAPSHOT_OPT_VAL => HoodieTableQueryType.SNAPSHOT
      case QUERY_TYPE_INCREMENTAL_OPT_VAL => HoodieTableQueryType.INCREMENTAL
      case QUERY_TYPE_READ_OPTIMIZED_OPT_VAL => HoodieTableQueryType.READ_OPTIMIZED
      case _ @ qt => throw new IllegalArgumentException(s"query-type ($qt) not supported")
    }
  }

  private def adapt(cache: FileStatusCache): BaseHoodieTableFileIndex.FileStatusCache = {
    new BaseHoodieTableFileIndex.FileStatusCache {
      override def get(path: Path): org.apache.hudi.common.util.Option[Array[FileStatus]] = toJavaOption(cache.getLeafFiles(path))
      override def put(path: Path, leafFiles: Array[FileStatus]): Unit = cache.putLeafFiles(path, leafFiles)
      override def invalidate(): Unit = cache.invalidateAll()
    }
  }
}

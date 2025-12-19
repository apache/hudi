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

import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.SparkHoodieTableFileIndex.{deduceQueryType, extractEqualityPredicatesLiteralValues, generateFieldMap, haveProperPartitionValues, shouldListLazily, shouldUsePartitionPathPrefixAnalysis, shouldValidatePartitionColumns}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.{FileSlice, HoodieTableQueryType}
import org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.ReflectionUtils
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.HoodieBootstrapConfig.DATA_QUERIES_ONLY
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.schema.Types.RecordType
import org.apache.hudi.internal.schema.utils.Conversions
import org.apache.hudi.keygen.{StringPartitionPathFormatter, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BasePredicate, BoundReference, EmptyRow, EqualTo, Expression, InterpretedPredicate, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{FileStatusCache, NoopCache}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, DateType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import javax.annotation.concurrent.NotThreadSafe

import java.lang.reflect.{Array => JArray}
import java.util.Collections

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.{Success, Try}

/**
 * Implementation of the [[BaseHoodieTableFileIndex]] for Spark
 *
 * @param spark                                           spark session
 * @param metaClient                                      Hudi table's meta-client
 * @param schemaSpec                                      optional table's schema
 * @param configProperties                                unifying configuration (in the form of generic properties)
 * @param specifiedQueryInstant                           instant as of which table is being queried
 * @param fileStatusCache                                 transient cache of fetched [[FileStatus]]es
 * @param startCompletionTime                             start completion time for incremental and CDC queries
 * @param endCompletionTime                               end completion time for incremental and CDC queries
 * @param shouldUseStringTypeForTimestampPartitionKeyType should use String type for timestamp partition key type
 */
@NotThreadSafe
class SparkHoodieTableFileIndex(spark: SparkSession,
                                metaClient: HoodieTableMetaClient,
                                schemaSpec: Option[StructType],
                                configProperties: TypedProperties,
                                queryPaths: Seq[StoragePath],
                                specifiedQueryInstant: Option[String] = None,
                                @transient fileStatusCache: FileStatusCache = NoopCache,
                                startCompletionTime: Option[String] = None,
                                endCompletionTime: Option[String] = None)
  extends BaseHoodieTableFileIndex(
    new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)),
    metaClient,
    configProperties,
    deduceQueryType(configProperties),
    queryPaths.asJava,
    toJavaOption(specifiedQueryInstant),
    false,
    false,
    SparkHoodieTableFileIndex.adapt(fileStatusCache),
    shouldListLazily(configProperties),
    toJavaOption(startCompletionTime),
    toJavaOption(endCompletionTime)
  )
    with SparkAdapterSupport
    with Logging {

  /**
   * Get the schema of the table.
   */
  lazy val schema: StructType = if (shouldFastBootstrap) {
      StructType(rawStructSchema.fields.filterNot(f => HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name)))
    } else {
      rawStructSchema
    }

  lazy val rawHoodieSchema: HoodieSchema = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    schemaUtil.getTableSchema
  }

  private lazy val rawStructSchema: StructType = schemaSpec.getOrElse {
    HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(rawHoodieSchema)
  }

  protected lazy val shouldFastBootstrap = configProperties.getBoolean(DATA_QUERIES_ONLY.key, false)

  /**
   * Get the partition schema from the hoodie.properties.
   */
  private lazy val _partitionSchemaFromProperties: StructType = {
    val tableConfig = metaClient.getTableConfig
    val partitionColumns = tableConfig.getPartitionFields
    val nameFieldMap = generateFieldMap(schema)

    if (partitionColumns.isPresent) {
      // Note that key generator class name could be null
      val keyGeneratorClassName = tableConfig.getKeyGeneratorClassName
      if (classOf[TimestampBasedKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)
        || classOf[TimestampBasedAvroKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)) {
        val partitionFields: Array[StructField] = partitionColumns.get().map(column => StructField(column, StringType))
        StructType(partitionFields)
      } else {
        val partitionFields: Array[StructField] = partitionColumns.get().filter(column => nameFieldMap.contains(column))
          .map(column => nameFieldMap.apply(column))

        if (partitionFields.length != partitionColumns.get().length) {
          val isBootstrapTable = tableConfig.getBootstrapBasePath.isPresent
          if (isBootstrapTable) {
            // For bootstrapped tables its possible the schema does not contain partition field when source table
            // is hive style partitioned. In this case we would like to treat the table as non-partitioned
            // as opposed to failing
            new StructType()
          } else {
            throw new IllegalArgumentException(s"Cannot find columns: " +
              s"'${partitionColumns.get().filter(col => !nameFieldMap.contains(col)).mkString(",")}' " +
              s"in the schema[${schema.fields.mkString(",")}]")
          }
        } else {
          new StructType(partitionFields)
        }
      }
    } else {
      // If the partition columns have not stored in hoodie.properties(the table that was
      // created earlier), we trait it as a non-partitioned table.
      logDebug("No partition columns available from hoodie.properties." +
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
    val partitionColumns = partitionSchema.fieldNames
    StructType(schema.fields.filterNot(f => partitionColumns.contains(f.name)))
  }

  /**
   * @VisibleForTesting
   */
  def partitionSchema: StructType = {
    if (!shouldReadAsPartitionedTable) {
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
    val prunedPartitions = listMatchingPartitionPaths(partitionFilters)
    getInputFileSlices(prunedPartitions: _*).asScala.map {
      case (partition, fileSlices) => (partition.path, fileSlices.asScala.toSeq)
    }.toMap
  }

  /**
   * Get all the cached partition paths pruned by the filter.
   *
   * @param predicates The filter condition
   * @return The pruned partition paths
   */
  def getPartitionPaths(predicates: Seq[Expression]): Seq[PartitionPath] = {
    listMatchingPartitionPaths(predicates)
  }

  /**
   * Prune the partition by the filter.This implementation is fork from
   * org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex#prunePartitions.
   *
   * @param predicates The filter condition.
   * @return The pruned partition paths.
   */
  def listMatchingPartitionPaths(predicates: Seq[Expression]): Seq[PartitionPath] = {
    val resolve = spark.sessionState.analyzer.resolver
    val partitionColumnNames = getPartitionColumns
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).forall { ref =>
        // NOTE: We're leveraging Spark's resolver here to appropriately handle case-sensitivity
        partitionColumnNames.exists(partCol => resolve(ref, partCol))
      }
    }

    if (partitionPruningPredicates.isEmpty) {
      val queryPartitionPaths = getAllQueryPartitionPaths.asScala.toSeq
      logInfo(s"No partition predicates provided, listing full table (${queryPartitionPaths.size} partitions)")
      queryPartitionPaths
    } else {
      // NOTE: We fallback to already cached partition-paths only in cases when we can subsequently
      //       rely on partition-pruning to eliminate not matching provided predicates (that requires
      //       partition-values to be successfully recovered from the partition-paths)
      val partitionPaths = if (areAllPartitionPathsCached && haveProperPartitionValues(getAllQueryPartitionPaths.asScala.toSeq)) {
        logDebug("All partition paths have already been cached, using these directly")
        getAllQueryPartitionPaths.asScala
      } else if (!shouldUsePartitionPathPrefixAnalysis(configProperties)) {
        logInfo("Partition path prefix analysis is disabled; falling back to fetching all partitions")
        getAllQueryPartitionPaths.asScala
      } else {
        tryPushDownPartitionPredicates(partitionColumnNames, partitionPruningPredicates)
      }

      // NOTE: In some cases, like for ex, when non-encoded slash '/' is used w/in the partition column's value,
      //       we might not be able to properly parse partition-values from the listed partition-paths.
      //       In that case, we simply could not apply partition pruning and will have to regress to scanning
      //       the whole table
      if (haveProperPartitionValues(partitionPaths.toSeq) && partitionSchema.nonEmpty) {
        val predicate = partitionPruningPredicates.reduce(expressions.And)
        val transformedPredicate = predicate.transform {
          case a: AttributeReference =>
            val index = partitionSchema.indexWhere(a.name == _.name)
            BoundReference(index, partitionSchema(index).dataType, nullable = true)
        }
        val boundPredicate: BasePredicate = try {
          // Try using 1-arg constructor via reflection
          val clazz = Class.forName("org.apache.spark.sql.catalyst.expressions.InterpretedPredicate")
          val ctor = clazz.getConstructor(classOf[Expression])
          ctor.newInstance(transformedPredicate).asInstanceOf[BasePredicate]
        } catch {
          case _: NoSuchMethodException | _: IllegalArgumentException =>
            // Fallback: Try using 2-arg constructor for certain Spark runtime
            val clazz = Class.forName("org.apache.spark.sql.catalyst.expressions.InterpretedPredicate")
            val ctor = clazz.getConstructor(classOf[Expression], classOf[Boolean])
            ctor.newInstance(transformedPredicate, java.lang.Boolean.FALSE)
              .asInstanceOf[BasePredicate]
        }
        val prunedPartitionPaths = partitionPaths.filter {
          partitionPath => boundPredicate.eval(InternalRow.fromSeq(partitionPath.values))
        }.toSeq

        logInfo(s"Using provided predicates to prune number of target table's partitions scanned from" +
          s" ${partitionPaths.size} to ${prunedPartitionPaths.size}")

        prunedPartitionPaths
      } else {
        logWarning(s"Unable to apply partition pruning, due to failure to parse partition values from the" +
          s" following path(s): ${partitionPaths.find(_.values.length == 0).map(e => e.getPath)}")

        partitionPaths.toSeq
      }
    }
  }

  // NOTE: Here we try to to achieve efficiency in avoiding necessity to recursively list deep folder structures of
  //       partitioned tables w/ multiple partition columns, by carefully analyzing provided partition predicates:
  //
  // 1. Firstly, when partition-predicates have
  //         - The form of equality predicates w/ static literals (for ex, like `date = '2022-01-01'`)
  //         - Fully specified proper prefix of the partition schema (ie fully binding first N columns
  //           of the partition schema adhering to hereby described rules)
  //
  // We will try to exploit this specific structure, and try to reduce the scope of a
  // necessary file-listings of partitions of the table to just the sub-folder under relative prefix
  // of the partition-path derived from the partition-column predicates. For ex, consider following
  // scenario:
  //
  // Table's partition schema (in-order):
  //
  //    country_code: string (for ex, 'us')
  //    date: string (for ex, '2022-01-01')
  //    hour: string (for ex, '08')
  //
  // Table's folder structure:
  //    us/
  //     |- 2022-01-01/06
  //     |- 2022-01-02/07
  //     ...
  //
  // In case we have incoming query specifies following predicates:
  //
  //    `... WHERE country_code = 'us' AND date = '2022-01-01'`
  //
  // We can deduce full partition-path w/o doing a single listing: `us/2022-01-01`
  //
  // 2. Try to push down all partition predicates when listing the sub-folder.
  // In case we have incoming query specifies following predicates:
  //
  //    `... WHERE country_code = 'us' AND date = '2022-01-01' and hour = '06'`
  //
  // We can deduce full partition-path w/o doing a single listing: `us/2022-01-01`, and then push down
  // these filters when listing `us/2022-01-01` to get the directory 'us/2022-01-01/06'
  private def tryPushDownPartitionPredicates(partitionColumnNames: Seq[String], partitionColumnPredicates: Seq[Expression]): Seq[PartitionPath] = {
    // Static partition-path prefix is defined as a prefix of the full partition-path where only
    // first N partition columns (in-order) have proper (static) values bound in equality predicates,
    // allowing in turn to build such prefix to be used in subsequent filtering
    val staticPartitionColumnNameValuePairs: Seq[(String, (String, Any))] = {
      // Extract from simple predicates of the form `date = '2022-01-01'` both
      // partition column and corresponding (literal) value
      val zoneId = configProperties.getString(DateTimeUtils.TIMEZONE_OPTION, SQLConf.get.sessionLocalTimeZone)
      val staticPartitionColumnValuesMap = extractEqualityPredicatesLiteralValues(partitionColumnPredicates, zoneId)
      // NOTE: For our purposes we can only construct partition-path prefix if proper prefix of the
      //       partition-schema has been bound by the partition-predicates
      partitionColumnNames.takeWhile(colName => staticPartitionColumnValuesMap.contains(colName))
        .map(colName => (colName, (staticPartitionColumnValuesMap(colName)._1, staticPartitionColumnValuesMap(colName)._2.get)))
    }

    val hiveStylePartitioning = metaClient.getTableConfig.getHiveStylePartitioningEnable.toBoolean
    val urlEncodePartitioning = metaClient.getTableConfig.getUrlEncodePartitioning.toBoolean

    val partitionTypesOption =if (hiveStylePartitioning && urlEncodePartitioning) {
      Try {
        SparkFilterHelper.convertDataType(partitionSchema).asInstanceOf[RecordType]
      } match {
        case Success(partitionRecordType)
          if partitionRecordType.fields().size() == _partitionSchemaFromProperties.size
            && Conversions.isPartitionSchemaSupportedConversion(partitionRecordType) =>
          Some(partitionRecordType)
        case _ =>
          None
      }
    } else {
      // Avoid convert partition schemas if hivestylePartitioning & urlEncodePartitioning is not enabled.
      None
    }

    (staticPartitionColumnNameValuePairs.isEmpty, partitionTypesOption) match {
      case (true, Some(partitionTypes)) =>
        // Push down partition filters without pathPrefix
        val convertedFilters = SparkFilterHelper.convertFilters(
          partitionColumnPredicates.flatMap {
            expr => sparkAdapter.translateFilter(expr)
          })
        listPartitionPaths(Collections.singletonList(""), partitionTypes, convertedFilters).asScala.toSeq
      case (true, None) =>
        logDebug("Unable to compose relative partition path prefix from the predicates; falling back to fetching all partitions")
        getAllQueryPartitionPaths.asScala.toSeq
      case (false, _) =>
        // Based on the static partition-column name-value pairs, we'll try to compose static partition-path
        // prefix to try to reduce the scope of the required file-listing
        val relativePartitionPathPrefix = composeRelativePartitionPath(staticPartitionColumnNameValuePairs)

        if (!metaClient.getStorage.exists(new StoragePath(getBasePath, relativePartitionPathPrefix))) {
          Seq()
        } else if (staticPartitionColumnNameValuePairs.length == partitionColumnNames.length) {
          // In case composed partition path is complete, we can return it directly avoiding extra listing operation
          Seq(new PartitionPath(relativePartitionPathPrefix, staticPartitionColumnNameValuePairs.map(_._2._2.asInstanceOf[AnyRef]).toArray))
        } else {
          partitionTypesOption.map { partitionTypes =>
            // Try to composite path prefix and filters to gain better performance
            val convertedFilters = SparkFilterHelper.convertFilters(
              partitionColumnPredicates.flatMap {
                expr => sparkAdapter.translateFilter(expr)
              })
            listPartitionPaths(Seq(relativePartitionPathPrefix).asJava, partitionTypes, convertedFilters).asScala.toSeq
          }.getOrElse {
            log.warn("Met incompatible issue when converting to hudi data type, rollback to list by prefix directly")
            listPartitionPaths(Seq(relativePartitionPathPrefix).asJava).asScala.toSeq
          }
        }
    }
  }

  /**
   * Construct relative partition path (i.e., partition prefix) from the given partition values
   *
   * @return relative partition path and a flag to indicate if the path is complete (i.e., not a prefix)
   */
  private def composeRelativePartitionPath(staticPartitionColumnNameValuePairs: Seq[(String, (String, Any))]): String = {
    checkState(staticPartitionColumnNameValuePairs.nonEmpty)

    // Since static partition values might not be available for all columns, we compile
    // a list of corresponding pairs of (partition column-name, corresponding value) if available
    val (staticPartitionColumnNames, staticPartitionColumnValues) = staticPartitionColumnNameValuePairs.unzip

    val hiveStylePartitioning = metaClient.getTableConfig.getHiveStylePartitioningEnable.toBoolean

    val partitionPathFormatter = new StringPartitionPathFormatter(
      JFunction.toJavaSupplier(() => new StringPartitionPathFormatter.JavaStringBuilder()),
      hiveStylePartitioning,
      arePartitionPathsUrlEncoded
    )

    partitionPathFormatter.combine(staticPartitionColumnNames.asJava,
      staticPartitionColumnValues.map(_._1): _*)
  }

  /**
   * @VisibleForTesting
   */
  def parsePartitionColumnValues(partitionColumns: Array[String], partitionPath: String): Array[Object] = {
    HoodieSparkUtils.parsePartitionColumnValues(
      partitionColumns,
      partitionPath,
      getBasePath,
      schema,
      metaClient.getTableConfig.propsMap,
      configProperties.getString(DateTimeUtils.TIMEZONE_OPTION, SQLConf.get.sessionLocalTimeZone),
      shouldValidatePartitionColumns(spark))
  }

  private def arePartitionPathsUrlEncoded: Boolean =
    metaClient.getTableConfig.getUrlEncodePartitioning.toBoolean
}

object SparkHoodieTableFileIndex extends SparkAdapterSupport {
  private val LOG = LoggerFactory.getLogger(classOf[SparkHoodieTableFileIndex])
  private val PUT_LEAF_FILES_METHOD_NAME = "putLeafFiles"

  private def haveProperPartitionValues(partitionPaths: Seq[PartitionPath]) = {
    partitionPaths.forall(_.values.length > 0)
  }

  private def extractEqualityPredicatesLiteralValues(predicates: Seq[Expression], zoneId: String): Map[String, (String, Option[Any])] = {
    // TODO support coercible expressions (ie attr-references casted to particular type), similar
    //      to `MERGE INTO` statement

    object ExtractableLiteral {
      def unapply(exp: Expression): Option[String] = exp match {
        case Literal(null, _) => None // `null`s can be cast as other types; we want to avoid NPEs.
        case Literal(value, _: ByteType | IntegerType | LongType | ShortType) => Some(value.toString)
        case Literal(value, _: StringType) => Some(value.toString)
        case Literal(value, _: DateType) =>
          Some(sparkAdapter.getDateFormatter(DateTimeUtils.getTimeZone(zoneId)).format(value.asInstanceOf[Int]))
        case _ => None
      }
    }

    // NOTE: To properly support predicates of the form `x = NULL`, we have to wrap result
    //       of the folded expression into [[Some]] (to distinguish it from the case when partition-column
    //       isn't bound to any value by the predicate)
    predicates.flatMap {
      case EqualTo(attr: AttributeReference, e @ ExtractableLiteral(valueString)) =>
        Seq((attr.name, (valueString, Some(e.eval(EmptyRow)))))
      case EqualTo(e @ ExtractableLiteral(valueString), attr: AttributeReference) =>
        Seq((attr.name, (valueString, Some(e.eval(EmptyRow)))))
      case _ => Seq.empty
    }.toMap
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
    // Certain Spark runtime like Databricks Spark has changed the FileStatusCache APIs
    // so we need to check the API to avoid NoSuchMethodError
    if (ReflectionUtils.getMethod(
      classOf[FileStatusCache], PUT_LEAF_FILES_METHOD_NAME, classOf[Path],
      JArray.newInstance(classOf[FileStatus], 0).getClass).isPresent) {
      new BaseHoodieTableFileIndex.FileStatusCache {
        override def get(path: StoragePath): org.apache.hudi.common.util.Option[java.util.List[StoragePathInfo]] =
          toJavaOption(cache.getLeafFiles(new Path(path.toUri)).map(opt => opt.map(
            e => HadoopFSUtils.convertToStoragePathInfo(e)).toList.asJava
          ))

        override def put(path: StoragePath, leafFiles: java.util.List[StoragePathInfo]): Unit =
          cache.putLeafFiles(new Path(path.toUri), leafFiles.asScala.map(e => new FileStatus(
            e.getLength, e.isDirectory, 0, e.getBlockSize, e.getModificationTime, new Path(e.getPath.toUri))).toArray)

        override def invalidate(): Unit = cache.invalidateAll()
      }
    } else {
      LOG.warn("Use no-op file status cache instead because the FileStatusCache APIs at runtime "
        + "are different from open-source Spark")
      new BaseHoodieTableFileIndex.NoopCache
    }
  }

  private def shouldValidatePartitionColumns(spark: SparkSession): Boolean = {
    // NOTE: We can't use helper, method nor the config-entry to stay compatible w/ Spark 2.4
    spark.sessionState.conf.getConfString("spark.sql.sources.validatePartitionColumns", "true").toBoolean
  }

  private def shouldListLazily(props: TypedProperties): Boolean = {
    props.getString(DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key,
      DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.defaultValue) == FILE_INDEX_LISTING_MODE_LAZY
  }

  private def shouldUsePartitionPathPrefixAnalysis(props: TypedProperties): Boolean = {
    props.getBoolean(DataSourceReadOptions.FILE_INDEX_LISTING_PARTITION_PATH_PREFIX_ANALYSIS_ENABLED.key,
      DataSourceReadOptions.FILE_INDEX_LISTING_PARTITION_PATH_PREFIX_ANALYSIS_ENABLED.defaultValue)
  }
}

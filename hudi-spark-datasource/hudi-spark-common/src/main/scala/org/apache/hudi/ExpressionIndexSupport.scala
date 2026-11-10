/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.ColumnStatsIndexSupport.{composeColumnStatStructType, extractWrapperValueV1}
import org.apache.hudi.ExpressionIndexSupport._
import org.apache.hudi.HoodieCatalystUtils.{withPersistedData, withPersistedDataset}
import org.apache.hudi.HoodieConversionUtils.{toJavaOption, toScalaOption}
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.{HoodieData, HoodieListData}
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection
import org.apache.hudi.core.index.expression.HoodieExpressionIndex
import org.apache.hudi.core.read.BaseHoodieTableFileIndex
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.metadata.{ColumnStatsIndexPrefixRawKey, HoodieMetadataPayload, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.metadata.stats.{SparkValueMetadataUtils, ValueMetadata, ValueType}
import org.apache.hudi.metadata.stats.ValueMetadata.getValueMetadata
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoExpressionIndexFilterExpr
import org.apache.spark.sql.hudi.dataskipping.{ExpressionIndexSource, IndexedExpression, Leaf, Op}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashMap

class ExpressionIndexSupport(spark: SparkSession,
                             tableSchema: StructType,
                             metadataConfig: HoodieMetadataConfig,
                             metaClient: HoodieTableMetaClient,
                             allowCaching: Boolean = false)
  extends SparkBaseIndexSupport (spark, metadataConfig, metaClient) with SparkAdapterSupport {

  @transient private lazy val cachedExpressionIndexViews: ParHashMap[Seq[String], DataFrame] = ParHashMap()


  // NOTE: Since [[metadataConfig]] is transient this has to be eagerly persisted, before this will be passed on to the executor
  private val inMemoryProjectionThreshold = metadataConfig.getColumnStatsIndexInMemoryProjectionThreshold

  override def getIndexName: String = ExpressionIndexSupport.INDEX_NAME

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    if (!isIndexAvailable || queryFilters.isEmpty) {
      Option.empty
    } else {
      lazy val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
      lazy val (prunedPartitions, prunedFileNames) = getPrunedPartitionsAndFileNames(fileIndex, prunedPartitionsAndFileSlices)
      lazy val prunedPartitionAndFileNames = getPrunedPartitionsAndFileNamesMap(prunedPartitionsAndFileSlices, includeLogFiles = true)
      // Each expression index a filter term matches prunes on its own; a file must pass all of them
      findIndexUses(queryFilters).flatMap { use =>
        val indexName = use.definition.getIndexName
        use.definition.getIndexType match {
          case HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS =>
            val expressionIndexRecords = loadExpressionIndexRecords(indexName, prunedPartitions, readInMemory)
            loadTransposed(Seq(indexName, "files"), use.indexed.sourceColumn, use.statsType, readInMemory, expressionIndexRecords) {
              transposedColStatsDF => Some(getCandidateFiles(transposedColStatsDF, use.conjuncts, prunedFileNames,
                _ => Seq(use.indexed.sourceColumn), Some(ExpressionIndexSource(use.indexed)), Option.apply(use.definition)))
            }
          case HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS =>
            val keys = bloomFilterKeys(use)
            if (keys.isEmpty) None else Some(getCandidateFilesForKeys(indexName, prunedPartitionAndFileNames, keys))
          case _ => None
        }
      }.reduceOption(_ intersect _)
    }
  }

  def prunePartitions(fileIndex: HoodieFileIndex,
                      queryFilters: Seq[Expression],
                      queryReferencedColumns: Seq[String]): Option[Set[String]] = {
    if (!isIndexAvailable || queryFilters.isEmpty) {
      Option.empty
    } else {
      lazy val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
      findIndexUses(queryFilters)
        .filter(_.definition.getIndexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS))
        .flatMap { use =>
          val expressionIndexRecords = loadExpressionIndexPartitionStatRecords(use.definition, readInMemory)
          loadTransposed(Seq(use.definition.getIndexName, "partitions"), use.indexed.sourceColumn, use.statsType, readInMemory,
            expressionIndexRecords) {
            transposedPartitionStatsDF => {
              try {
                transposedPartitionStatsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
                val allPartitions = transposedPartitionStatsDF.select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
                  .collect()
                  .map(_.getString(0))
                  .toSet
                if (allPartitions.nonEmpty) {
                  // NOTE: a leaf whose term the index holds no stats for compiles to TRUE, so it prunes no partition
                  val source = ExpressionIndexSource(use.indexed)
                  val indexFilter = use.conjuncts.map(translateIntoExpressionIndexFilterExpr(_, source)).reduce(And)
                  Some(transposedPartitionStatsDF.where(sparkAdapter.createColumnFromExpression(indexFilter))
                    .select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
                    .collect()
                    .map(_.getString(0))
                    .toSet)
                } else {
                  // Partition stats do not exist, skip the pruning
                  Option.empty
                }
              } finally {
                transposedPartitionStatsDF.unpersist()
              }
            }
          }
        }.reduceOption(_ intersect _)
    }
  }

  /**
   * Loads view of the Column Stats Index in a transposed format where single row coalesces every columns'
   * statistics for a single file, returning it as [[DataFrame]]
   *
   * Please check out scala-doc of the [[transpose]] method explaining this view in more details
   */
  def loadTransposed[T](cacheKey: Seq[String],
                        sourceColumn: String,
                        statsType: DataType,
                        shouldReadInMemory: Boolean,
                        colStatRecords: HoodieData[HoodieMetadataColumnStats])(block: DataFrame => T): T = {
    cachedExpressionIndexViews.get(cacheKey) match {
      case Some(cachedDF) =>
        block(cachedDF)
      case None =>
        val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = colStatRecords
        withPersistedData(colStatsRecords, StorageLevel.MEMORY_ONLY) {
          val (transposedRows, indexSchema) = transpose(colStatsRecords, Seq(sourceColumn), statsType)
          val df = if (shouldReadInMemory) {
            // NOTE: This will instantiate a [[Dataset]] backed by [[LocalRelation]] holding all of the rows
            //       of the transposed table in memory, facilitating execution of the subsequently chained operations
            //       on it locally (on the driver; all such operations are actually going to be performed by Spark's
            //       Optimizer)
            sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, transposedRows.collectAsList().asScala.toSeq, indexSchema)
          } else {
            val rdd = HoodieJavaRDD.getJavaRDD(transposedRows)
            spark.createDataFrame(rdd, indexSchema)
          }

          if (allowCaching) {
            cachedExpressionIndexViews.put(cacheKey, df)
            // NOTE: Instead of collecting the rows from the index and hold them in memory, we instead rely
            //       on Spark as (potentially distributed) cache managing data lifecycle, while we simply keep
            //       the referenced to persisted [[DataFrame]] instance
            df.persist(StorageLevel.MEMORY_ONLY)

            block(df)
          } else {
            withPersistedDataset(df) {
              block(df)
            }
          }
        }
    }
  }

  /**
   * Transposes and converts the raw table format of the Column Stats Index representation,
   * where each row/record corresponds to individual (column, file) pair, into the table format
   * where each row corresponds to single file with statistic for individual columns collated
   * w/in such row:
   *
   * Metadata Table Column Stats Index format:
   *
   * <pre>
   *  +---------------------------+------------+------------+------------+-------------+
   *  |        fileName           | columnName |  minValue  |  maxValue  |  num_nulls  |
   *  +---------------------------+------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          A |          1 |         10 |           0 |
   *  | another_base_file.parquet |          A |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+------------+-------------+
   * </pre>
   *
   * Returned table format
   *
   * <pre>
   *  +---------------------------+------------+------------+-------------+
   *  |          file             | A_minValue | A_maxValue | A_nullCount |
   *  +---------------------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          1 |         10 |           0 |
   *  | another_base_file.parquet |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+-------------+
   * </pre>
   *
   * NOTE: Column Stats Index might potentially contain statistics for many columns (if not all), while
   *       query at hand might only be referencing a handful of those. As such, we collect all the
   *       column references from the filtering expressions, and only transpose records corresponding to the
   *       columns referenced in those
   *
   * @param colStatsRecords [[HoodieData[HoodieMetadataColumnStats]]] bearing raw Column Stats Index records
   * @param queryColumns target columns to be included into the final table
   * @return reshaped table according to the format outlined above
   */
  private def transpose(colStatsRecords: HoodieData[HoodieMetadataColumnStats], queryColumns: Seq[String], statsType: DataType): (HoodieData[Row], StructType) = {
    val tableSchemaFieldMap = tableSchema.fields.map(f => (f.name, f)).toMap
    // NOTE: We're sorting the columns to make sure final index schema matches layout
    //       of the transposed table
    val sortedTargetColumnsSet = TreeSet(queryColumns:_*)

    // NOTE: This is a trick to avoid pulling all of [[ColumnStatsIndexSupport]] object into the lambdas'
    //       closures below
    val indexedColumns = queryColumns.toSet

    // NOTE: It's crucial to maintain appropriate ordering of the columns
    //       matching table layout: hence, we cherry-pick individual columns
    //       instead of simply filtering in the ones we're interested in the schema
    val (indexSchema, targetIndexedColumns) = composeIndexSchema(sortedTargetColumnsSet.toSeq, indexedColumns, tableSchema, statsType)

    // Here we perform complex transformation which requires us to modify the layout of the rows
    // of the dataset, and therefore we rely on low-level RDD API to avoid incurring encoding/decoding
    // penalty of the [[Dataset]], since it's required to adhere to its schema at all times, while
    // RDDs are not;
    val useJava8api = spark.sessionState.conf.datetimeJava8ApiEnabled
    val transposedRows: HoodieData[Row] = colStatsRecords
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      .filter(JFunction.toJavaSerializableFunction(r => sortedTargetColumnsSet.contains(r.getColumnName)))
      .mapToPair(JFunction.toJavaSerializableFunctionPairOut(r => {
        if (r.getMinValue == null && r.getMaxValue == null) {
          // Corresponding row could be null in either of the 2 cases
          //    - Column contains only null values (in that case both min/max have to be nulls)
          //    - This is a stubbed Column Stats record (used as a tombstone)
          collection.Pair.of(r.getFileName, r)
        } else {
          val minValueWrapper = r.getMinValue
          val maxValueWrapper = r.getMaxValue

          checkState(minValueWrapper != null && maxValueWrapper != null, "Invalid Column Stats record: either both min/max have to be null, or both have to be non-null")

          val colName = r.getColumnName
          val colType = tableSchemaFieldMap(colName).dataType

          val valueMetadata = getValueMetadata(r.getValueType)
          val minValue = extractExpressionIndexValue(minValueWrapper, colType, valueMetadata, useJava8api)
          val maxValue = extractExpressionIndexValue(maxValueWrapper, colType, valueMetadata, useJava8api)

          // Update min-/max-value structs w/ unwrapped values in-place
          r.setMinValue(minValue)
          r.setMaxValue(maxValue)

          collection.Pair.of(r.getFileName, r)
        }
      }))
      .groupByKey()
      .map(JFunction.toJavaSerializableFunction(p => {
        val columnRecordsSeq: Seq[HoodieMetadataColumnStats] = p.getValue.asScala.toSeq
        val fileName: String = p.getKey
        val valueCount: Long = columnRecordsSeq.head.getValueCount

        // To properly align individual rows (corresponding to a file) w/in the transposed projection, we need
        // to align existing column-stats for individual file with the list of expected ones for the
        // whole transposed projection (a superset of all files)
        val columnRecordsMap = columnRecordsSeq.map(r => (r.getColumnName, r)).toMap
        val alignedColStatRecordsSeq = targetIndexedColumns.map(columnRecordsMap.get)

        val coalescedRowValuesSeq =
          alignedColStatRecordsSeq.foldLeft(ListBuffer[Any](fileName, valueCount)) {
            case (acc, opt) =>
              opt match {
                case Some(colStatRecord) =>
                  acc ++= Seq(colStatRecord.getMinValue, colStatRecord.getMaxValue, colStatRecord.getNullCount)
                case None =>
                  // NOTE: This could occur in either of the following cases:
                  //    1. When certain columns exist in the schema but are absent in some data files due to
                  //       schema evolution or other reasons, these columns will not be present in the column stats.
                  //       In this case, we fill in default values by setting the min, max and null-count to null
                  //       (this behavior is consistent with reading non-existent columns from Parquet).
                  //    2. When certain columns are present both in the schema and the data files,
                  //       but the column stats are absent for these columns due to their types not supporting indexing,
                  //       we also set these columns to default values.
                  //
                  // This approach prevents errors during data skipping and, because the filter includes an isNull check,
                  // these conditions will not affect the accurate return of files from data skipping.
                  acc ++= Seq(null, null, null)
              }
          }

        Row(coalescedRowValuesSeq.toSeq: _*)
      }))

    (transposedRows, indexSchema)
  }

  /**
   * @param statsType type of the stored min / max values: the indexed expression's type, for ex INT for `hour(ts)`
   */
  def composeIndexSchema(targetColumnNames: Seq[String], indexedColumns: Set[String], tableSchema: StructType, statsType: DataType): (StructType, Seq[String]) = {
    val fileNameField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME, StringType, nullable = true, Metadata.empty)
    val valueCountField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT, LongType, nullable = true, Metadata.empty)

    val targetIndexedColumns = targetColumnNames.filter(indexedColumns.contains(_))
    val targetIndexedFields = targetIndexedColumns.map(colName => tableSchema.fields.find(f => f.name == colName).get)

    (StructType(
      targetIndexedFields.foldLeft(Seq(fileNameField, valueCountField)) {
        case (acc, field) =>
          acc ++ Seq(
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE, statsType),
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE, statsType),
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT, LongType))
      }
    ), targetIndexedColumns)
  }

  def loadColumnStatsIndexRecords(targetColumns: Seq[String], prunedPartitions: Option[Set[String]] = None, shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    // Read Metadata Table's Column Stats Index records into [[HoodieData]] container by
    //    - Fetching the records from CSI by key-prefixes (encoded column names)
    //    - Extracting [[HoodieMetadataColumnStats]] records
    //    - Filtering out nulls
    checkState(targetColumns.nonEmpty)

    // Create raw key prefixes based on column names and optional partition names
    val rawKeys = if (prunedPartitions.isDefined) {
      val partitionsList = prunedPartitions.get.toList
      targetColumns.flatMap(colName =>
        partitionsList.map(partitionPath => new ColumnStatsIndexPrefixRawKey(colName, partitionPath))
      )
    } else {
      targetColumns.map(colName => new ColumnStatsIndexPrefixRawKey(colName))
    }

    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(
        HoodieListData.eager(rawKeys.asJava), HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, shouldReadInMemory)

    val columnStatsRecords: HoodieData[HoodieMetadataColumnStats] =
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      metadataRecords.map(JFunction.toJavaSerializableFunction(record => {
          toScalaOption(record.getData.getInsertValue(null, null))
            .map(metadataRecord => metadataRecord.asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata)
            .orNull
        }))
        .filter(JFunction.toJavaSerializableFunction(columnStatsRecord => columnStatsRecord != null))

    columnStatsRecords
  }

  override def invalidateCaches(): Unit = {
    cachedExpressionIndexViews.foreach { case (_, df) => df.unpersist() }
    cachedExpressionIndexViews.clear()
  }

  /**
   * Return true if metadata table is enabled and expression index metadata partition is available.
   */
  def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled && metaClient.getIndexMetadata.isPresent && !metaClient.getIndexMetadata.get().getIndexDefinitions.isEmpty
  }

  /**
   * Each expression index some query conjunct reads, with those conjuncts. A conjunct reads an index when it compiles
   * to a filter on the index's stats, that is when its term is the index's `f(col)` ([[IndexedExpression.matches]]) or
   * a rule derives a range from it. Any other conjunct cannot prune, and reading the index for it would return every
   * file and end the lookup before the indexes after this one.
   *
   * Only `=` and `IN` conjuncts read an expression index for now, as before; ranges follow with #20537.
   */
  private def findIndexUses(queryFilters: Seq[Expression]): Seq[IndexUse] = {
    val conjuncts = queryFilters.flatMap(SparkBaseIndexSupport.splitConjuncts).filter(isEqualityOrIn)
    metaClient.getIndexMetadata.get().getIndexDefinitions.values().asScala.toSeq
      .filter(definition => MetadataPartitionType.fromPartitionPath(definition.getIndexName).equals(MetadataPartitionType.EXPRESSION_INDEX))
      .flatMap { definition =>
        val indexed = IndexedExpression(definition)
        val source = ExpressionIndexSource(indexed)
        val uses = conjuncts.filterNot(translateIntoExpressionIndexFilterExpr(_, source).equals(TrueLiteral))
        if (uses.isEmpty) None else Some(IndexUse(definition, indexed, uses))
      }
  }

  // NOTE: Not `InSet`: its values are internal ones, for ex a DATE is an INT, which do not compare with the stats
  private def isEqualityOrIn(conjunct: Expression): Boolean = Leaf.parse(conjunct).exists(_.op match {
    case Op.Eq | Op.In => true
    case _ => false
  })

  /**
   * Bloom filter keys: the values of `f(col) = v` and `f(col) IN (...)`, as strings like the writer added them. A file
   * passes if it may hold any key, which is conservative across conjuncts. A conjunct with a value that is not a
   * non-null literal gives no keys, and is skipped.
   */
  private def bloomFilterKeys(use: IndexUse): List[String] =
    use.conjuncts.flatMap(Leaf.parse).filter(leaf => use.indexed.matches(leaf.term)).flatMap { leaf =>
      val values: Seq[Any] = leaf.values.map {
        case Literal(value, _) => value
        case _ => null
      }
      if (values.isEmpty || values.contains(null)) Seq.empty else values.map(_.toString)
    }.toList

  private def loadExpressionIndexRecords(indexPartition: String,
                                         prunedPartitions: Set[String],
                                         shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(indexPartition)
    val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = loadExpressionIndexForColumnsInternal(
      indexDefinition.getSourceFields.asScala.toSeq, prunedPartitions, indexPartition, shouldReadInMemory)
    //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
    colStatsRecords
  }

  private def loadExpressionIndexPartitionStatRecords(indexDefinition: HoodieIndexDefinition, shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    // We are omitting the partition name and only using the column name and expression index partition stat prefix to fetch the records
    // Create a ColumnStatsIndexKey with the column and partition prefix
    val rawKey = new ColumnStatsIndexPrefixRawKey(indexDefinition.getSourceFields.get(0),
      toJavaOption(Option(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX)))
    val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = loadExpressionIndexForColumnsInternal(
      indexDefinition.getIndexName, shouldReadInMemory, Seq(rawKey))
    //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
    colStatsRecords
  }

  def loadExpressionIndexDataFrame(indexPartition: String,
                                   prunedPartitions: Set[String],
                                   shouldReadInMemory: Boolean): DataFrame = {
    val colStatsDF = {
      val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(indexPartition)
      val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = loadExpressionIndexForColumnsInternal(
        indexDefinition.getSourceFields.asScala.toSeq, prunedPartitions, indexPartition, shouldReadInMemory)
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      val catalystRows: HoodieData[InternalRow] = colStatsRecords.map[InternalRow](JFunction.toJavaSerializableFunction(r => {
        val converter = HoodieSchemaConversionUtils.createGenericRecordToInternalRowConverter(HoodieSchema.fromAvroSchema(HoodieMetadataColumnStats.SCHEMA$), columnStatsRecordStructType)
        converter(r).orNull
      }))

      if (shouldReadInMemory) {
        // NOTE: This will instantiate a [[Dataset]] backed by [[LocalRelation]] holding all of the rows
        //       of the transposed table in memory, facilitating execution of the subsequently chained operations
        //       on it locally (on the driver; all such operations are actually going to be performed by Spark's
        //       Optimizer)
        sparkAdapter.getUnsafeUtils.createDataFrameFromInternalRows(spark, catalystRows.collectAsList().asScala.toSeq, columnStatsRecordStructType)
      } else {
        sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(spark, HoodieJavaRDD.getJavaRDD(catalystRows), columnStatsRecordStructType)
      }
    }

    colStatsDF.select(targetColumnStatsIndexColumns.map(col): _*)
  }

  private def loadExpressionIndexForColumnsInternal(targetColumns: Seq[String],
                                                    prunedPartitions: Set[String],
                                                    indexPartition: String,
                                                    shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    // Read Metadata Table's Expression Index records into [[HoodieData]] container by
    //    - Fetching the records from CSI by key-prefixes (column names)
    //    - Extracting [[HoodieMetadataColumnStats]] records
    //    - Filtering out nulls
    checkState(targetColumns.nonEmpty)

    // Create raw key prefixes based on column names and optional partition names
    val rawKeys = if (prunedPartitions.nonEmpty) {
      val partitionsList = prunedPartitions.toList
      targetColumns.flatMap(colName =>
        partitionsList.map(partitionPath => new ColumnStatsIndexPrefixRawKey(colName, partitionPath))
      )
    } else {
      targetColumns.map(colName => new ColumnStatsIndexPrefixRawKey(colName))
    }

    loadExpressionIndexForColumnsInternal(indexPartition, shouldReadInMemory, rawKeys)
  }

  private def loadExpressionIndexForColumnsInternal(indexPartition: String, shouldReadInMemory: Boolean, keyPrefixes: Iterable[ColumnStatsIndexPrefixRawKey]) = {
    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(
        HoodieListData.eager(keyPrefixes.toSeq.asJava), indexPartition, shouldReadInMemory)
    val columnStatsRecords: HoodieData[HoodieMetadataColumnStats] =
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      metadataRecords.map(JFunction.toJavaSerializableFunction(record => {
          toScalaOption(record.getData.getInsertValue(null, null))
            .map(metadataRecord => metadataRecord.asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata)
            .orNull
        }))
        .filter(JFunction.toJavaSerializableFunction(columnStatsRecord => columnStatsRecord != null))

    columnStatsRecords
  }

  def getPrunedPartitionsAndFileNamesMap(prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         includeLogFiles: Boolean = false): Map[String, Set[String]] = {
    prunedPartitionsAndFileSlices.foldLeft(Map.empty[String, Set[String]]) {
      case (partitionToFileMap, (partitionPathOpt, fileSlices)) =>
        partitionPathOpt match {
          case Some(partitionPath) =>
            val fileNames = fileSlices.flatMap { fileSlice =>
              val baseFile = Option(fileSlice.getBaseFile.orElse(null)).map(_.getFileName).toSeq
              val logFiles = if (includeLogFiles) {
                fileSlice.getLogFiles.iterator().asScala.map(_.getFileName).toSeq
              } else Seq.empty[String]
              baseFile ++ logFiles
            }.toSet

            // Update the map with the new partition and its file names
            partitionToFileMap.updated(partitionPath.getPath, partitionToFileMap.getOrElse(partitionPath.getPath, Set.empty) ++ fileNames)
          case None =>
            partitionToFileMap // Skip if no partition path
        }
    }
  }

  private def getCandidateFilesForKeys(indexPartition: String, prunedPartitionAndFileNames: Map[String, Set[String]], keys: List[String]): Set[String] = {
    val candidateFiles = prunedPartitionAndFileNames.flatMap { case (partition, fileNames) =>
      fileNames.filter { fileName =>
        val bloomFilterOpt = toScalaOption(metadataTable.getBloomFilter(partition, fileName, indexPartition))
        bloomFilterOpt match {
          case Some(bloomFilter) =>
            keys.exists(bloomFilter.mightContain)
          case None =>
            true // If bloom filter is empty or undefined, assume the file might contain the record key
        }
      }
    }.toSet

    candidateFiles
  }
}

object ExpressionIndexSupport {

  def extractExpressionIndexValue(valueWrapper: AnyRef, dataType: DataType, valueMetadata: ValueMetadata, useJava8api: Boolean): Any = {
    valueMetadata.getValueType match {
      case ValueType.V1 => extractWrapperValueV1(valueWrapper, dataType)
      case _ => extractExpressionIndexValueV2(valueWrapper, valueMetadata, useJava8api)
    }
  }

  private def extractExpressionIndexValueV2(valueWrapper: AnyRef, valueMetadata: ValueMetadata, useJava8api: Boolean): Any = {
    SparkValueMetadataUtils.convertJavaTypeToSparkType(valueMetadata.unwrapValue(valueWrapper), useJava8api)
  }

  val INDEX_NAME = "EXPRESSION"

  /** One expression index, and the query conjuncts that read it. */
  private case class IndexUse(definition: HoodieIndexDefinition, indexed: IndexedExpression, conjuncts: Seq[Expression]) {
    /** Type of the stored min / max: the matched term's, since the writer applied the same function to the same column. */
    def statsType: DataType = conjuncts.flatMap(_.find(indexed.matches)).head.dataType
  }

  /**
   * Target Column Stats Index columns which internally are mapped onto fields of the corresponding
   * Column Stats record payload ([[HoodieMetadataColumnStats]]) persisted w/in Metadata Table
   */
  private val targetColumnStatsIndexColumns = Seq(
    HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME
  )

  private val columnStatsRecordStructType: StructType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(HoodieSchema.fromAvroSchema(HoodieMetadataColumnStats.SCHEMA$))
}

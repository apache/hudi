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

import org.apache.hudi.ColumnStatsIndexSupport.{composeColumnStatStructType, deserialize, tryUnpackValueWrapper}
import org.apache.hudi.ExpressionIndexSupport._
import org.apache.hudi.HoodieCatalystUtils.{withPersistedData, withPersistedDataset}
import org.apache.hudi.HoodieConversionUtils.{toJavaOption, toScalaOption}
import org.apache.hudi.RecordLevelIndexSupport.filterQueryWithRecordKey
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.{HoodieData, HoodieListData}
import org.apache.hudi.common.function.{SerializableFunction, SerializableFunctionUnchecked}
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.{collection, StringUtils}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.hash.{ColumnIndexID, PartitionIndexID}
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.index.expression.HoodieExpressionIndex
import org.apache.hudi.metadata.{ColumnStatsIndexPrefixRawKey, HoodieMetadataPayload, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionStatsIndexKey
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.HoodieUnsafeUtils.{createDataFrameFromInternalRows, createDataFrameFromRDD, createDataFrameFromRows}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, DateAdd, DateFormatClass, DateSub, EqualTo, Expression, FromUnixTime, In, Literal, ParseToDate, ParseToTimestamp, RegExpExtract, RegExpReplace, StringSplit, StringTrim, StringTrimLeft, StringTrimRight, Substring, UnaryExpression, UnixTimestamp}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
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
  extends SparkBaseIndexSupport (spark, metadataConfig, metaClient) {

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
    lazy val expressionIndexPartitionOpt = getExpressionIndexPartitionAndLiterals(queryFilters)
    if (isIndexAvailable && queryFilters.nonEmpty && expressionIndexPartitionOpt.nonEmpty) {
      val (indexPartition, expressionIndexQuery, literals) = expressionIndexPartitionOpt.get
      val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(indexPartition)
      if (indexDefinition.getIndexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)) {
        val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
        val (prunedPartitions, prunedFileNames) = getPrunedPartitionsAndFileNames(fileIndex, prunedPartitionsAndFileSlices)
        val expressionIndexRecords = loadExpressionIndexRecords(indexPartition, prunedPartitions, readInMemory)
        loadTransposed(queryReferencedColumns, readInMemory, expressionIndexRecords, expressionIndexQuery) {
          transposedColStatsDF =>Some(getCandidateFiles(transposedColStatsDF, Seq(expressionIndexQuery), prunedFileNames, isExpressionIndex = true, Option.apply(indexDefinition)))
        }
      } else if (indexDefinition.getIndexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS)) {
        val prunedPartitionAndFileNames = getPrunedPartitionsAndFileNamesMap(prunedPartitionsAndFileSlices, includeLogFiles = true)
        Option.apply(getCandidateFilesForKeys(indexPartition, prunedPartitionAndFileNames, literals))
      } else {
        Option.empty
      }
    } else {
      Option.empty
    }
  }

  def prunePartitions(fileIndex: HoodieFileIndex,
                      queryFilters: Seq[Expression],
                      queryReferencedColumns: Seq[String]): Option[Set[String]] = {
    lazy val expressionIndexPartitionOpt = getExpressionIndexPartitionAndLiterals(queryFilters)
    if (isIndexAvailable && queryFilters.nonEmpty && expressionIndexPartitionOpt.nonEmpty) {
      val (indexPartition, expressionIndexQuery, _) = expressionIndexPartitionOpt.get
      val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(indexPartition)
      if (indexDefinition.getIndexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)) {
        val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
        val expressionIndexRecords = loadExpressionIndexPartitionStatRecords(indexDefinition, readInMemory)
        loadTransposed(queryReferencedColumns, readInMemory, expressionIndexRecords, expressionIndexQuery) {
          transposedPartitionStatsDF => {
            try {
              transposedPartitionStatsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
              val allPartitions = transposedPartitionStatsDF.select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
                .collect()
                .map(_.getString(0))
                .toSet
              if (allPartitions.nonEmpty) {
                // NOTE: [[translateIntoColumnStatsIndexFilterExpr]] has covered the case where the
                //       column in a filter does not have the stats available, by making sure such a
                //       filter does not prune any partition.
                val indexSchema = transposedPartitionStatsDF.schema
                val indexedCols : Seq[String] = indexDefinition.getSourceFields.asScala.toSeq
                val indexFilter = Seq(expressionIndexQuery).map(translateIntoColumnStatsIndexFilterExpr(_, isExpressionIndex = true, indexedCols = indexedCols)).reduce(And)
                Some(transposedPartitionStatsDF.where(new Column(indexFilter))
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
      } else {
        Option.empty
      }
    } else {
      Option.empty
    }
  }

  /**
   * Loads view of the Column Stats Index in a transposed format where single row coalesces every columns'
   * statistics for a single file, returning it as [[DataFrame]]
   *
   * Please check out scala-doc of the [[transpose]] method explaining this view in more details
   */
  def loadTransposed[T](targetColumns: Seq[String],
                        shouldReadInMemory: Boolean,
                        colStatRecords: HoodieData[HoodieMetadataColumnStats],
                        expressionIndexQuery: Expression)(block: DataFrame => T): T = {
    cachedExpressionIndexViews.get(targetColumns) match {
      case Some(cachedDF) =>
        block(cachedDF)
      case None =>
        val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = colStatRecords
        withPersistedData(colStatsRecords, StorageLevel.MEMORY_ONLY) {
          val (transposedRows, indexSchema) = transpose(colStatsRecords, targetColumns, expressionIndexQuery)
          val df = if (shouldReadInMemory) {
            // NOTE: This will instantiate a [[Dataset]] backed by [[LocalRelation]] holding all of the rows
            //       of the transposed table in memory, facilitating execution of the subsequently chained operations
            //       on it locally (on the driver; all such operations are actually going to be performed by Spark's
            //       Optimizer)
            createDataFrameFromRows(spark, transposedRows.collectAsList().asScala.toSeq, indexSchema)
          } else {
            val rdd = HoodieJavaRDD.getJavaRDD(transposedRows)
            spark.createDataFrame(rdd, indexSchema)
          }

          if (allowCaching) {
            cachedExpressionIndexViews.put(targetColumns, df)
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
  private def transpose(colStatsRecords: HoodieData[HoodieMetadataColumnStats], queryColumns: Seq[String], expressionIndexQuery: Expression): (HoodieData[Row], StructType) = {
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
    val (indexSchema, targetIndexedColumns) = composeIndexSchema(sortedTargetColumnsSet.toSeq, indexedColumns, tableSchema, expressionIndexQuery)

    // Here we perform complex transformation which requires us to modify the layout of the rows
    // of the dataset, and therefore we rely on low-level RDD API to avoid incurring encoding/decoding
    // penalty of the [[Dataset]], since it's required to adhere to its schema at all times, while
    // RDDs are not;
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

          val minValue = deserialize(tryUnpackValueWrapper(minValueWrapper), colType)
          val maxValue = deserialize(tryUnpackValueWrapper(maxValueWrapper), colType)

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

  def composeIndexSchema(targetColumnNames: Seq[String], indexedColumns: Set[String], tableSchema: StructType, expressionIndexQuery: Expression): (StructType, Seq[String]) = {
    val fileNameField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME, StringType, nullable = true, Metadata.empty)
    val valueCountField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT, LongType, nullable = true, Metadata.empty)

    val targetIndexedColumns = targetColumnNames.filter(indexedColumns.contains(_))
    val targetIndexedFields = targetIndexedColumns.map(colName => tableSchema.fields.find(f => f.name == colName).get)

    val dataType: DataType = expressionIndexQuery match {
      case eq: EqualTo => eq.right.asInstanceOf[Literal].dataType
      case in: In => in.list(0).asInstanceOf[Literal].dataType
      case _ => targetIndexedFields(0).dataType
    }

    (StructType(
      targetIndexedFields.foldLeft(Seq(fileNameField, valueCountField)) {
        case (acc, field) =>
          acc ++ Seq(
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE, dataType),
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE, dataType),
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

  private def filterQueriesWithFunctionalFilterKey(queryFilters: Seq[Expression], sourceFieldOpt: Option[String],
                                                   attributeFetcher: Function1[Expression, Expression]): List[Tuple2[Expression, List[String]]] = {
    var expressionIndexQueries: List[Tuple2[Expression, List[String]]] = List.empty
    for (query <- queryFilters) {
      filterQueryWithRecordKey(query, sourceFieldOpt, attributeFetcher).foreach({
        case (exp: Expression, literals: List[String]) =>
          expressionIndexQueries = expressionIndexQueries :+ Tuple2.apply(exp, literals)
      })
    }

    expressionIndexQueries
  }

  /**
   * Searches for an index partition based on the specified index function and target column name.
   *
   * This method looks up the index definitions available in the metadata of a `metaClient` instance
   * and attempts to find an index partition where the index function and the source fields match
   * the provided arguments. If a matching index definition is found, the partition identifier for
   * that index is returned.
   *
   * @param queryFilters A sequence of `Expression` objects to analyze. Each expression should involve a single column
   *                     for the method to consider it (expressions involving multiple columns are skipped).
   * @return An `Option` containing the index partition identifier if a matching index definition is found.
   *         Returns `None` if no matching index definition is found.
   */
  private def getExpressionIndexPartitionAndLiterals(queryFilters: Seq[Expression]): Option[Tuple3[String, Expression, List[String]]] = {
    val indexDefinitions = metaClient.getIndexMetadata.get().getIndexDefinitions.asScala
    if (indexDefinitions.nonEmpty) {
      val functionDefinitions = indexDefinitions.values
        .filter(definition => MetadataPartitionType.fromPartitionPath(definition.getIndexName).equals(MetadataPartitionType.EXPRESSION_INDEX))
        .toList
      var indexPartitionAndLiteralsOpt: Option[Tuple3[String, Expression, List[String]]] = Option.empty
      functionDefinitions.foreach(indexDefinition => {
        val queryInfoOpt = extractQueryAndLiterals(queryFilters, indexDefinition)
        if (queryInfoOpt.isDefined) {
          indexPartitionAndLiteralsOpt = Option.apply(Tuple3.apply(indexDefinition.getIndexName, queryInfoOpt.get._1, queryInfoOpt.get._2))
        }
      })
      indexPartitionAndLiteralsOpt
    } else {
      Option.empty
    }
  }

  /**
   * Extracts mappings from function names to column names from a sequence of expressions.
   *
   * This method iterates over a given sequence of Spark SQL expressions and identifies expressions
   * that contain function calls corresponding to keys in the ExpressionIndexFunction. It supports only
   * expressions that are simple binary expressions involving a single column. If an expression contains
   * one of the functions and operates on a single column, this method maps the function name to the
   * column name.
   */
  private def extractQueryAndLiterals(queryFilters: Seq[Expression], indexDefinition: HoodieIndexDefinition): Option[(Expression, List[String])] = {
    val attributeFetcher = (expr: Expression) => {
      expr match {
        case expression: UnaryExpression => expression.child
        case expression: DateFormatClass if expression.right.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexFormatOption()) =>
          expression.left
        case expression: FromUnixTime
          if expression.format.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexFormatOption(TimestampFormatter.defaultPattern())) =>
          expression.sec
        case expression: UnixTimestamp if expression.right.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexFormatOption(TimestampFormatter.defaultPattern())) =>
          expression.timeExp
        case expression: ParseToDate if (expression.format.isEmpty && StringUtils.isNullOrEmpty(indexDefinition.getExpressionIndexFormatOption()))
          || (expression.format.isDefined && expression.format.get.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexFormatOption())) =>
          expression.left
        case expression: ParseToTimestamp if (expression.format.isEmpty && StringUtils.isNullOrEmpty(indexDefinition.getExpressionIndexFormatOption()))
          || (expression.format.isDefined && expression.format.get.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexFormatOption())) =>
          expression.left
        case expression: DateAdd if expression.days.isInstanceOf[Literal] && expression.days.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexDaysOption) =>
          expression.startDate
        case expression: DateSub if expression.days.isInstanceOf[Literal] && expression.days.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexDaysOption) =>
          expression.startDate
        case expression: Substring if expression.pos.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexPositionOption)
          && expression.len.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexLengthOption)=>
          expression.str
        case expression: StringTrim if (expression.trimStr.isEmpty && StringUtils.isNullOrEmpty(indexDefinition.getExpressionIndexTrimStringOption))
          || (expression.trimStr.isDefined && expression.trimStr.get.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexTrimStringOption))  => expression.srcStr
        case expression: StringTrimLeft if (expression.trimStr.isEmpty && StringUtils.isNullOrEmpty(indexDefinition.getExpressionIndexTrimStringOption))
          || (expression.trimStr.isDefined && expression.trimStr.get.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexTrimStringOption))  => expression.srcStr
        case expression: StringTrimRight if (expression.trimStr.isEmpty && StringUtils.isNullOrEmpty(indexDefinition.getExpressionIndexTrimStringOption))
          || (expression.trimStr.isDefined && expression.trimStr.get.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexTrimStringOption)) => expression.srcStr
        case expression: RegExpReplace if expression.pos.asInstanceOf[Literal].value.toString.equals("1")
          && expression.regexp.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexPatternOption)
          && expression.rep.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexReplacementOption) =>
          expression.subject
        case expression: RegExpExtract if expression.regexp.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexPatternOption)
          && expression.idx.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexIndexOption) =>
          expression.subject
        case expression: StringSplit if expression.limit.asInstanceOf[Literal].value.toString.equals("-1")
          && expression.regex.asInstanceOf[Literal].value.toString.equals(indexDefinition.getExpressionIndexPatternOption) =>
          expression.str
        case other => other
      }
    }
    val expressionIndexQueries = filterQueriesWithFunctionalFilterKey(queryFilters, Option.apply(indexDefinition.getSourceFields.get(0)), attributeFetcher)
    var queryAndLiteralsOpt: Option[(Expression, List[String])] = Option.empty
    expressionIndexQueries.foreach { tuple =>
      val (expr, literals) = (tuple._1, tuple._2)
      queryAndLiteralsOpt = Option.apply(Tuple2.apply(expr, literals))
    }
    queryAndLiteralsOpt
  }

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
        val converter = AvroConversionUtils.createAvroToInternalRowConverter(HoodieMetadataColumnStats.SCHEMA$, columnStatsRecordStructType)
        converter(r).orNull
      }))

      if (shouldReadInMemory) {
        // NOTE: This will instantiate a [[Dataset]] backed by [[LocalRelation]] holding all of the rows
        //       of the transposed table in memory, facilitating execution of the subsequently chained operations
        //       on it locally (on the driver; all such operations are actually going to be performed by Spark's
        //       Optimizer)
        createDataFrameFromInternalRows(spark, catalystRows.collectAsList().asScala.toSeq, columnStatsRecordStructType)
      } else {
        createDataFrameFromRDD(spark, HoodieJavaRDD.getJavaRDD(catalystRows), columnStatsRecordStructType)
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
            partitionToFileMap.updated(partitionPath.path, partitionToFileMap.getOrElse(partitionPath.path, Set.empty) ++ fileNames)
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
  val INDEX_NAME = "EXPRESSION"
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

  private val columnStatsRecordStructType: StructType = AvroConversionUtils.convertAvroSchemaToStructType(HoodieMetadataColumnStats.SCHEMA$)
}

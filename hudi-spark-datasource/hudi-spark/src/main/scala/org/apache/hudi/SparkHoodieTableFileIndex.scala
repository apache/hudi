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

import org.apache.hadoop.fs.Path
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.keygen.{TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{FileStatusCache, NoopCache, SparkParsePartitionUtil}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// TODO unify w/ HoodieFileIndex
class SparkHoodieTableFileIndex(spark: SparkSession,
                                metaClient: HoodieTableMetaClient,
                                schemaSpec: Option[StructType],
                                configProperties: TypedProperties,
                                @transient fileStatusCache: FileStatusCache = NoopCache)
  extends AbstractHoodieTableFileIndex(
    engineContext = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)),
    metaClient,
    configProperties,
    fileStatusCache
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

    val nameFieldMap = schema.fields.map(filed => filed.name -> filed).toMap

    if (partitionColumns.isPresent) {
      val partitionFields = partitionColumns.get().map(column =>
        nameFieldMap.getOrElse(column, throw new IllegalArgumentException(s"Cannot find column: '" +
          s"$column' in the schema[${schema.fields.mkString(",")}]")))
      new StructType(partitionFields)
    } else { // If the partition columns have not stored in hoodie.properties(the table that was
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
   * @param partitionFilters partition column filters
   * @return mapping from string partition paths to its base/log files
   */
  def listFileSlices(partitionFilters: Seq[Expression]): Map[String, Seq[FileSlice]] = {
    // Prune the partition path by the partition filters
    val prunedPartitions = prunePartition(cachedAllInputFileSlices.keys.toSeq, partitionFilters)
    prunedPartitions.map(partition => {
      (partition.partitionPath, cachedAllInputFileSlices(partition))
    }).toMap
  }

  /**
   * Prune the partition by the filter.This implementation is fork from
   * org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex#prunePartitions.
   *
   * @param partitionPaths All the partition paths.
   * @param predicates     The filter condition.
   * @return The Pruned partition paths.
   */
  def prunePartition(partitionPaths: Seq[PartitionRowPath],
                     predicates: Seq[Expression]): Seq[PartitionRowPath] = {

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
        case PartitionRowPath(values, _) => boundPredicate.eval(InternalRow.fromSeq(values))
      }
      logInfo(s"Total partition size is: ${partitionPaths.size}," +
        s" after partition prune size is: ${prunedPartitionPaths.size}")
      prunedPartitionPaths
    } else {
      partitionPaths
    }
  }

  override protected def parsePartitionValuesFromPath(pathWithPartitionName: Path): Option[Seq[(String, Any)]] = {
    val timeZoneId = Option.apply(configProperties.getString(DateTimeUtils.TIMEZONE_OPTION))
      .getOrElse(SQLConf.get.sessionLocalTimeZone)
    val partitionDataTypes = partitionSchema.map(f => f.name -> f.dataType).toMap

    sparkParsePartitionUtil.parsePartition(
      pathWithPartitionName,
      typeInference = false,
      Set(new Path(basePath)),
      partitionDataTypes,
      DateTimeUtils.getTimeZone(timeZoneId)
    )
      .map(pv => pv.columnNames.zip(pv.literals).map(t => Tuple2(t._1, t._2.value)))
  }
}

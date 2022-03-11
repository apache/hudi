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
import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.SparkHoodieTableFileIndex.{deduceQueryType, toJavaOption}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.{FileSlice, HoodieTableQueryType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileStatusCache, NoopCache}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.language.implicitConversions

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
  private lazy val _partitionSchemaFromProperties: StructType =
    HoodieCommonUtils.getPartitionSchemaFromProperty(metaClient, Some(schema))

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
    val prunedPartitions = HoodieCommonUtils.prunePartition(partitionSchema,
      cachedAllInputFileSlices.asScala.keys.toSeq, partitionFilters)
    prunedPartitions.map(partition => {
      (partition.path, cachedAllInputFileSlices.get(partition).asScala)
    }).toMap
  }

  protected def parsePartitionColumnValues(partitionColumns: Array[String], partitionPath: String): Array[Object] = {
    HoodieCommonUtils.parsePartitionColumnValues(sparkParsePartitionUtil, configProperties,
      basePath, partitionSchema, partitionColumns, partitionPath)
  }
}

object SparkHoodieTableFileIndex {

  implicit def toJavaOption[T](opt: Option[T]): org.apache.hudi.common.util.Option[T] =
    if (opt.isDefined) {
      org.apache.hudi.common.util.Option.of(opt.get)
    } else {
      org.apache.hudi.common.util.Option.empty()
    }

  private def deduceQueryType(configProperties: TypedProperties): HoodieTableQueryType = {
    configProperties.asScala(QUERY_TYPE.key()) match {
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

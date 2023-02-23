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

package org.apache.spark.sql.catalyst.plans.logcal

import org.apache.hudi.{DataSourceReadOptions, DefaultSource}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.util.Locale
import scala.collection.immutable.HashSet
import scala.collection.mutable


case class HoodieQueryWithKV(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}

object HoodieQueryWithKV {

  val FUNC_NAME = "hudi_query_with_kv"

  private val KV_SPLIT = "=>"

  // Add a white list to the key to prevent users adding other parameters
  val KeyWhiteSet: HashSet[String] = HashSet(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
    , DataSourceReadOptions.INCREMENTAL_FORMAT.key()
    , DataSourceReadOptions.QUERY_TYPE.key()
    , DataSourceReadOptions.REALTIME_MERGE.key()
    , DataSourceReadOptions.READ_PATHS.key()
    , DataSourceReadOptions.READ_PRE_COMBINE_FIELD.key()
    , DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.key()
    , DataSourceReadOptions.BEGIN_INSTANTTIME.key()
    , DataSourceReadOptions.END_INSTANTTIME.key()
    , DataSourceReadOptions.INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.key()
    , DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS.key()
    , DataSourceReadOptions.INCR_PATH_GLOB.key()
    , DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key()
    , DataSourceReadOptions.ENABLE_DATA_SKIPPING.key()
    , DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key()
    , DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.key()
    , DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key()
  )

  def resolve(spark: SparkSession, func: HoodieQueryWithKV): LogicalPlan = {
    val args: Seq[Expression] = func.args
    if (args == null || args.isEmpty) {
      throw new AnalysisException("hudi_query_kv`s parameter can`t be null.")
    }
    val tableName: String = args.head.eval().toString
    if (tableName.contains(KV_SPLIT)) {
      throw new AnalysisException("The first parameter must be a hudi table, not a key value pair.")
    }
    val identifier: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    if (!spark.sessionState.catalog.tableExists(identifier)) {
      throw new AnalysisException(s"The table $tableName does not exist. Please check.")
    }
    val catalogTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
    if (catalogTable.provider.map(_.toLowerCase(Locale.ROOT)).orNull != "hudi") {
      throw new AnalysisException(s"The hudi_query_kv function only supports querying the hudi table.")
    }

    val options: mutable.Map[String, String] = mutable.Map("path" -> catalogTable.location.toString) ++ parseOptions(args.tail)

    val hoodieDataSource = new DefaultSource
    val relation: BaseRelation = hoodieDataSource.createRelation(spark.sqlContext, options.toMap)
    new LogicalRelation(
      relation,
      relation.schema.toAttributes,
      Some(catalogTable),
      false
    )
  }

  private def parseOptions(args: Seq[Expression]): Map[String, String] = {
    val options = mutable.Map.empty[String, String]

    args.foreach(arg => {
      val kv: String = arg.eval().toString
      if (!kv.contains(KV_SPLIT)) {
        throw new AnalysisException(s"hudi_query_kv`s parameters can only be key value pairs like (k=>v).Not support $kv")
      }
      val pair: Array[String] = kv.split(KV_SPLIT, 2)
      if (KeyWhiteSet.contains(pair.apply(0).trim)) {
        options += (pair.apply(0).trim -> pair.apply(1).trim)
      } else {
        throw new AnalysisException(s"The hudi_query_kv function only query parameters are supported,Not support ${pair.apply(0)}")
      }
    })
    options.toMap
  }
}



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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType

/**
 * NOTE: Since support for [[TableCatalog]] was only added in Spark 3, this trait
 *       is going to be an empty one simply serving as a placeholder (for compatibility w/ Spark 2)
 */
trait HoodieCatalogUtils {}

object HoodieCatalogUtils {

  def refreshTable(spark: SparkSession, qualifiedTableName: String): Unit = {
    val tableId = spark.sessionState.sqlParser.parseTableIdentifier(qualifiedTableName)
    refreshTable(spark, tableId)
  }

  /**
   * NOTE: This is borrowed from Spark 3.1.3
   *
   * TODO elaborate
   */
  def refreshTable(spark: SparkSession, tableId: TableIdentifier): Unit = {
    val sessionCatalog = spark.sessionState.catalog
    val tableMetadata = sessionCatalog.getTempViewOrPermanentTableMetadata(tableId)

    // Before proceeding we validate whether this table is actually cached w/in [[SessionCatalog]],
    // since, for ex, in case of writing via Spark DataSource (V1) API, Spark wouldn't actually
    // resort to caching the data
    val cachedPlan = sessionCatalog.getCachedTable(
      QualifiedTableName(tableId.database.getOrElse(tableMetadata.database), tableId.identifier))

    if (cachedPlan != null) {
      // NOTE: Provided that this table is still cached, following operation wouldn't trigger
      //       subsequent resolving and ultimately listing of the table
      val queryExecution = spark.withActive {
        spark.sessionState.executePlan(UnresolvedRelation(tableId))
      }

      if (tableMetadata.tableType == CatalogTableType.VIEW) {
        // Temp or persistent views: refresh (or invalidate) any metadata/data cached
        // in the plan recursively
        queryExecution.analyzed.refresh()
      } else {
        // Non-temp tables: refresh the metadata cache
        sessionCatalog.invalidateCachedTable(tableId)
      }

      // If this table is cached as an InMemoryRelation, drop the original
      // cached version and make the new version cached lazily
      val cache = spark.sharedState.cacheManager.lookupCachedData(queryExecution.logical)

      // Uncache the logical plan
      // NOTE: This is a no-op for the table itself if it's not cached, but will invalidate all
      // caches referencing this table.
      spark.sharedState.cacheManager.uncacheQuery(spark, cascade = true, plan = queryExecution.logical)

      // TODO can we refresh the CacheManager if we're not refreshing the relation?
      //if (cache.nonEmpty) {
      //  // save the cache name and cache level for recreation
      //  val cacheName = cache.get.cachedRepresentation.cacheBuilder.tableName
      //  val cacheLevel = cache.get.cachedRepresentation.cacheBuilder.storageLevel
      //
      //  // creates a new logical plan since the old table refers to old relation which
      //  // should be refreshed
      //  val newTable = spark.table(tableIdent)
      //
      //  // recache with the same name and cache level.
      //  spark.sharedState.cacheManager.cacheQuery(newTable, cacheName, cacheLevel)
      //}
    }
  }
}

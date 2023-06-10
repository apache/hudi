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

import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}

/**
 * NOTE: Since support for [[TableCatalog]] was only added in Spark 3, this trait
 *       is going to be an empty one simply serving as a placeholder (for compatibility w/ Spark 2)
 */
trait HoodieCatalogUtils {}

object HoodieCatalogUtils {

  /**
   * Please check scala-doc for other overloaded [[refreshTable()]] operation
   */
  def refreshTable(spark: SparkSession, qualifiedTableName: String): Unit = {
    val tableId = spark.sessionState.sqlParser.parseTableIdentifier(qualifiedTableName)
    refreshTable(spark, tableId)
  }

  /**
   * Refreshes metadata and flushes cached data (resolved [[LogicalPlan]] representation,
   * already loaded [[InMemoryRelation]]) for the table identified by [[tableId]].
   *
   * This method is usually invoked at the end of the write operation to make sure cached
   * data/metadata are synchronized with the state on storage.
   *
   * NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
   *       This is borrowed from Spark 3.1.3 and modified to satisfy Hudi needs:
   *          - Unlike Spark canonical implementation, in case of Hudi this method is invoked
   *            after writes carried out via Spark DataSource integration as well and as such
   *            in these cases data might actually be missing from the caches, therefore
   *            actually re-triggering resolution phase (involving file-listing, etc) for the
   *            first time
   *          - Additionally, this method is modified to avoid refreshing [[LogicalRelation]]
   *            completely to make sure that we're not re-triggering the file-listing of the
   *            table, immediately after it's been written, instead deferring it to subsequent
   *            read operation
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
      // NOTE: Provided that this table is still cached, following operation would not be
      //       triggering subsequent resolution and listing of the table
      val table = spark.table(tableId)

      if (tableMetadata.tableType == CatalogTableType.VIEW) {
        // Temp or persistent views: refresh (or invalidate) any metadata/data cached
        // in the plan recursively
        table.queryExecution.analyzed.refresh()
      } else {
        // Non-temp tables: refresh the metadata cache
        sessionCatalog.refreshTable(tableId)
      }

      // Here we simply clear up the cached data for this particular logical plan as well as
      // all other entries dependent on it. On top of that, Spark original implementation however,
      // also caches the new refreshed logical plan. By avoiding refreshing here (w/in the scope
      // of the write) we instead defer it to occur during subsequent read operation.
      //
      // NOTE: This is a no-op for the table itself if it's not cached, but will invalidate all
      //       caches referencing this table.
      spark.sharedState.cacheManager.uncacheQuery(table, cascade = true)
    }
  }
}

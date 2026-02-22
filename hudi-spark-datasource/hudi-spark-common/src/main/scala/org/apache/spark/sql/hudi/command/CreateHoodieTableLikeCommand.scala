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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.util.ConfigUtils
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException

import scala.util.control.NonFatal

case class CreateHoodieTableLikeCommand(targetTable: TableIdentifier,
                                        sourceTable: TableIdentifier,
                                        fileFormat: CatalogStorageFormat,
                                        properties: Map[String, String] = Map.empty,
                                        ignoreIfExists: Boolean)
  extends HoodieLeafRunnableCommand with SparkAdapterSupport {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    val tableIsExists = catalog.tableExists(targetTable)
    if (tableIsExists) {
      if (ignoreIfExists) {
        // scalastyle:off
        return Seq.empty[Row]
        // scalastyle:on
      } else {
        throw new IllegalArgumentException(s"Table $targetTable already exists.")
      }
    }

    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)

    val newStorage = if (fileFormat.inputFormat.isDefined) {
      fileFormat
    } else {
      sourceTableDesc.storage.copy(locationUri = fileFormat.locationUri)
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType = if (newStorage.locationUri.isEmpty) {
      CatalogTableType.MANAGED
    } else {
      CatalogTableType.EXTERNAL
    }

    val targetTableProperties = if (sparkAdapter.isHoodieTable(sourceTableDesc)) {
      HoodieOptionConfig.extractHoodieOptions(sourceTableDesc.properties) ++ properties
    } else {
      properties
    }

    val newTableDesc = CatalogTable(
      identifier = targetTable,
      tableType = tblType,
      storage = newStorage,
      schema = sourceTableDesc.schema,
      provider = Some("hudi"),
      partitionColumnNames = sourceTableDesc.partitionColumnNames,
      bucketSpec = sourceTableDesc.bucketSpec,
      properties = targetTableProperties,
      tracksPartitionsInCatalog = sourceTableDesc.tracksPartitionsInCatalog)

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, newTableDesc)
    // check if there are conflict between table configs defined in hoodie table and properties defined in catalog.
    CreateHoodieTableCommand.validateTblProperties(hoodieCatalogTable)

    val queryAsProp = hoodieCatalogTable.catalogProperties.get(ConfigUtils.IS_QUERY_AS_RO_TABLE)
    if (!hoodieCatalogTable.hoodieTableExists) {
      // init hoodie table for a normal table (not a ro/rt table)
      // TODO: Find a way to block ro/rt table creation
      hoodieCatalogTable.initHoodieTable()
    }

    try {
      // create catalog table for this hoodie table
      CreateHoodieTableCommand.createTableInCatalog(sparkSession, hoodieCatalogTable, ignoreIfExists, queryAsProp)
    } catch {
      case NonFatal(e) =>
        throw new HoodieException("Failed to create catalog table in metastore", e)
    }
    Seq.empty[Row]
  }
}

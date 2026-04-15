/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.internal.schema.Types
import org.apache.hudi.storage.{HoodieStorage, StoragePath}
import org.apache.hudi.util.PartitionPathFilterUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf

import java.util

import scala.collection.JavaConverters._

class CatalogBackedTableMetadata(engineContext: HoodieEngineContext,
                                 tableConfig: HoodieTableConfig,
                                 storage: HoodieStorage,
                                 datasetBasePath: String) extends
  FileSystemBackedTableMetadata(engineContext, tableConfig, storage, datasetBasePath) with Logging {

  private val sparkSession = engineContext.asInstanceOf[HoodieSparkEngineContext].getSqlContext.sparkSession
  private val catalogTableName = tableConfig.getTableName
  private lazy val catalogDatabaseName =
    if (StringUtils.isNullOrEmpty(tableConfig.getDatabaseName)) {
      sparkSession.sessionState.catalog.getCurrentDatabase
    } else {
      tableConfig.getDatabaseName
    }
  private lazy val tableIdentifier = TableIdentifier(catalogTableName, Some(catalogDatabaseName))
  private lazy val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)

  private def isPartitionedTable: Boolean = {
    catalogTable.partitionColumnNames.nonEmpty
  }

  private def shouldUseCatalogPartitions: Boolean = {
    isPartitionedTable && catalogTable.tracksPartitionsInCatalog
  }

  override def getAllPartitionPaths():
  util.List[String] =
    if (!isPartitionedTable) {
      util.Collections.emptyList()
    } else if (shouldUseCatalogPartitions) {
      sparkSession.sessionState.catalog.externalCatalog
        .listPartitions(catalogDatabaseName, catalogTableName)
        .map(catalogTablePartition => {
          val partitionPathURI = new StoragePath(catalogTablePartition.location)
          FSUtils.getRelativePartitionPath(dataBasePath, partitionPathURI)
        }).asJava
    } else {
      super.getAllPartitionPaths()
    }

  override def getPartitionPathWithPathPrefixes(relativePathPrefixes: util.List[String]):
  util.List[String] =
    if (!isPartitionedTable) {
      util.Collections.emptyList()
    } else if (shouldUseCatalogPartitions) {
      filterPartitionsBasedOnRelativePathPrefixes(relativePathPrefixes,
        sparkSession.sessionState.catalog.externalCatalog
          .listPartitions(catalogDatabaseName, catalogTableName))
    } else {
      super.getPartitionPathWithPathPrefixes(relativePathPrefixes)
    }

  override def getPartitionPathWithPathPrefixUsingFilterExpression(relativePathPrefix: util.List[String],
                                                                   partitionFields: Types.RecordType,
                                                                   pushedExpr: org.apache.hudi.expression.Expression,
                                                                   partitionPredicateExpressions: util.List[Object]):
  util.List[String] = {
    if (!isPartitionedTable) {
      util.Collections.emptyList()
    } else if (shouldUseCatalogPartitions) {
      val partitionPredicateExpressionSeq = partitionPredicateExpressions.asScala.map(_.asInstanceOf[Expression]).toSeq
      filterPartitionsBasedOnRelativePathPrefixes(relativePathPrefix,
        sparkSession.sessionState.catalog.externalCatalog
          .listPartitionsByFilter(catalogDatabaseName, catalogTableName, partitionPredicateExpressionSeq,
            SQLConf.get.sessionLocalTimeZone))
    } else {
      super.getPartitionPathWithPathPrefixUsingFilterExpression(relativePathPrefix, partitionFields, pushedExpr)
    }
  }

  private def filterPartitionsBasedOnRelativePathPrefixes(relativePathPrefix: util.List[String],
                                                          catalogTablePartitionSeq: Seq[CatalogTablePartition]):
  util.List[String] = {
    // Convert CatalogTablePartition object to String object containing relativePartitionPath.
    // and use relativePathPrefixesPredicate to filter the partition paths further
    val relativePathPrefixPredicate = PartitionPathFilterUtil.relativePathPrefixPredicate(relativePathPrefix)
    catalogTablePartitionSeq
      .map(catalogTablePartition => {
        val partitionPathURI = new StoragePath(catalogTablePartition.location)
        FSUtils.getRelativePartitionPath(dataBasePath, partitionPathURI)
      }).filter(relativePathPrefixPredicate.test)
      .asJava
  }
}

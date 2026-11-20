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

import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.schema.internal.Types
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.sync.common.HoodieMetaSyncOperations.{HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, HOODIE_LAST_COMMIT_TIME_SYNC}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf

import java.util

import scala.collection.JavaConverters._

/**
 * Lists partitions from the catalog entry the read was resolved from. The entry is used only while its last
 * synced commit matches the table's latest completed commit; otherwise partitions are listed from the file system.
 */
class CatalogBackedTableMetadata(engineContext: HoodieEngineContext,
                                 metaClient: HoodieTableMetaClient,
                                 catalogTableIdentifier: TableIdentifier) extends
  FileSystemBackedTableMetadata(engineContext, metaClient.getTableConfig, metaClient.getStorage, metaClient.getBasePath.toString)
  with Logging {

  private val sparkSession = engineContext.asInstanceOf[HoodieSparkEngineContext].getSqlContext.sparkSession
  private val catalogDatabaseName = catalogTableIdentifier.database.get
  private val catalogTableName = catalogTableIdentifier.table
  private lazy val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(catalogTableIdentifier)

  /**
   * Whether the catalog entry was synced at the table's latest completed commit, matching what meta sync records
   * (the latest completed commit and, when present, the latest completion time). Commits that meta sync does not
   * record, such as async compaction and clustering, and conditional sync that skips commits without partition
   * changes, make this false on a catalog that is otherwise current; the file system listing is used then by design.
   */
  private lazy val isCatalogInSync: Boolean = {
    val completedCommits = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val latestCommit = completedCommits.lastInstant
    val lastSyncedCommit = catalogTable.properties.get(HOODIE_LAST_COMMIT_TIME_SYNC)
    val lastSyncedCompletion = catalogTable.properties.get(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC)
    val inSync = !latestCommit.isPresent || (lastSyncedCommit.contains(latestCommit.get.requestedTime)
      && lastSyncedCompletion.forall(completion => toScalaOption(completedCommits.getLatestCompletionTime).contains(completion)))
    if (!inSync) {
      logInfo(s"Listing partitions of ${metaClient.getBasePath} from the file system: catalog entry "
        + s"$catalogTableIdentifier was last synced at ${lastSyncedCommit.getOrElse("no recorded commit")}, "
        + s"behind the latest completed commit ${latestCommit.get.requestedTime}")
    }
    inSync
  }

  private def isPartitionedTable: Boolean = {
    catalogTable.partitionColumnNames.nonEmpty
  }

  private def shouldUseCatalogPartitions: Boolean = {
    isPartitionedTable && catalogTable.tracksPartitionsInCatalog && isCatalogInSync
  }

  override def getAllPartitionPaths():
  util.List[String] =
    if (isCatalogInSync && !isPartitionedTable) {
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
    if (isCatalogInSync && !isPartitionedTable) {
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
                                                                   pushedExpr: org.apache.hudi.common.expression.Expression,
                                                                   partitionPredicateExpressions: util.List[Object]):
  util.List[String] = {
    if (isCatalogInSync && !isPartitionedTable) {
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
    val relativePathPrefixPredicate = HoodieTableMetadataUtil.relativePathPrefixPredicate(relativePathPrefix)
    catalogTablePartitionSeq
      .map(catalogTablePartition => {
        val partitionPathURI = new StoragePath(catalogTablePartition.location)
        FSUtils.getRelativePartitionPath(dataBasePath, partitionPathURI)
      }).filter(relativePathPrefixPredicate.test)
      .asJava
  }
}

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

package org.apache.spark.sql.hudi.command.procedures

import java.util.Locale
import java.util.function.Supplier

object HoodieProcedures {
  private val BUILDERS: Map[String, Supplier[ProcedureBuilder]] = initProcedureBuilders

  def newBuilder(name: String): ProcedureBuilder = {
    val builderSupplier = BUILDERS.get(name.toLowerCase(Locale.ROOT))
    if (builderSupplier.isDefined) builderSupplier.get.get() else null
  }

  def procedures(): Map[String, Supplier[ProcedureBuilder]] = {
    BUILDERS
  }

  private def initProcedureBuilders: Map[String, Supplier[ProcedureBuilder]] = {
    Map((RunCompactionProcedure.NAME, RunCompactionProcedure.builder)
      ,(ShowCompactionProcedure.NAME, ShowCompactionProcedure.builder)
      ,(CreateSavepointProcedure.NAME, CreateSavepointProcedure.builder)
      ,(DeleteSavepointProcedure.NAME, DeleteSavepointProcedure.builder)
      ,(RollbackToSavepointProcedure.NAME, RollbackToSavepointProcedure.builder)
      ,(RollbackToInstantTimeProcedure.NAME, RollbackToInstantTimeProcedure.builder)
      ,(RunClusteringProcedure.NAME, RunClusteringProcedure.builder)
      ,(ShowClusteringProcedure.NAME, ShowClusteringProcedure.builder)
      ,(ShowCommitsProcedure.NAME, ShowCommitsProcedure.builder)
      ,(ShowCommitsMetadataProcedure.NAME, ShowCommitsMetadataProcedure.builder)
      ,(ShowArchivedCommitsProcedure.NAME, ShowArchivedCommitsProcedure.builder)
      ,(ShowArchivedCommitsMetadataProcedure.NAME, ShowArchivedCommitsMetadataProcedure.builder)
      ,(ShowCommitFilesProcedure.NAME, ShowCommitFilesProcedure.builder)
      ,(ShowCommitPartitionsProcedure.NAME, ShowCommitPartitionsProcedure.builder)
      ,(ShowCommitWriteStatsProcedure.NAME, ShowCommitWriteStatsProcedure.builder)
      ,(CommitsCompareProcedure.NAME, CommitsCompareProcedure.builder)
      ,(ShowSavepointsProcedure.NAME, ShowSavepointsProcedure.builder)
      ,(DeleteMarkerProcedure.NAME, DeleteMarkerProcedure.builder)
      ,(ShowRollbacksProcedure.NAME, ShowRollbacksProcedure.builder)
      ,(ShowRollbackDetailProcedure.NAME, ShowRollbackDetailProcedure.builder)
      ,(RunRollbackInflightTableServiceProcedure.NAME, RunRollbackInflightTableServiceProcedure.builder)
      ,(ExportInstantsProcedure.NAME, ExportInstantsProcedure.builder)
      ,(ShowAllFileSystemViewProcedure.NAME, ShowAllFileSystemViewProcedure.builder)
      ,(ShowLatestFileSystemViewProcedure.NAME, ShowLatestFileSystemViewProcedure.builder)
      ,(ShowHoodieLogFileMetadataProcedure.NAME, ShowHoodieLogFileMetadataProcedure.builder)
      ,(ShowHoodieLogFileRecordsProcedure.NAME, ShowHoodieLogFileRecordsProcedure.builder)
      ,(StatsWriteAmplificationProcedure.NAME, StatsWriteAmplificationProcedure.builder)
      ,(StatsFileSizeProcedure.NAME, StatsFileSizeProcedure.builder)
      ,(HdfsParquetImportProcedure.NAME, HdfsParquetImportProcedure.builder)
      ,(RunBootstrapProcedure.NAME, RunBootstrapProcedure.builder)
      ,(ShowBootstrapMappingProcedure.NAME, ShowBootstrapMappingProcedure.builder)
      ,(ShowBootstrapPartitionsProcedure.NAME, ShowBootstrapPartitionsProcedure.builder)
      ,(UpgradeTableProcedure.NAME, UpgradeTableProcedure.builder)
      ,(DowngradeTableProcedure.NAME, DowngradeTableProcedure.builder)
      ,(ShowMetadataTableColumnStatsProcedure.NAME, ShowMetadataTableColumnStatsProcedure.builder)
      ,(ShowColumnStatsOverlapProcedure.NAME, ShowColumnStatsOverlapProcedure.builder)
      ,(ShowMetadataTableFilesProcedure.NAME, ShowMetadataTableFilesProcedure.builder)
      ,(ShowMetadataTablePartitionsProcedure.NAME, ShowMetadataTablePartitionsProcedure.builder)
      ,(CreateMetadataTableProcedure.NAME, CreateMetadataTableProcedure.builder)
      ,(DeleteMetadataTableProcedure.NAME, DeleteMetadataTableProcedure.builder)
      ,(InitMetadataTableProcedure.NAME, InitMetadataTableProcedure.builder)
      ,(ShowMetadataTableStatsProcedure.NAME, ShowMetadataTableStatsProcedure.builder)
      ,(ValidateMetadataTableFilesProcedure.NAME, ValidateMetadataTableFilesProcedure.builder)
      ,(ShowFsPathDetailProcedure.NAME, ShowFsPathDetailProcedure.builder)
      ,(ShowFileStatusProcedure.NAME, ShowFileStatusProcedure.builder)
      ,(CopyToTableProcedure.NAME, CopyToTableProcedure.builder)
      ,(RepairAddpartitionmetaProcedure.NAME, RepairAddpartitionmetaProcedure.builder)
      ,(RepairCorruptedCleanFilesProcedure.NAME, RepairCorruptedCleanFilesProcedure.builder)
      ,(RepairDeduplicateProcedure.NAME, RepairDeduplicateProcedure.builder)
      ,(RepairMigratePartitionMetaProcedure.NAME, RepairMigratePartitionMetaProcedure.builder)
      ,(RepairOverwriteHoodiePropsProcedure.NAME, RepairOverwriteHoodiePropsProcedure.builder)
      ,(RunCleanProcedure.NAME, RunCleanProcedure.builder)
      ,(ValidateHoodieSyncProcedure.NAME, ValidateHoodieSyncProcedure.builder)
      ,(ShowInvalidParquetProcedure.NAME, ShowInvalidParquetProcedure.builder)
      ,(HiveSyncProcedure.NAME, HiveSyncProcedure.builder)
      ,(CopyToTempViewProcedure.NAME, CopyToTempViewProcedure.builder)
      ,(ShowCommitExtraMetadataProcedure.NAME, ShowCommitExtraMetadataProcedure.builder)
      ,(ShowTablePropertiesProcedure.NAME, ShowTablePropertiesProcedure.builder)
      ,(HelpProcedure.NAME, HelpProcedure.builder)
      ,(ArchiveCommitsProcedure.NAME, ArchiveCommitsProcedure.builder)
      ,(RunTTLProcedure.NAME, RunTTLProcedure.builder)
      ,(DropPartitionProcedure.NAME, DropPartitionProcedure.builder)
      ,(TruncateTableProcedure.NAME, TruncateTableProcedure.builder)
      ,(PartitionBucketIndexManager.NAME, PartitionBucketIndexManager.builder)
      ,(ShowCleansProcedure.NAME, ShowCleansProcedure.builder)
      ,(ShowCleansPartitionMetadataProcedure.NAME, ShowCleansPartitionMetadataProcedure.builder)
      ,(ShowCleansPlanProcedure.NAME, ShowCleansPlanProcedure.builder)
      ,(ShowTimelineProcedure.NAME, ShowTimelineProcedure.builder)
      ,(SetAuditLockProcedure.NAME, SetAuditLockProcedure.builder)
      ,(ShowAuditLockStatusProcedure.NAME, ShowAuditLockStatusProcedure.builder)
      ,(ValidateAuditLockProcedure.NAME, ValidateAuditLockProcedure.builder)
      ,(CleanupAuditLockProcedure.NAME, CleanupAuditLockProcedure.builder)
    )
  }
}

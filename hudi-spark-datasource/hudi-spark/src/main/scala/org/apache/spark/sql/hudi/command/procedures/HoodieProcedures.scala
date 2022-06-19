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

import com.google.common.collect.ImmutableMap

import java.util
import java.util.Locale
import java.util.function.Supplier

object HoodieProcedures {
  private val BUILDERS: util.Map[String, Supplier[ProcedureBuilder]] = initProcedureBuilders

  def newBuilder(name: String): ProcedureBuilder = {
    val builderSupplier: Supplier[ProcedureBuilder] = BUILDERS.get(name.toLowerCase(Locale.ROOT))
    if (builderSupplier != null) builderSupplier.get else null
  }

  private def initProcedureBuilders: util.Map[String, Supplier[ProcedureBuilder]] = {
    val mapBuilder: ImmutableMap.Builder[String, Supplier[ProcedureBuilder]] = ImmutableMap.builder()
    mapBuilder.put(RunCompactionProcedure.NAME, RunCompactionProcedure.builder)
    mapBuilder.put(ShowCompactionProcedure.NAME, ShowCompactionProcedure.builder)
    mapBuilder.put(CreateSavepointsProcedure.NAME, CreateSavepointsProcedure.builder)
    mapBuilder.put(DeleteSavepointsProcedure.NAME, DeleteSavepointsProcedure.builder)
    mapBuilder.put(RollbackSavepointsProcedure.NAME, RollbackSavepointsProcedure.builder)
    mapBuilder.put(RollbackToInstantTimeProcedure.NAME, RollbackToInstantTimeProcedure.builder)
    mapBuilder.put(RunClusteringProcedure.NAME, RunClusteringProcedure.builder)
    mapBuilder.put(ShowClusteringProcedure.NAME, ShowClusteringProcedure.builder)
    mapBuilder.put(ShowCommitsProcedure.NAME, ShowCommitsProcedure.builder)
    mapBuilder.put(ShowCommitsMetadataProcedure.NAME, ShowCommitsMetadataProcedure.builder)
    mapBuilder.put(ShowSavepointsProcedure.NAME, ShowSavepointsProcedure.builder)
    mapBuilder.put(DeleteMarkerProcedure.NAME, DeleteMarkerProcedure.builder)
    mapBuilder.put(ShowRollbacksProcedure.NAME, ShowRollbacksProcedure.builder)
    mapBuilder.put(ShowRollbackDetailProcedure.NAME, ShowRollbackDetailProcedure.builder)
    mapBuilder.put(ExportInstantsProcedure.NAME, ExportInstantsProcedure.builder)
    mapBuilder.put(ShowAllFileSystemViewProcedure.NAME, ShowAllFileSystemViewProcedure.builder)
    mapBuilder.put(ShowLatestFileSystemViewProcedure.NAME, ShowLatestFileSystemViewProcedure.builder)
    mapBuilder.put(ShowHoodieLogFileMetadataProcedure.NAME, ShowHoodieLogFileMetadataProcedure.builder)
    mapBuilder.put(ShowHoodieLogFileRecordsProcedure.NAME, ShowHoodieLogFileRecordsProcedure.builder)
    mapBuilder.put(StatsWriteAmplificationProcedure.NAME, StatsWriteAmplificationProcedure.builder)
    mapBuilder.put(StatsFileSizeProcedure.NAME, StatsFileSizeProcedure.builder)
    mapBuilder.put(HdfsParquetImportProcedure.NAME, HdfsParquetImportProcedure.builder)
    mapBuilder.build
  }
}

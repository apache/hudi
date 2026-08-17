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

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.avro.model.{HoodieClusteringPlan, HoodieRequestedReplaceMetadata}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{ClusteringUtils, Option => HOption}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.io.util.FileIOUtils
import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import java.util
import java.util.Locale
import java.util.function.Supplier

import scala.collection.JavaConverters._

class RepairClusteringPlanProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.required(2, "instant", DataTypes.StringType),
    ProcedureParameter.optional(3, "op", DataTypes.StringType),
    ProcedureParameter.optional(4, "invalid_parquet_files", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "validation_parallelism", DataTypes.IntegerType, 100),
    ProcedureParameter.optional(6, "need_delete", DataTypes.BooleanType, false),
    ProcedureParameter.optional(7, "dry_run", DataTypes.BooleanType, true),
    ProcedureParameter.optional(8, "backup", DataTypes.BooleanType, true),
    ProcedureParameter.optional(9, "allow_empty_plan", DataTypes.BooleanType, false),
    ProcedureParameter.optional(10, "options", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("deleted", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("reason", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val instantTime = getArgValueOrDefault(args, PARAMETERS(2)).get.toString
    val invalidParquetFiles = getArgValueOrDefault(args, PARAMETERS(4)).map(_.toString).getOrElse("")
    val operation = getOperation(getArgValueOrDefault(args, PARAMETERS(3)), invalidParquetFiles)
    val parallelism = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[Int]
    val needDelete = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[Boolean]
    val dryRun = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[Boolean]
    val shouldBackup = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[Boolean]
    val allowEmptyPlan = getArgValueOrDefault(args, PARAMETERS(9)).get.asInstanceOf[Boolean]
    val options = getArgValueOrDefault(args, PARAMETERS(10)).map(_.toString).getOrElse("")

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)
    val (_, clusteringPlan) = RepairClusteringPlanProcedure.getRequestedClusteringPlan(metaClient, instantTime)
    val candidates = getRepairCandidates(operation, invalidParquetFiles, clusteringPlan, parallelism)
    if (candidates.isEmpty) {
      Seq.empty
    } else {
      val candidatePaths = candidates.map(_.path).toSet
      if (!allowEmptyPlan && !RepairClusteringPlanProcedure.hasRetainedInputGroup(clusteringPlan, candidatePaths)) {
        throw new HoodieException(
          s"Repairing clustering instant $instantTime would remove all input groups. "
            + "Set allow_empty_plan => true to allow this operation.")
      }

      if (dryRun) {
        candidates.map(candidate =>
          Row(instantTime, candidate.path, RepairClusteringPlanProcedure.WOULD_REMOVE_FROM_PLAN, false, candidate.reason))
      } else {
        var client: SparkRDDWriteClient[_] = null
        var repairedPaths = Set.empty[String]
        val confs = if (options.trim.isEmpty) Map.empty[String, String] else HoodieCLIUtils.extractOptions(options)
        val tableNameOpt = tableName.map(_.toString)
        try {
          client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, confs, tableNameOpt)
          val transactionOwner = HOption.of(metaClient.getInstantGenerator.createNewInstant(
            HoodieInstant.State.REQUESTED,
            clusteringPlanAction(metaClient, instantTime),
            instantTime))
          val txnManager = client.getTransactionManager
          txnManager.beginStateChange(transactionOwner, metaClient.reloadActiveTimeline().filterCompletedInstants().lastInstant())
          try {
            val latestMetaClient = createMetaClient(jsc, basePath)
            val (latestInstant, latestPlan) = RepairClusteringPlanProcedure.getRequestedClusteringPlan(latestMetaClient, instantTime)
            val latestPlanFiles = RepairClusteringPlanProcedure.getPlanDataFiles(latestPlan).toSet
            val filesToRepair = candidatePaths.intersect(latestPlanFiles)
            if (filesToRepair.nonEmpty) {
              if (!allowEmptyPlan && !RepairClusteringPlanProcedure.hasRetainedInputGroup(latestPlan, filesToRepair)) {
                throw new HoodieException(
                  s"Repairing clustering instant $instantTime would remove all input groups. "
                    + "Set allow_empty_plan => true to allow this operation.")
              }

              val repairedPlan = RepairClusteringPlanProcedure.pruneClusteringPlan(latestPlan, filesToRepair)
              RepairClusteringPlanProcedure.rewriteRequestedClusteringPlan(
                latestMetaClient, latestInstant, repairedPlan, latestPlan.getExtraMetadata, shouldBackup)
              repairedPaths = filesToRepair
            }
          } finally {
            txnManager.endStateChange(transactionOwner)
          }
        } finally {
          if (client != null) {
            client.close()
          }
        }

        val deleteResults = if (needDelete) {
          deletePhysicalFiles(repairedPaths.toSeq)
        } else {
          Map.empty[String, Boolean]
        }

        candidates.map { candidate =>
          val repaired = repairedPaths.contains(candidate.path)
          Row(
            instantTime,
            candidate.path,
            if (repaired) RepairClusteringPlanProcedure.REMOVED_FROM_PLAN else RepairClusteringPlanProcedure.NOT_FOUND_IN_PLAN,
            deleteResults.getOrElse(candidate.path, false),
            candidate.reason)
        }
      }
    }
  }

  private def getOperation(operationArg: Option[Any], invalidParquetFiles: String): String = {
    operationArg
      .map(_.toString.trim)
      .filter(_.nonEmpty)
      .getOrElse {
        if (invalidParquetFiles.trim.nonEmpty) {
          RepairClusteringPlanProcedure.DELETE_OPERATION
        } else {
          RepairClusteringPlanProcedure.VALIDATE_DELETE_OPERATION
        }
      }
      .toLowerCase(Locale.ROOT)
  }

  private def getRepairCandidates(operation: String,
                                  invalidParquetFiles: String,
                                  clusteringPlan: HoodieClusteringPlan,
                                  parallelism: Int): Seq[RepairCandidate] = {
    operation match {
      case RepairClusteringPlanProcedure.DELETE_OPERATION =>
        parseInvalidFiles(invalidParquetFiles).map(path =>
          RepairCandidate(path, RepairClusteringPlanProcedure.USER_REQUESTED))
      case RepairClusteringPlanProcedure.VALIDATE_DELETE_OPERATION | RepairClusteringPlanProcedure.CHECK_AND_DELETE_OPERATION =>
        validateInvalidParquetFiles(clusteringPlan, parallelism).map(path =>
          RepairCandidate(path, RepairClusteringPlanProcedure.NOT_PARQUET_FILE))
      case unsupported =>
        throw new UnsupportedOperationException(
          s"Unsupported operation: '$unsupported'. Supported operations: delete, validate_delete, checkanddelete")
    }
  }

  private def parseInvalidFiles(filesParam: String): Seq[String] = {
    require(filesParam != null && filesParam.trim.nonEmpty,
      "Please set the files to be removed from the clustering plan via the parameter invalid_parquet_files.")

    filesParam.split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct
      .toSeq
  }

  private def validateInvalidParquetFiles(clusteringPlan: HoodieClusteringPlan,
                                          parallelism: Int): Seq[String] = {
    val allFiles = RepairClusteringPlanProcedure.getPlanDataFiles(clusteringPlan).toSeq
    if (allFiles.isEmpty) {
      Seq.empty
    } else {
      val serHadoopConf = new SerializableConfiguration(jsc.hadoopConfiguration())
      val rddParallelism = Math.max(1, Math.min(allFiles.size, parallelism))
      jsc.parallelize(allFiles.asJava, rddParallelism).rdd.filter { path =>
        var isInvalid = false
        if (path.endsWith(".parquet")) {
          try {
            ParquetFileReader.readFooter(serHadoopConf.value, new Path(path), SKIP_ROW_GROUPS).getFileMetaData
          } catch {
            case e: Exception =>
              isInvalid = Option(e.getMessage).exists(_.contains("is not a Parquet file"))
          }
        }
        isInvalid
      }.collect().toSeq
    }
  }

  private def deletePhysicalFiles(filesToDelete: Seq[String]): Map[String, Boolean] = {
    val storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())
    filesToDelete.map { filePath =>
      val path = new Path(filePath)
      val fs = HadoopFSUtils.getFs(path, storageConf.unwrap())
      val deleted = if (fs.exists(path)) {
        if (!fs.delete(path, false)) {
          throw new HoodieException(s"Failed to delete invalid parquet file after repairing clustering plan: $filePath")
        }
        true
      } else {
        false
      }
      filePath -> deleted
    }.toMap
  }

  private def clusteringPlanAction(metaClient: HoodieTableMetaClient, instantTime: String): String = {
    RepairClusteringPlanProcedure.getRequestedClusteringPlan(metaClient, instantTime)._1.getAction
  }

  override def build: Procedure = new RepairClusteringPlanProcedure()
}

object RepairClusteringPlanProcedure {
  val NAME = "repair_clustering_plan"

  val DELETE_OPERATION = "delete"
  val VALIDATE_DELETE_OPERATION = "validate_delete"
  val CHECK_AND_DELETE_OPERATION = "checkanddelete"

  val WOULD_REMOVE_FROM_PLAN = "WOULD_REMOVE_FROM_PLAN"
  val REMOVED_FROM_PLAN = "REMOVED_FROM_PLAN"
  val NOT_FOUND_IN_PLAN = "NOT_FOUND_IN_PLAN"
  val USER_REQUESTED = "USER_REQUESTED"
  val NOT_PARQUET_FILE = "NOT_PARQUET_FILE"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new RepairClusteringPlanProcedure()
  }

  private def getRequestedClusteringPlan(metaClient: HoodieTableMetaClient,
                                         instantTime: String): (HoodieInstant, HoodieClusteringPlan) = {
    val pendingPlan = ClusteringUtils.getAllPendingClusteringPlans(metaClient).iterator().asScala
      .find(plan => plan.getLeft.requestedTime == instantTime && plan.getLeft.isRequested)
      .getOrElse {
        throw new HoodieException(
          s"The requested clustering plan for instant $instantTime does not exist. Modifications are not supported.")
      }

    (pendingPlan.getLeft, pendingPlan.getRight)
  }

  private def getPlanDataFiles(plan: HoodieClusteringPlan): Iterable[String] = {
    Option(plan.getInputGroups)
      .map(_.asScala)
      .getOrElse(Seq.empty)
      .flatMap(group => Option(group.getSlices).map(_.asScala).getOrElse(Seq.empty))
      .map(_.getDataFilePath)
      .filter(path => path != null && path.nonEmpty)
  }

  private def hasRetainedInputGroup(plan: HoodieClusteringPlan, filesToRemove: Set[String]): Boolean = {
    Option(plan.getInputGroups)
      .map(_.asScala)
      .getOrElse(Seq.empty)
      .exists { group =>
        Option(group.getSlices)
          .map(_.asScala)
          .getOrElse(Seq.empty)
          .exists(slice => !filesToRemove.contains(slice.getDataFilePath))
      }
  }

  private def pruneClusteringPlan(plan: HoodieClusteringPlan,
                                  filesToRemove: Set[String]): HoodieClusteringPlan = {
    val retainedGroups = Option(plan.getInputGroups)
      .map(_.asScala)
      .getOrElse(Seq.empty)
      .map { group =>
        val retainedSlices = Option(group.getSlices)
          .map(_.asScala)
          .getOrElse(Seq.empty)
          .filter(slice => !filesToRemove.contains(slice.getDataFilePath))
          .asJava
        group.setSlices(retainedSlices)
        group
      }
      .filter(group => group.getSlices != null && !group.getSlices.isEmpty)
      .asJava

    plan.setInputGroups(retainedGroups)
    plan
  }

  private def rewriteRequestedClusteringPlan(metaClient: HoodieTableMetaClient,
                                             clusteringInstant: HoodieInstant,
                                             clusteringPlan: HoodieClusteringPlan,
                                             extraMetadata: util.Map[String, String],
                                             shouldBackup: Boolean): Unit = {
    val activeTimeline = metaClient.getActiveTimeline
    val instantFileName = metaClient.getInstantFileNameGenerator.getFileName(clusteringInstant)
    val instantPath = new StoragePath(metaClient.getTimelinePath, instantFileName)
    val backupDir = new StoragePath(
      metaClient.getMetaPath,
      s".repair/repair_clustering_plan/${clusteringInstant.requestedTime}/${System.currentTimeMillis()}")
    val backupPath = new StoragePath(backupDir, instantFileName)

    try {
      if (shouldBackup) {
        activeTimeline.copyInstant(clusteringInstant, backupDir)
      }
      activeTimeline.deleteInstantFileIfExists(clusteringInstant)

      val requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder
        .setOperationType(WriteOperationType.CLUSTER.name())
        .setExtraMetadata(Option(extraMetadata).getOrElse(new util.HashMap[String, String]()))
        .setClusteringPlan(clusteringPlan)
        .build()

      clusteringInstant.getAction match {
        case HoodieTimeline.CLUSTERING_ACTION =>
          activeTimeline.saveToPendingClusterCommit(clusteringInstant, requestedReplaceMetadata)
        case HoodieTimeline.REPLACE_COMMIT_ACTION =>
          activeTimeline.saveToPendingReplaceCommit(clusteringInstant, requestedReplaceMetadata)
        case action =>
          throw new HoodieException(s"Unsupported clustering instant action: $action")
      }

      if (!metaClient.getStorage.exists(instantPath)) {
        throw new HoodieException(s"Failed to rewrite requested clustering instant: $clusteringInstant")
      }
    } catch {
      case e: Exception =>
        restoreRequestedInstantIfNeeded(metaClient, instantPath, backupPath, shouldBackup)
        throw new HoodieException(s"Failed to repair clustering plan for instant ${clusteringInstant.requestedTime}", e)
    }
  }

  private def restoreRequestedInstantIfNeeded(metaClient: HoodieTableMetaClient,
                                              instantPath: StoragePath,
                                              backupPath: StoragePath,
                                              shouldBackup: Boolean): Unit = {
    if (shouldBackup && !metaClient.getStorage.exists(instantPath) && metaClient.getStorage.exists(backupPath)) {
      FileIOUtils.copy(metaClient.getStorage, backupPath, metaClient.getStorage, instantPath, false, false)
    }
  }
}

case class RepairCandidate(path: String, reason: String)

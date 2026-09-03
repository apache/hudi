/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.comet

import org.apache.hudi.{HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieLogFile}
import org.apache.hudi.storage.StoragePath

import org.apache.comet.rules.CometExternalV1ScanProvider
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.comet.{CometHudiNativeScanExec, SerializedPlan}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Converts a Hudi data source V1 scan into a Comet native scan that reads file slices through
 * hudi-rs. Registered with Comet's scan rule through the JDK ServiceLoader.
 *
 * Conversion is conservative: any shape outside the supported set (snapshot reads of Parquet
 * tables whose partition columns are stored in the data files, with primitive output types)
 * returns None, which leaves the plan on Hudi's JVM scan.
 */
class HudiCometScanProvider extends CometExternalV1ScanProvider with Logging {

  override def supportsFormat(fileFormat: FileFormat): Boolean =
    fileFormat.isInstanceOf[HoodieFileGroupReaderBasedFileFormat]

  override def convert(
      session: SparkSession,
      scanExec: FileSourceScanExec): Option[SparkPlan] = {
    if (!session.conf
        .getOption(HudiCometScanProvider.SCAN_ENABLED_KEY)
        .exists(_.toBoolean)) {
      return None
    }
    val format =
      scanExec.relation.fileFormat.asInstanceOf[HoodieFileGroupReaderBasedFileFormat]
    if (format.isIncrementalQuery || format.isBootstrapTable ||
      format.isMultipleBaseFileFormats || format.getRequiredFilters.nonEmpty) {
      return None
    }
    if (scanExec.requiredSchema.isEmpty) {
      return None
    }
    if (scanExec.partitionFilters.exists(
        _.find(_.isInstanceOf[DynamicPruningExpression]).isDefined)) {
      return None
    }
    if (scanExec.output.exists(attr =>
        attr.dataType.isInstanceOf[StructType] || attr.dataType.isInstanceOf[ArrayType] ||
          attr.dataType.isInstanceOf[MapType])) {
      return None
    }

    val fileIndex = scanExec.relation.location match {
      case index: HoodieFileIndex => index
      case _ => return None
    }
    val tableConfig = fileIndex.metaClient.getTableConfig
    val tableVersion = tableConfig.getTableVersion.versionCode()
    // The versions the hudi-rs file group reader accepts. Merge-on-read
    // additionally requires 8+, where log files are sealed per commit, so the
    // planned file paths fully determine the data the reader must see.
    if (!HudiCometScanProvider.SUPPORTED_TABLE_VERSIONS.contains(tableVersion) ||
      (format.isMORTable && tableVersion < 8)) {
      return None
    }
    if (tableConfig.shouldDropPartitionColumns()) {
      return None
    }
    if (tableConfig.getBaseFileFormat != HoodieFileFormat.PARQUET) {
      return None
    }
    // Every output column must come out of the data files: the native reader
    // has no partition-value append path. Partition columns are stored in the
    // files (dropped partition columns are gated out above), but the JVM read
    // appends the value parsed from the partition path instead, and the two
    // agree only when the key generator writes values into the path unchanged.
    // Generators that transform values (timestamp-based, custom) stay on the
    // JVM scan whenever a partition column is projected.
    val partitionColumns = scanExec.relation.partitionSchema.fieldNames.toSet
    val availableColumns = scanExec.relation.dataSchema.fieldNames.toSet ++ partitionColumns
    if (!scanExec.output.forall(attr => availableColumns.contains(attr.name))) {
      return None
    }
    if (scanExec.output.exists(attr => partitionColumns.contains(attr.name)) &&
      !HudiCometScanProvider.IDENTITY_KEY_GENERATORS.contains(
        Option(tableConfig.getKeyGeneratorClassName)
          .getOrElse("org.apache.hudi.keygen.SimpleKeyGenerator"))) {
      return None
    }

    val baseUri = fileIndex.metaClient.getBasePath.toString
    val slices = extractFileSlices(scanExec, baseUri, format.isMORTable) match {
      case Some(s) => s
      case None => return None
    }

    val scanHashCode = scanExec.canonicalized.hashCode()
    val commonBuilder = OperatorOuterClass.HudiScanCommon
      .newBuilder()
      .setTableBaseUri(baseUri)
      .setSessionTimezone(session.sessionState.conf.sessionLocalTimeZone)
      .setScanHashCode(scanHashCode)
      // Bounds the log-file scan to instants at or before the query instant,
      // exactly as the JVM file group reader does with the same timestamp; a
      // snapshot read passes the latest completed instant, a time-travel read
      // the requested one. Base files need no bound: the driver's listing only
      // names base files from admitted commits.
      .putOptions("hoodie.read.end.timestamp", format.getQueryTimestamp)
    for (attr <- scanExec.output) {
      val field = OperatorOuterClass.SparkStructField
        .newBuilder()
        .setName(attr.name)
        .setNullable(attr.nullable)
      serializeDataType(attr.dataType) match {
        case Some(dataType) => field.setDataType(dataType)
        case None => return None
      }
      commonBuilder.addRequiredSchema(field)
    }
    val commonBytes = commonBuilder.build().toByteArray

    val perPartitionBytes = slices.map { slice =>
      OperatorOuterClass.HudiScan
        .newBuilder()
        .addFileSlices(
          OperatorOuterClass.HudiFileSlice
            .newBuilder()
            .setBaseFilePath(slice._1)
            .addAllLogFilePaths(slice._2.asJava))
        .build()
        .toByteArray
    }.toArray

    // The plan carries only the identity of the scan; the full common block and each
    // partition's slices are injected from the byte arrays above at execution time.
    val placeholder = OperatorOuterClass.Operator
      .newBuilder()
      .setPlanId(scanExec.id)
      .setHudiScan(
        OperatorOuterClass.HudiScan
          .newBuilder()
          .setCommon(
            OperatorOuterClass.HudiScanCommon
              .newBuilder()
              .setTableBaseUri(baseUri)
              .setScanHashCode(scanHashCode)))
      .build()

    logInfo(
      s"Converting Hudi scan of $baseUri to a Comet native scan " +
        s"(${perPartitionBytes.length} file slices)")
    Some(
      CometHudiNativeScanExec(
        placeholder,
        scanExec.output,
        scanExec,
        SerializedPlan(None),
        baseUri,
        scanHashCode,
        commonBytes,
        perPartitionBytes))
  }

  /**
   * One (base file path, log file paths) pair per file slice, with every path relative to the
   * table base as the hudi-rs reader expects, or None when any partition holds a shape the
   * native reader does not take: CDC file groups, a slice with no base file, or a file outside
   * the table base path.
   */
  private def extractFileSlices(
      scanExec: FileSourceScanExec,
      baseUri: String,
      isMor: Boolean): Option[Seq[(String, Seq[String])]] = {
    val basePrefix =
      if (baseUri.endsWith(StoragePath.SEPARATOR)) baseUri else baseUri + StoragePath.SEPARATOR
    def relativize(path: String): Option[String] = {
      val normalized = new StoragePath(path).toString
      if (normalized.startsWith(basePrefix)) {
        Some(normalized.substring(basePrefix.length))
      } else {
        None
      }
    }
    val slices = new ArrayBuffer[(String, Seq[String])]()
    for (partition <- scanExec.selectedPartitions) {
      partition.values match {
        case _: HoodiePartitionCDCFileGroupMapping => return None
        case mapping: HoodiePartitionFileSliceMapping =>
          for (file <- partition.files) {
            val filePath = file.getPath.toString
            val fileId = FSUtils.getFileIdFromFilePath(new StoragePath(filePath))
            mapping.getSlice(fileId) match {
              case Some(slice) =>
                if (!slice.getBaseFile.isPresent) {
                  return None
                }
                // A log-bearing slice outside a merge-on-read snapshot read
                // (e.g. a read-optimized query) must not be merged natively.
                if (!isMor && slice.getLogFiles.findAny().isPresent) {
                  return None
                }
                val base = relativize(slice.getBaseFile.get().getPath) match {
                  case Some(p) => p
                  case None => return None
                }
                val logFiles = new ArrayBuffer[String]()
                for (logFile <- slice.getLogFiles
                    .sorted(HoodieLogFile.getLogFileComparator)
                    .iterator()
                    .asScala) {
                  relativize(logFile.getPath.toString) match {
                    case Some(p) => logFiles += p
                    case None => return None
                  }
                }
                slices += ((base, logFiles.toSeq))
              case None =>
                relativize(filePath) match {
                  case Some(p) => slices += ((p, Seq.empty))
                  case None => return None
                }
            }
          }
        case _ =>
          for (file <- partition.files) {
            relativize(file.getPath.toString) match {
              case Some(p) => slices += ((p, Seq.empty))
              case None => return None
            }
          }
      }
    }
    Some(slices.toSeq)
  }
}

object HudiCometScanProvider {

  /** Session config gating the native Hudi scan; off unless set to true. */
  val SCAN_ENABLED_KEY = "spark.comet.scan.hudi.enabled"

  val SUPPORTED_TABLE_VERSIONS: Set[Int] = Set(6, 8, 9)

  /** Key generators that write partition values into the path unchanged. */
  val IDENTITY_KEY_GENERATORS: Set[String] = Set(
    "org.apache.hudi.keygen.SimpleKeyGenerator",
    "org.apache.hudi.keygen.ComplexKeyGenerator",
    "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
}

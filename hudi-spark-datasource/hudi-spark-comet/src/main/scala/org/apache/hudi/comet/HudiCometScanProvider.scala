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

import org.apache.hudi.{DataSourceReadOptions, HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, SparkAdapterSupport}
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieReaderConfig, RecordMergeMode}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieLogFile}
import org.apache.hudi.storage.StoragePath

import org.apache.comet.CometConf
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.rules.CometExternalV1ScanProvider
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PlanExpression
import org.apache.spark.sql.comet.{CometHudiNativeScanExec, SerializedPlan}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType}

import java.util.IdentityHashMap

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
class HudiCometScanProvider extends CometExternalV1ScanProvider with SparkAdapterSupport with Logging {

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
    val relation = scanExec.relation
    val format = relation.fileFormat.asInstanceOf[HoodieFileGroupReaderBasedFileFormat]
    if (format.isIncrementalQuery || format.isBootstrapTable ||
      format.isMultipleBaseFileFormats || format.getRequiredFilters.nonEmpty) {
      return None
    }
    if (scanExec.requiredSchema.isEmpty) {
      return None
    }
    // Spark lists the partitions without the filters that hold a subquery (a
    // scalar subquery, or a dynamic partition pruning filter) and applies
    // those at execution time; the listing read below has not been pruned by
    // them.
    if (scanExec.partitionFilters.exists(_.exists(_.isInstanceOf[PlanExpression[_]]))) {
      return None
    }
    if (scanExec.output.exists(attr =>
        attr.dataType.isInstanceOf[StructType] || attr.dataType.isInstanceOf[ArrayType] ||
          attr.dataType.isInstanceOf[MapType])) {
      return None
    }
    // hudi-rs converts the whole table schema, projected or not, and has no
    // Arrow mapping for Avro's local timestamps yet.
    if (relation.dataSchema.exists(_.dataType == TimestampNTZType)) {
      return None
    }

    // The guards Comet applies to its own native scan. The external provider
    // hook runs before them, so they are repeated here.
    val conf = session.sessionState.conf
    if (!CometConf.COMET_EXEC_ENABLED.get(conf)) {
      return None
    }
    if (conf.ignoreCorruptFiles || conf.ignoreMissingFiles ||
      relation.options.get("ignorecorruptfiles").contains("true") ||
      relation.options.get("ignoremissingfiles").contains("true")) {
      return None
    }
    if (!conf.parquetVectorizedReaderEnabled &&
      !CometConf.COMET_SCAN_ALLOW_DISABLED_PARQUET_VECTORIZED_READER.get(conf)) {
      return None
    }
    if (CometParquetUtils.encryptionEnabled(
        session.sessionState.newHadoopConfWithOptions(relation.options))) {
      return None
    }

    // Read options that change what the JVM read returns and that hudi-rs
    // does not take. Hudi folds the session's hoodie.* confs into the relation
    // options; the session is checked too in case a caller set one later.
    def readOption(key: String): Option[String] =
      relation.options.get(key).orElse(session.conf.getOption(key))
    if (readOption(HoodieReaderConfig.MERGE_TYPE.key)
        .exists(_.equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE))) {
      return None
    }
    if (readOption(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key)
        .exists(_.toBoolean)) {
      return None
    }
    // Schema on read resolves renamed and retyped columns through the table's
    // internal schema history, which only the JVM reader has.
    if (readOption(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key).exists(_.toBoolean)) {
      return None
    }

    val fileIndex = relation.location match {
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
    // hudi-rs refuses to open a table with more than one ordering field, and
    // its log merge implements only the two built-in merge modes: a CUSTOM
    // mode (a payload class or merger) and the LSM layout's version collapse
    // have no native counterpart.
    if (tableConfig.getOrderingFields.size > 1 || tableConfig.isLSMTreeStorageLayout) {
      return None
    }
    if (format.isMORTable) {
      val mergeMode = tableConfig.getRecordMergeMode
      if (mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING &&
        mergeMode != RecordMergeMode.EVENT_TIME_ORDERING) {
        return None
      }
    }
    val baseUri = fileIndex.metaClient.getBasePath.toString
    // hudi-rs opens the table through object_store, which resolves only these
    // schemes; any other would fail at task time instead of falling back.
    val scheme = Option(new StoragePath(baseUri).toUri.getScheme).getOrElse("file")
    if (!HudiCometScanProvider.SUPPORTED_SCHEMES.contains(scheme.toLowerCase)) {
      return None
    }
    // Every output column must come out of the data files: the native reader
    // has no partition-value append path. Partition columns are stored in the
    // files (dropped partition columns are gated out above), but the JVM read
    // appends the value parsed back from the partition path instead, and the
    // two agree only when the key generator writes values into the path
    // unchanged and the type's path form round-trips. Generators that
    // transform values (timestamp-based, custom) and types whose rendering
    // loses information (a timestamp keeps milliseconds and follows the JVM
    // time zone) stay on the JVM scan. A scan of a partitioned table always
    // outputs its partition columns, whatever the query projects.
    val partitionSchema = relation.partitionSchema
    val partitionColumns = partitionSchema.fieldNames.toSet
    val availableColumns = relation.dataSchema.fieldNames.toSet ++ partitionColumns
    if (!scanExec.output.forall(attr => availableColumns.contains(attr.name))) {
      return None
    }
    if (scanExec.output.exists(attr => partitionColumns.contains(attr.name))) {
      if (!HudiCometScanProvider.IDENTITY_KEY_GENERATORS.contains(
          Option(tableConfig.getKeyGeneratorClassName)
            .getOrElse("org.apache.hudi.keygen.SimpleKeyGenerator"))) {
        return None
      }
      if (!partitionSchema.forall(field =>
          HudiCometScanProvider.PATH_ROUND_TRIP_PARTITION_TYPES.contains(field.dataType))) {
        return None
      }
      // A null or empty partition value is written under the default partition
      // path, which the JVM read parses back as null while the data file holds
      // what was written.
      if (scanExec.selectedPartitions.exists(partition =>
          partitionSchema.indices.exists(partition.values.isNullAt))) {
        return None
      }
    }

    val tasks = planTasks(session, scanExec, baseUri, format.isMORTable) match {
      case Some(t) => t
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
      .putOptions("hoodie.read.as.of.timestamp", format.getQueryTimestamp)
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

    val perPartitionBytes = tasks.map { slices =>
      val builder = OperatorOuterClass.HudiScan.newBuilder()
      for (slice <- slices) {
        builder.addFileSlices(
          OperatorOuterClass.HudiFileSlice
            .newBuilder()
            .setBaseFilePath(slice._1)
            .addAllLogFilePaths(slice._2.asJava))
      }
      builder.build().toByteArray
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
        s"(${tasks.map(_.size).sum} file slices in ${tasks.size} tasks)")
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
   * The file slices of each task, as (base file path, log file paths) pairs with every path
   * relative to the table base as the hudi-rs reader expects, or None when any partition holds a
   * shape the native reader does not take: CDC file groups, a slice with no base file, or a file
   * outside the table base path.
   *
   * Slices are packed into tasks the way Spark's file scan packs its files, by base file size up
   * to the same split size, so small files share a task. A base file larger than the split size
   * is one task here, where Spark's copy-on-write read would split it by byte range.
   */
  private def planTasks(
      session: SparkSession,
      scanExec: FileSourceScanExec,
      baseUri: String,
      isMor: Boolean): Option[Seq[Seq[(String, Seq[String])]]] = {
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
    val partitionedFileUtils = sparkAdapter.getSparkPartitionedFileUtils
    val files = new ArrayBuffer[PartitionedFile]()
    val slicesByFile = new IdentityHashMap[PartitionedFile, (String, Seq[String])]()
    for (partition <- scanExec.selectedPartitions) {
      for (file <- partition.files) {
        val filePath = file.getPath.toString
        val slice: (String, Seq[String]) = partition.values match {
          case _: HoodiePartitionCDCFileGroupMapping => return None
          case mapping: HoodiePartitionFileSliceMapping =>
            val fileId = FSUtils.getFileIdFromFilePath(new StoragePath(filePath))
            mapping.getSlice(fileId) match {
              case Some(fileSlice) =>
                if (!fileSlice.getBaseFile.isPresent) {
                  return None
                }
                // A log-bearing slice outside a merge-on-read snapshot read
                // (e.g. a read-optimized query) must not be merged natively.
                if (!isMor && fileSlice.getLogFiles.findAny().isPresent) {
                  return None
                }
                val base = relativize(fileSlice.getBaseFile.get().getPath) match {
                  case Some(p) => p
                  case None => return None
                }
                val logFiles = new ArrayBuffer[String]()
                for (logFile <- fileSlice.getLogFiles
                    .sorted(HoodieLogFile.getLogFileComparator)
                    .iterator()
                    .asScala) {
                  relativize(logFile.getPath.toString) match {
                    case Some(p) => logFiles += p
                    case None => return None
                  }
                }
                (base, logFiles.toSeq)
              case None =>
                relativize(filePath) match {
                  case Some(p) => (p, Seq.empty)
                  case None => return None
                }
            }
          case _ =>
            relativize(filePath) match {
              case Some(p) => (p, Seq.empty)
              case None => return None
            }
        }
        val partitionedFile = partitionedFileUtils.createPartitionedFile(
          partition.values, new StoragePath(filePath), 0, file.getLen)
        files += partitionedFile
        slicesByFile.put(partitionedFile, slice)
      }
    }
    // Largest first, as Spark orders its splits before packing them.
    val sortedFiles = files.sortBy(_.length)(Ordering[Long].reverse).toSeq
    val maxSplitBytes = FilePartition.maxSplitBytes(session, scanExec.selectedPartitions)
    Some(sparkAdapter.getFilePartitions(session, sortedFiles, maxSplitBytes)
      .map(_.files.map(slicesByFile.get).toSeq))
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

  /** Partition column types whose value survives the path rendering and parse the JVM read does. */
  val PATH_ROUND_TRIP_PARTITION_TYPES: Set[DataType] =
    Set(StringType, IntegerType, LongType, ShortType, ByteType, DateType, BooleanType)

  /** URI schemes the object_store crate behind hudi-rs resolves. */
  val SUPPORTED_SCHEMES: Set[String] =
    Set("file", "s3", "s3a", "gs", "abfs", "abfss", "az", "adl", "azure")
}

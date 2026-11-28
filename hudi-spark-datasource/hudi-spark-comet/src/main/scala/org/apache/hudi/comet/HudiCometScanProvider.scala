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
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.rules.CometExternalV1ScanProvider
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.QueryPlanSerde.serializeDataType
import org.apache.comet.serde.operator.HudiBaseFileScanSerde

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PlanExpression
import org.apache.spark.sql.comet.{CometHudiNativeScanExec, SerializedPlan}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.{HoodieFileGroupReaderBasedFileFormat, ParquetOptions}
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType}

import java.lang.ref.WeakReference
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
    // those once the subqueries resolve at execution. The checks below run on
    // that unpruned listing, a superset of what the scan reads, and the exec
    // node plans its tasks again after pruning.
    val runtimeFilters =
      scanExec.partitionFilters.filter(_.exists(_.isInstanceOf[PlanExpression[_]]))
    if (scanExec.output.exists(attr =>
        attr.dataType.isInstanceOf[StructType] || attr.dataType.isInstanceOf[ArrayType] ||
          attr.dataType.isInstanceOf[MapType])) {
      return None
    }
    // hudi-rs converts the whole table schema, projected or not, and its
    // mapping of Avro's local timestamps is not validated against the JVM read.
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
    val hadoopConf = session.sessionState.newHadoopConfWithOptions(relation.options)
    if (CometParquetUtils.encryptionEnabled(hadoopConf)) {
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
    // The native scan reads through object_store, which resolves only these
    // schemes; any other would fail at task time instead of falling back.
    val scheme = Option(new StoragePath(baseUri).toUri.getScheme).getOrElse("file")
    if (!HudiCometScanProvider.SUPPORTED_SCHEMES.contains(scheme.toLowerCase)) {
      return None
    }
    // An S3 or ABFS table is read through the object store Comet's own native
    // scan would build for these files, so the same Hadoop settings (fs.s3a.*
    // keys, per-bucket overrides, credential providers) apply. Every table
    // file lives under the base path.
    val objectStoreOptions = NativeConfig.extractScanObjectStoreOptions(
      hadoopConf,
      Seq(new StoragePath(baseUri).toUri)) match {
      case Right(options) => options
      case Left(_) => return None
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
      if (HudiCometScanProvider.eligibilityListing(scanExec, fileIndex).exists(partition =>
          partitionSchema.indices.exists(partition.values.isNullAt))) {
        return None
      }
    }

    // The tasks are planned when the scan executes, over the partitions it lists then, so
    // listing runs inside the query's stages as Spark's own scan does. Here every slice of a
    // listing without data filters, a superset of what the scan reads, is only checked.
    if (!HudiCometScanProvider.canPlan(
        session, HudiCometScanProvider.eligibilityListing(scanExec, fileIndex), baseUri,
        format.isMORTable)) {
      return None
    }

    // Runtime filters still hold AQE's pruning placeholders here, which differ between copies of
    // the same scan; leaving them out lets equal scans share an exchange or query stage.
    val canonicalScan = scanExec
      .copy(partitionFilters = scanExec.partitionFilters.filterNot(runtimeFilters.contains))
      .canonicalized
    val scanHashCode = canonicalScan.hashCode()
    // Every instant of every action on the active timeline, with its state and completion time,
    // plus the table config, so the value changes whenever anything is added, completed, rolled
    // back or restored, and whenever a table property changes without a commit.
    val timelineSnapshot = {
      val metaClient = fileIndex.metaClient
      val instants = metaClient.getActiveTimeline.getInstants.asScala
      val digest = java.security.MessageDigest.getInstance("SHA-256")
      def add(entry: String): Unit =
        digest.update(entry.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      instants.foreach { instant =>
        add(s"${instant.getState},${instant.getAction},${instant.requestedTime}," +
          s"${instant.getCompletionTime};")
      }
      val props = metaClient.getTableConfig.getProps
      props.stringPropertyNames.asScala.toSeq.sorted.foreach { name =>
        add(s"$name=${props.getProperty(name)};")
      }
      s"${instants.size}-${digest.digest().map("%02x".format(_)).mkString}"
    }
    val commonBuilder = OperatorOuterClass.HudiScanCommon
      .newBuilder()
      .setTableBaseUri(baseUri)
      .setSessionTimezone(session.sessionState.conf.sessionLocalTimeZone)
      .setScanHashCode(scanHashCode)
      .setTimelineSnapshot(timelineSnapshot)
      // Bounds the log-file scan to instants at or before the query instant,
      // exactly as the JVM file group reader does with the same timestamp; a
      // snapshot read passes the latest completed instant, a time-travel read
      // the requested one. Base files need no bound: the driver's listing only
      // names base files from admitted commits.
      .putOptions("hoodie.read.as.of.timestamp", format.getQueryTimestamp)
      .putAllObjectStoreOptions(objectStoreOptions.asJava)
    // A file slice with no log files is its base file, which the JVM scan reads with Spark's
    // Parquet reader. Comet's native Parquet scan reads those slices here, with the scan's data
    // filters, unless the read is one where the two could differ:
    //  - a time-travel read, kept whole on the reader that bounds it by the query instant;
    //  - a table with a millisecond timestamp, whose older base files the JVM reader repairs
    //    against the table schema;
    //  - a table partitioned on a nested field, whose partition column the files store nested
    //    while the JVM scan takes its value from the path;
    //  - a read Comet's Parquet scan declines.
    // A base file that stores a column as another type than the table's, or lacks a column
    // other than a table data column, is found from its footer when it is read, and stays on
    // the Hudi reader.
    val baseFilesAsParquet = session.conf
      .getOption(HudiCometScanProvider.BASE_FILES_AS_PARQUET_ENABLED_KEY)
      .forall(_.toBoolean) &&
      readOption(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key).isEmpty &&
      !format.hasTimestampMillisField &&
      !partitionColumns.exists(_.contains(".")) &&
      !tableConfig.getPartitionFields.orElse(Array.empty[String]).exists(_.contains("."))
    if (baseFilesAsParquet) {
      HudiBaseFileScanSerde.nativeScanCommon(
        s"HudiScan $baseUri",
        scanExec.output,
        scanExec.dataFilters,
        objectStoreOptions,
        hadoopConf,
        conf).foreach { parquetScan =>
        val parquetOptions = new ParquetOptions(relation.options, scanExec.conf)
        commonBuilder
          .setParquetScan(parquetScan)
          .addAllTableDataColumns(
            relation.dataSchema.fieldNames.filterNot(partitionColumns.contains).toSeq.asJava)
          .setDatetimeRebaseModeInRead(parquetOptions.datetimeRebaseModeInRead)
          .setInt96RebaseModeInRead(parquetOptions.int96RebaseModeInRead)
      }
    }
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
      s"Converting Hudi scan of $baseUri to a Comet native scan")
    Some(
      CometHudiNativeScanExec(
        placeholder,
        scanExec.output,
        scanExec,
        SerializedPlan(None),
        baseUri,
        canonicalScan,
        scanHashCode,
        commonBytes,
        format.isMORTable,
        runtimeFilters))
  }
}

object HudiCometScanProvider extends SparkAdapterSupport {

  /**
   * Listings without data filters, by the file index's timeline instance and then by file index,
   * query instant and canonical static partition filters. AQE plans a query again after each
   * stage with new scan nodes over the same relation, and scans that differ only in data filters
   * share one entry; leaving data filters out also skips Hudi's data skipping, which only the
   * executed scan needs. Refreshing a file index reloads its timeline and so starts new entries;
   * the old ones go when their timeline is collected.
   */
  private val listings = new java.util.WeakHashMap[
    AnyRef, java.util.Map[AnyRef, (WeakReference[HoodieFileIndex], Seq[PartitionDirectory])]]()

  private val MaxListingsPerTimeline = 32

  /** The partition directories `scan` selects, through the Spark version's adapter. */
  def selectedPartitionDirectories(scan: FileSourceScanExec): Seq[PartitionDirectory] =
    sparkAdapter.getSparkPartitionedFileUtils.selectedPartitionDirectories(scan)

  /** The partitions `scanExec` lists without its data filters: a superset of what it reads. */
  def eligibilityListing(
      scanExec: FileSourceScanExec,
      fileIndex: HoodieFileIndex): Seq[PartitionDirectory] = {
    val unfiltered = scanExec.copy(dataFilters = Nil)
    val queryTimestamp = scanExec.relation.fileFormat match {
      case format: HoodieFileGroupReaderBasedFileFormat => format.getQueryTimestamp
      case _ => return selectedPartitionDirectories(unfiltered)
    }
    // Timelines compare by identity, and the file index lists from the one its meta client holds.
    val timeline = fileIndex.metaClient.getActiveTimeline
    val staticFilters =
      scanExec.partitionFilters.filterNot(_.exists(_.isInstanceOf[PlanExpression[_]]))
    val key =
      (System.identityHashCode(fileIndex), queryTimestamp, staticFilters.map(_.canonicalized))
    val byKey = listings.synchronized {
      var map = listings.get(timeline)
      if (map == null) {
        map = java.util.Collections.synchronizedMap(new java.util.LinkedHashMap[
          AnyRef, (WeakReference[HoodieFileIndex], Seq[PartitionDirectory])](16, 0.75f, true) {
          override def removeEldestEntry(eldest: java.util.Map.Entry[
              AnyRef, (WeakReference[HoodieFileIndex], Seq[PartitionDirectory])]): Boolean =
            size() > MaxListingsPerTimeline
        })
        listings.put(timeline, map)
      }
      map
    }
    val cached = byKey.get(key)
    if (cached != null && (cached._1.get eq fileIndex)) {
      cached._2
    } else {
      val listed = selectedPartitionDirectories(unfiltered)
      byKey.put(key, (new WeakReference(fileIndex), listed))
      listed
    }
  }

  /** Session config gating the native Hudi scan; off unless set to true. */
  val SCAN_ENABLED_KEY = "spark.comet.scan.hudi.enabled"

  /**
   * Session config for reading file slices that have no log files through Comet's native Parquet
   * scan instead of the Hudi reader; on unless set to false.
   */
  val BASE_FILES_AS_PARQUET_ENABLED_KEY = "spark.comet.scan.hudi.baseFilesAsParquet.enabled"

  private val PARQUET_EXTENSION = HoodieFileFormat.PARQUET.getFileExtension

  val SUPPORTED_TABLE_VERSIONS: Set[Int] = Set(6, 8, 9)

  /** Key generators that write partition values into the path unchanged. */
  val IDENTITY_KEY_GENERATORS: Set[String] = Set(
    "org.apache.hudi.keygen.SimpleKeyGenerator",
    "org.apache.hudi.keygen.ComplexKeyGenerator",
    "org.apache.hudi.keygen.NonpartitionedKeyGenerator")

  /** Partition column types whose value survives the path rendering and parse the JVM read does. */
  val PATH_ROUND_TRIP_PARTITION_TYPES: Set[DataType] =
    Set(StringType, IntegerType, LongType, ShortType, ByteType, DateType, BooleanType)

  /** URI schemes the object_store crate behind the native scan resolves. */
  val SUPPORTED_SCHEMES: Set[String] =
    Set("file", "s3", "s3a", "gs", "abfs", "abfss", "az", "adl", "azure")

  /** Whether every slice in `partitions` is one the native reader takes. */
  def canPlan(
      session: SparkSession,
      partitions: Seq[PartitionDirectory],
      baseUri: String,
      isMor: Boolean): Boolean =
    planTasks(session, partitions, baseUri, isMor).isDefined

  /**
   * The serialized file slices of each task over `partitions`, or None when a partition holds a
   * shape the native reader does not take (see [[planTasks]]).
   */
  def planPartitions(
      session: SparkSession,
      partitions: Seq[PartitionDirectory],
      baseUri: String,
      isMor: Boolean): Option[Array[Array[Byte]]] =
    planTasks(session, partitions, baseUri, isMor).map(_.map { slices =>
      val builder = OperatorOuterClass.HudiScan.newBuilder()
      for (slice <- slices) {
        val sliceBuilder = OperatorOuterClass.HudiFileSlice
          .newBuilder()
          .setBaseFilePath(slice.baseFilePath)
          .addAllLogFilePaths(slice.logFilePaths.asJava)
        if (slice.baseFileSize > 0) {
          sliceBuilder.setBaseFileSize(slice.baseFileSize)
        }
        slice.plainBaseFile.foreach(sliceBuilder.setBaseFileUri)
        builder.addFileSlices(sliceBuilder)
      }
      builder.build().toByteArray
    }.toArray)

  /**
   * A file slice as a task reads it: the base file and log file paths relative to the table base,
   * as the hudi-rs reader expects them, and the base file's size from the listing (0 when the
   * listing does not know it). `plainBaseFile` is set for a slice the JVM scan reads as a plain
   * Parquet file, which the file index lists without a file slice mapping because it has no log
   * files and no bootstrap base file; it holds the base file's URI as Comet's native Parquet scan
   * takes it.
   */
  private case class PlannedSlice(
      baseFilePath: String,
      logFilePaths: Seq[String],
      baseFileSize: Long,
      plainBaseFile: Option[String])

  /**
   * The file slices of each task, or None when any partition holds a shape the native reader
   * does not take: CDC file groups, a slice with no base file, a file outside the table base
   * path.
   *
   * Slices are packed into tasks the way Spark's file scan packs its files, by base file size up
   * to the same split size, so small files share a task. A base file larger than the split size
   * is one task here, where Spark's copy-on-write read would split it by byte range.
   */
  private def planTasks(
      session: SparkSession,
      partitions: Seq[PartitionDirectory],
      baseUri: String,
      isMor: Boolean): Option[Seq[Seq[PlannedSlice]]] = {
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
    val slicesByFile = new IdentityHashMap[PartitionedFile, PlannedSlice]()
    for (partition <- partitions) {
      for (file <- partition.files) {
        val filePath = file.getPath.toString
        // (base file path, log file paths, whether the JVM scan reads the slice as a file,
        // base file size)
        val slice: (String, Seq[String], Boolean, Long) = partition.values match {
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
                (base, logFiles.toSeq, false, math.max(fileSlice.getBaseFile.get().getFileSize, 0L))
              case None =>
                relativize(filePath) match {
                  case Some(p) => (p, Seq.empty, true, file.getLen)
                  case None => return None
                }
            }
          case _ =>
            relativize(filePath) match {
              case Some(p) => (p, Seq.empty, true, file.getLen)
              case None => return None
            }
        }
        val partitionedFile = partitionedFileUtils.createPartitionedFile(
          partition.values, new StoragePath(filePath), 0, file.getLen)
        files += partitionedFile
        val plainBaseFile =
          if (slice._3 && file.getLen > 0 && slice._1.endsWith(PARQUET_EXTENSION)) {
            Some(partitionedFile.filePath.toString)
          } else {
            None
          }
        slicesByFile.put(partitionedFile, PlannedSlice(slice._1, slice._2, slice._4, plainBaseFile))
      }
    }
    // Largest first, as Spark orders its splits before packing them.
    val sortedFiles = files.sortBy(_.length)(Ordering[Long].reverse).toSeq
    val maxSplitBytes = FilePartition.maxSplitBytes(session, partitions)
    Some(sparkAdapter.getFilePartitions(session, sortedFiles, maxSplitBytes)
      .map(_.files.map(slicesByFile.get).toSeq))
  }
}


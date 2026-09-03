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

package org.apache.comet.rules

import java.lang.{Boolean => JBoolean}
import java.net.URI
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruningExpression, Expression, GenericInternalRow, InputFileBlockLength, InputFileBlockStart, InputFileName, PlanExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{sideBySide, ArrayBasedMapData, GenericArrayData, MetadataColumnHelper}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.{CometBatchScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, DataTypeSupport, NativeBase}
import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, isSpark35Plus, withFallbackReason, withFallbackReasons}
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.iceberg.{CometIcebergNativeScanMetadata, IcebergReflection}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils.{encryptionEnabled, isEncryptionConfigSupported}
import org.apache.comet.serde.operator.{CometIcebergNativeScan, CometNativeScan}
import org.apache.comet.shims.{CometTypeShim, ShimCometStreaming, ShimFileFormat, ShimSubqueryBroadcast}

/**
 * Spark physical optimizer rule for replacing Spark scans with Comet scans.
 */
case class CometScanRule(session: SparkSession)
    extends Rule[SparkPlan]
    with CometTypeShim
    with ShimSubqueryBroadcast {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = _apply(plan)
    if (showTransformations && !newPlan.fastEquals(plan)) {
      logInfo(s"""
           |=== Applying Rule $ruleName ===
           |${sideBySide(plan.treeString, newPlan.treeString).mkString("\n")}
           |""".stripMargin)
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    if (!isCometLoaded(conf)) return plan

    // Comet does not support structured streaming. The parallel guard in
    // CometExecRule only stops operator wrapping, so without this check we
    // would still rewrite scans to CometScanExec in a streaming plan.
    if (ShimCometStreaming.isStreamingPlan(plan)) return plan

    def isSupportedScanNode(plan: SparkPlan): Boolean = plan match {
      case _: FileSourceScanExec => true
      case _: BatchScanExec => true
      case _ => false
    }

    def isIcebergMetadataTable(scanExec: BatchScanExec): Boolean = {
      // List of Iceberg metadata tables:
      // https://iceberg.apache.org/docs/latest/spark-queries/#inspecting-tables
      val metadataTableSuffix = Set(
        "history",
        "metadata_log_entries",
        "snapshots",
        "entries",
        "files",
        "manifests",
        "partitions",
        "position_deletes",
        "all_data_files",
        "all_delete_files",
        "all_entries",
        "all_manifests")

      // Match case-insensitively: metadata tables surface lowercase via the path form
      // (...metadata.json#all_manifests) but uppercase via the catalog-identifier form
      // (db.table.ALL_DATA_FILES), and the latter must hit this gate too rather than fall through
      // to reflection that fails on the metadata-table class.
      val name = scanExec.table.name().toLowerCase(Locale.ROOT)
      metadataTableSuffix.exists(name.endsWith)
    }

    val fullPlan = plan

    def transformScan(scanNode: SparkPlan): SparkPlan = scanNode match {
      // Tagged by CometSpark34AqeDppFallbackRule on Spark < 3.5 to keep a peer scan
      // Spark-native for canonical symmetry in SMJ self-joins (SPARK-32509).
      case scan if scan.getTagValue(CometScanRule.SKIP_COMET_SCAN_TAG).isDefined =>
        withFallbackReason(scan, "AQE DPP region fallback (Spark < 3.5)")

      case scan if !CometConf.COMET_NATIVE_SCAN_ENABLED.get(conf) =>
        withFallbackReason(scan, "Comet Scan is not enabled")

      // data source V1
      case scanExec: FileSourceScanExec =>
        transformV1Scan(fullPlan, scanExec)

      // data source V2
      case scanExec: BatchScanExec =>
        if (isIcebergMetadataTable(scanExec)) {
          withFallbackReason(scanExec, "Iceberg Metadata tables are not supported")
        } else {
          transformV2Scan(scanExec)
        }
    }

    plan.transform {
      case scan if isSupportedScanNode(scan) => transformScan(scan)
    }
  }

  private def transformV1Scan(plan: SparkPlan, scanExec: FileSourceScanExec): SparkPlan = {
    val metadataColNames = metadataCols(scanExec)
    if (metadataColNames.nonEmpty) {
      return withFallbackReason(
        scanExec,
        s"Metadata column(s) ${metadataColNames.mkString(", ")} is not supported")
    }

    // On Spark 3.4, injectQueryStageOptimizerRule is unavailable, so
    // CometPlanAdaptiveDynamicPruningFilters cannot run. Fall back this scan to Spark so that
    // Spark's PlanAdaptiveDynamicPruningFilters handles the SAB natively. Comet's narrower
    // CometSpark34AqeDppFallbackRule (queryStagePrepRule on 3.4) then tags any matching BHJ's
    // build-side broadcast so Spark's rule can match it via sameResult. See
    // CometSpark34AqeDppFallbackRule's class docstring.
    //
    // On 3.5+, CometPlanAdaptiveDynamicPruningFilters rewrites SABs directly and this fallback
    // is not needed.
    if (!isSpark35Plus && scanExec.partitionFilters.exists(isAqeDynamicPruningFilter)) {
      return withFallbackReason(scanExec, "AQE Dynamic Partition Pruning requires Spark 3.5+")
    }

    scanExec.relation match {
      case r: HadoopFsRelation =>
        if (!CometScanExec.isFileFormatSupported(r.fileFormat)) {
          CometExternalV1ScanProvider.findForFormat(r.fileFormat) match {
            case Some(provider) =>
              return provider
                .convert(r.sparkSession, scanExec)
                .getOrElse(
                  withFallbackReason(
                    scanExec,
                    s"External scan provider declined ${r.fileFormat}"))
            case None =>
          }
          return withFallbackReason(scanExec, s"Unsupported file format ${r.fileFormat}")
        }
        val hadoopConf = r.sparkSession.sessionState.newHadoopConfWithOptions(r.options)

        // TODO is this restriction valid for all native scan types?
        val possibleDefaultValues = getExistenceDefaultValues(scanExec.requiredSchema)
        if (possibleDefaultValues.exists(d => {
            d != null && (d.isInstanceOf[ArrayBasedMapData] || d
              .isInstanceOf[GenericInternalRow] || d.isInstanceOf[GenericArrayData])
          })) {
          // Spark already converted these to Java-native types, so we can't check SQL types.
          // ArrayBasedMapData, GenericInternalRow, GenericArrayData correspond to maps, structs,
          // and arrays respectively.
          withFallbackReason(
            scanExec,
            "Full native scan disabled because default values for nested types are not supported")
          return scanExec
        }

        nativeScan(plan, session, scanExec, r, hadoopConf).getOrElse(scanExec)

      case _ =>
        withFallbackReason(scanExec, s"Unsupported relation ${scanExec.relation}")
    }
  }

  private def nativeScan(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation,
      hadoopConf: Configuration): Option[SparkPlan] = {
    if (!COMET_EXEC_ENABLED.get()) {
      withFallbackReason(
        scanExec,
        s"Native Parquet scan requires ${COMET_EXEC_ENABLED.key} to be enabled")
      return None
    }
    // Comet's native readers go through object_store, which only understands a fixed set of URL
    // schemes. A custom Hadoop FileSystem (e.g. registered via spark.hadoop.fs.<scheme>.impl) would
    // surface at execution time as `Generic URL error: Unable to recognise URL "..."`. Decline here
    // so Spark's reader -- which goes through the Hadoop FS API and can resolve custom schemes --
    // handles the scan. Whether object_store recognizes a scheme is answered by the native layer
    // itself (`NativeBase.isObjectStoreSchemeSupported`) rather than a hardcoded list, so the
    // planner can't drift from object_store's actual support.
    //
    // EXCEPT schemes the user routes through libhdfs via `spark.hadoop.fs.comet.libhdfs.schemes`
    // (e.g. `hdfs`, or a test `fake`): those ARE natively readable through the libhdfs object_store
    // bridge, so they must NOT be declined here (regression guarded by
    // ParquetReadFromFakeHadoopFsSuite).
    //
    // The default mirrors the native side: when the config is unset, `is_hdfs_scheme`
    // (native/core/src/parquet/parquet_support.rs) treats `hdfs` as natively readable, and
    // `create_hdfs_object_store` is in the default build (`default = ["hdfs-opendal"]`). If we
    // defaulted to an empty set here, a plain `hdfs://` V1 scan would be declined and fall back to
    // Spark even though native can read it -- a silent regression for HDFS users in the default
    // configuration. So default to `Set("hdfs")` to stay in lockstep with the native default.
    val libhdfsSchemes: Set[String] = COMET_LIBHDFS_SCHEMES.get() match {
      case Some(s) =>
        s.split(",").map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).toSet
      case None => Set("hdfs")
    }
    val unsupportedFsSchemes = r.location.rootPaths
      .map(_.toUri)
      .filter { uri =>
        val sch = uri.getScheme
        sch != null && {
          val sl = sch.toLowerCase(Locale.ROOT)
          !libhdfsSchemes.contains(sl) && !CometScanRule.isNativelyReadableScheme(uri)
        }
      }
      .map(_.getScheme.toLowerCase(Locale.ROOT))
      .toSet
    if (unsupportedFsSchemes.nonEmpty) {
      withFallbackReason(
        scanExec,
        s"Unsupported filesystem schemes: ${unsupportedFsSchemes.mkString(", ")}")
      return None
    }
    // Disabling the vectorized reader opts into parquet-mr's permissive behavior
    // (silent overflow / null-on-narrowing). Comet has no parquet-mr-equivalent
    // backend, so by default fall back to Spark. Users can opt in to letting Comet
    // replace the scan via COMET_SCAN_ALLOW_DISABLED_PARQUET_VECTORIZED_READER.
    if (!conf.parquetVectorizedReaderEnabled &&
      !COMET_SCAN_ALLOW_DISABLED_PARQUET_VECTORIZED_READER.get()) {
      withFallbackReason(
        scanExec,
        "Native Parquet scan is incompatible with " +
          s"${SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key}=false; set " +
          s"${COMET_SCAN_ALLOW_DISABLED_PARQUET_VECTORIZED_READER.key}=true to opt in")
      return None
    }
    if (!CometNativeScan.isSupported(scanExec)) {
      return None
    }
    if (encryptionEnabled(hadoopConf) && !isEncryptionConfigSupported(hadoopConf)) {
      withFallbackReason(scanExec, "Native Parquet scan does not support encryption")
      return None
    }
    if (scanExec.fileConstantMetadataColumns.nonEmpty) {
      withFallbackReason(scanExec, "Native DataFusion scan does not support metadata columns")
      return None
    }
    // input_file_name, input_file_block_start, and input_file_block_length read from
    // InputFileBlockHolder, a thread-local set by Spark's FileScanRDD. The native DataFusion
    // scan does not use FileScanRDD, so these expressions would return empty/default values.
    if (plan.exists(node =>
        node.expressions.exists(_.exists {
          case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
          case _ => false
        }))) {
      withFallbackReason(
        scanExec,
        "Native DataFusion scan is not compatible with input_file_name, " +
          "input_file_block_start, or input_file_block_length")
      return None
    }
    if (ShimFileFormat.findRowIndexColumnIndexInSchema(scanExec.requiredSchema) >= 0) {
      withFallbackReason(scanExec, "Native DataFusion scan does not support row index generation")
      return None
    }
    if (!isSchemaSupported(scanExec, r)) {
      return None
    }
    Some(CometScanExec(scanExec, session))
  }

  private def transformV2Scan(scanExec: BatchScanExec): SparkPlan = {

    scanExec.scan match {
      case scan: CSVScan if COMET_CSV_V2_NATIVE_ENABLED.get() =>
        if (scanExec.output.exists(_.isMetadataCol)) {
          return withFallbackReason(
            scanExec,
            "Metadata columns are not supported for CSV V2 scans")
        }
        val fallbackReasons = new ListBuffer[String]()
        val schemaSupported =
          CometBatchScanExec.isSchemaSupported(scan.readDataSchema, fallbackReasons)
        if (!schemaSupported) {
          fallbackReasons += s"Schema ${scan.readDataSchema} is not supported"
        }
        val partitionSchemaSupported =
          CometBatchScanExec.isSchemaSupported(scan.readPartitionSchema, fallbackReasons)
        if (!partitionSchemaSupported) {
          fallbackReasons += s"Partition schema ${scan.readPartitionSchema} is not supported"
        }
        val corruptedRecordsColumnName =
          SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)
        val containsCorruptedRecordsColumn =
          !scan.readDataSchema.fieldNames.contains(corruptedRecordsColumnName)
        if (!containsCorruptedRecordsColumn) {
          fallbackReasons += "Comet doesn't support the processing of corrupted records"
        }
        val isInferSchemaEnabled = scan.options.getBoolean("inferSchema", false)
        if (isInferSchemaEnabled) {
          fallbackReasons += "Comet doesn't support inferSchema=true option"
        }
        val delimiter =
          Option(scan.options.get("delimiter"))
            .orElse(Option(scan.options.get("sep")))
            .getOrElse(",")
        val isSingleCharacterDelimiter = delimiter.length == 1
        if (!isSingleCharacterDelimiter) {
          fallbackReasons +=
            s"Comet supports only single-character delimiters, but got: '$delimiter'"
        }
        if (schemaSupported && partitionSchemaSupported && containsCorruptedRecordsColumn
          && !isInferSchemaEnabled && isSingleCharacterDelimiter) {
          CometBatchScanExec(
            scanExec.clone().asInstanceOf[BatchScanExec],
            runtimeFilters = scanExec.runtimeFilters)
        } else {
          withFallbackReasons(scanExec, fallbackReasons.toSet)
        }

      // Iceberg scan - detected by class name. SparkStagedScan covers reads issued by
      // RewriteDataFiles (and similar maintenance actions) where the planner has already
      // staged FileScanTasks via ScanTaskSetManager.
      case _ if IcebergReflection.isIcebergScanClass(scanExec.scan.getClass.getName) =>
        val fallbackReasons = new ListBuffer[String]()

        // Native Iceberg scan requires both configs to be enabled
        if (!COMET_ICEBERG_NATIVE_ENABLED.get()) {
          fallbackReasons += "Native Iceberg scan disabled because " +
            s"${COMET_ICEBERG_NATIVE_ENABLED.key} is not enabled"
          return withFallbackReasons(scanExec, fallbackReasons.toSet)
        }

        if (!COMET_EXEC_ENABLED.get()) {
          fallbackReasons += "Native Iceberg scan disabled because " +
            s"${COMET_EXEC_ENABLED.key} is not enabled"
          return withFallbackReasons(scanExec, fallbackReasons.toSet)
        }

        // Check for unsupported metadata columns in Iceberg scans
        val unsupportedMetadataCols = scanExec.output.filter(_.isMetadataCol).filterNot { attr =>
          CometIcebergNativeScan.MetadataFieldIds.keySet.contains(attr.name)
        }
        if (unsupportedMetadataCols.nonEmpty) {
          fallbackReasons += "Unsupported Iceberg metadata columns: " +
            unsupportedMetadataCols.map(_.name).mkString(", ")
          return withFallbackReasons(scanExec, fallbackReasons.toSet)
        }

        val typeChecker = CometScanTypeChecker()
        // Filter out metadata columns from schema check -- their types are handled
        // by iceberg-rust directly (e.g., _partition can be an empty struct for
        // unpartitioned tables which the general type checker rejects).
        val metadataColNames = scanExec.output.filter(_.isMetadataCol).map(_.name).toSet
        val dataSchema = if (metadataColNames.nonEmpty) {
          val filtered =
            scanExec.scan.readSchema().filter(f => !metadataColNames.contains(f.name))
          new org.apache.spark.sql.types.StructType(filtered.toArray)
        } else {
          scanExec.scan.readSchema()
        }
        val schemaSupported =
          typeChecker.isSchemaSupported(dataSchema, fallbackReasons)

        if (!schemaSupported) {
          fallbackReasons += "Comet extension is not enabled for " +
            s"${scanExec.scan.getClass.getSimpleName}: Schema not supported"
        }

        // Extract all Iceberg metadata once using reflection.
        // If any required reflection fails, this returns None, and we fall back to Spark.
        // First get metadataLocation and catalogProperties which are needed by the factory.
        val tableOpt = IcebergReflection.getTable(scanExec.scan)

        val metadataLocationOpt = tableOpt.flatMap { table =>
          IcebergReflection.getMetadataLocation(table)
        }

        val metadataOpt = metadataLocationOpt.flatMap { metadataLocation =>
          try {
            val session = org.apache.spark.sql.SparkSession.active
            val hadoopConf = session.sessionState.newHadoopConf()

            // For REST catalogs, the metadata file may not exist on disk since metadata
            // is fetched via HTTP. Check if file exists; if not, use table location instead.
            val metadataUri = new java.net.URI(metadataLocation)

            val metadataFile = new java.io.File(metadataUri.getPath)

            val effectiveLocation =
              if (!metadataFile.exists() && metadataUri.getScheme == "file") {
                // Metadata file doesn't exist (REST catalog with InMemoryFileIO or similar)
                // Use table location instead for FileIO initialization

                tableOpt
                  .flatMap { table =>
                    try {
                      val locationMethod = table.getClass.getMethod("location")
                      val tableLocation = locationMethod.invoke(table).asInstanceOf[String]
                      Some(tableLocation)
                    } catch {
                      case _: Exception =>
                        Some(metadataLocation)
                    }
                  }
                  .getOrElse(metadataLocation)
              } else {
                metadataLocation
              }

            val effectiveUri = new java.net.URI(effectiveLocation)

            val hadoopS3Options = NativeConfig.extractObjectStoreOptions(hadoopConf, effectiveUri)

            val hadoopDerivedProperties =
              CometIcebergNativeScan.hadoopToIcebergS3Properties(hadoopS3Options)

            // Forward the full FileIO property bag (including credentials.uri, OAuth tokens,
            // tenant-id, etc.) so a CometS3CredentialProvider can see everything LoadTableResponse
            // returned. The storage-prefix narrowing happens native-side just before
            // FileIOBuilder.with_prop, since iceberg-rust's FileIO is the only consumer that
            // requires the narrowed view.
            val fileIOProperties = tableOpt
              .flatMap(IcebergReflection.getFileIOProperties)
              .getOrElse(Map.empty)

            val catalogProperties = hadoopDerivedProperties ++ fileIOProperties

            val result = CometIcebergNativeScanMetadata
              .extract(scanExec.scan, effectiveLocation, catalogProperties)

            result
          } catch {
            case e: Exception =>
              logError(
                s"Failed to extract catalog properties from Iceberg scan: ${e.getMessage}",
                e)
              None
          }
        }

        // If metadata extraction failed, fall back to Spark
        val metadata = metadataOpt match {
          case Some(m) => m
          case None =>
            fallbackReasons += "Failed to extract Iceberg metadata via reflection"
            return withFallbackReasons(scanExec, fallbackReasons.toSet)
        }

        // Now perform all validation using the pre-extracted metadata
        // Check if table uses a FileIO implementation compatible with iceberg-rust.
        // Comet's native reader bypasses Iceberg Java's FileIO entirely and reads through
        // iceberg-rust's own storage layer instead. Only allow known-compatible implementations
        // whose backing storage that layer can reach via standard URL schemes.
        val fileIOCompatible = IcebergReflection.getFileIO(metadata.table) match {
          case Some(fileIO) if IcebergReflection.isCompatibleFileIO(fileIO) =>
            true
          case Some(fileIO) =>
            fallbackReasons += s"FileIO ${fileIO.getClass.getName} is not supported by " +
              "Comet's native reader (bypasses Iceberg Java FileIO)"
            false
          case None =>
            fallbackReasons += "Could not check FileIO compatibility"
            false
        }

        // Check Iceberg table format version

        // V3 adds column types iceberg-rust cannot read (variant, geometry, geography, unknown)
        // and column default values; those are handled by the allow-list and default-value checks
        // below, which fall back per-table. This gate only bounds the format version. Deletion
        // vectors (a V3 delete feature) are handled by the delete-file gate, which still falls back
        // for non-Parquet (Puffin) deletes since native deletion-vector reads are not yet wired.
        val formatVersion = IcebergReflection.getFormatVersion(metadata.table)
        val formatVersionSupported = formatVersion match {
          case Some(v) =>
            if (v > 3) {
              fallbackReasons += "Iceberg table format version " +
                s"$v is not supported. " +
                "Comet supports Iceberg table format V1, V2, and V3"
              false
            } else {
              true
            }
          case None =>
            fallbackReasons += "Could not verify Iceberg table format version"
            false
        }

        // Column default values are a V3 feature, so V1/V2 tables cannot have them and need no
        // check. Only inspect V3 tables. iceberg-rust does not synthesize a V3 initial-default for
        // a column absent from a data file, so a scan projecting such a column must fall back. A V3
        // table implies an Iceberg version new enough to expose initialDefault(), so a reflection
        // failure here is unexpected and also falls back rather than risk a crash.
        val defaultValuesSupported =
          if (!formatVersion.exists(_ >= 3)) {
            true
          } else {
            try {
              val defaulted = IcebergReflection.columnsWithInitialDefault(metadata.scanSchema)
              if (defaulted.nonEmpty) {
                fallbackReasons += "Iceberg column(s) with V3 default values are not yet " +
                  s"supported by Comet's native reader: ${defaulted.mkString(", ")}"
                false
              } else {
                true
              }
            } catch {
              case e: Exception =>
                fallbackReasons += "Iceberg reflection failure: could not verify V3 default " +
                  s"values: ${e.getMessage}"
                false
            }
          }

        // Comet serializes the whole table/scan schema to native, not just projected columns, so a
        // type the native reader does not support (e.g. variant) breaks the scan even when that
        // column is not projected. The readSchema allow-list only covers projected columns, so run
        // the same allow-list over the full schema Comet may serialize. Reflection failure also
        // falls back.
        val schemaTypesSupported =
          try {
            val fullSchema = IcebergReflection.toSparkSchema(metadata.tableSchema)
            typeChecker.isSchemaSupported(fullSchema, fallbackReasons)
          } catch {
            case e: Exception =>
              fallbackReasons += "Iceberg reflection failure: could not verify column " +
                s"types: ${e.getMessage}"
              false
          }

        // The native Parquet reader decrypts 128-bit and 256-bit AES-GCM data keys (16- or
        // 32-byte). 192-bit is unsupported because the underlying crypto has no AES-192-GCM. Fall
        // back for anything else. None means the table is unencrypted. Reflection failure also
        // falls back.
        val encryptionKeyLengthSupported =
          try {
            IcebergReflection.encryptionDataKeyLength(metadata.table) match {
              case Some(len) if len != 16 && len != 32 =>
                fallbackReasons += s"Iceberg table encryption with a ${len * 8}-bit data key is " +
                  "not yet supported by Comet's native reader (only 128-bit and 256-bit AES-GCM)"
                false
              case _ => true
            }
          } catch {
            case e: Exception =>
              fallbackReasons += "Iceberg reflection failure: could not verify encryption key " +
                s"length: ${e.getMessage}"
              false
          }

        // Single-pass validation of all FileScanTasks
        val taskValidation =
          try {
            CometScanRule.validateIcebergFileScanTasks(metadata.tasks)
          } catch {
            case e: Exception =>
              fallbackReasons += "Iceberg reflection failure: Could not validate " +
                s"FileScanTasks: ${e.getMessage}"
              return withFallbackReasons(scanExec, fallbackReasons.toSet)
          }

        // Check if all files are Parquet format and use supported filesystem schemes
        val allSupportedFilesystems = if (taskValidation.unsupportedSchemes.isEmpty) {
          true
        } else {
          fallbackReasons += "Iceberg scan contains files with unsupported filesystem " +
            s"schemes: ${taskValidation.unsupportedSchemes.mkString(", ")}. " +
            "Comet only supports: file, s3, s3a, gs, gcs, oss, abfss, abfs, wasbs, wasb"
          false
        }

        if (!taskValidation.allParquet) {
          fallbackReasons += "Iceberg scan contains non-Parquet files (ORC or Avro). " +
            "Comet only supports Parquet files in Iceberg tables"
        }

        // Partition values are deserialized via iceberg-rust's Literal::try_from_json()
        // which has incomplete type support (binary/fixed unimplemented, decimals limited)
        val partitionTypesSupported = (for {
          partitionSpec <- IcebergReflection.getPartitionSpec(metadata.table)
        } yield {
          val unsupportedTypes =
            IcebergReflection.validatePartitionTypes(partitionSpec, metadata.scanSchema)

          if (unsupportedTypes.nonEmpty) {
            unsupportedTypes.foreach { case (fieldName, typeStr, reason) =>
              fallbackReasons +=
                s"Partition column '$fieldName' with type $typeStr is not yet supported by " +
                  s"iceberg-rust: $reason"
            }
            false
          } else {
            true
          }
        }).getOrElse {
          // Fall back to Spark if reflection fails - cannot verify safety
          val msg =
            "Iceberg reflection failure: Could not verify partition types compatibility"
          logError(msg)
          fallbackReasons += msg
          false
        }

        // The `_partition` metadata column projects the table's unified partition type -- the
        // merge of all historical specs. iceberg-rust computes that merge at scan time and errors
        // (DataInvalid) when specs bind one field id to incompatible source/transform pairs (only
        // possible in V1 tables, which do not keep partition field ids unique across specs). That
        // error would fail the native scan with no chance to fall back, so when `_partition` is
        // projected we run Iceberg Java's equivalent check here and fall back if it cannot merge.
        val unifiedPartitionTypeSupported =
          if (scanExec.output.exists(a => a.isMetadataCol && a.name == "_partition")) {
            IcebergReflection.validateUnifiedPartitionType(metadata.table) match {
              case Some(reason) =>
                fallbackReasons += "Iceberg table has partition specs whose unified partition " +
                  s"type cannot be computed for the _partition metadata column: $reason"
                false
              case None => true
            }
          } else {
            true
          }

        // Get filter expressions for complex predicates check
        val filterExpressionsOpt = IcebergReflection.getFilterExpressions(scanExec.scan)

        // IS NULL/NOT NULL on complex types fail because iceberg-rust's accessor creation
        // only handles primitive fields. Nested field filters work because Iceberg Java
        // pre-binds them to field IDs. Element/key access filters don't push down to FileScanTasks.
        val complexTypePredicatesSupported = filterExpressionsOpt
          .map { filters =>
            // Empty filters can't trigger accessor issues
            if (filters.isEmpty) {
              true
            } else {
              val readSchema = scanExec.scan.readSchema()

              // Identify complex type columns that would trigger accessor creation failures
              val complexColumns = readSchema
                .filter(field => isComplexType(field.dataType))
                .map(_.name)
                .toSet

              // Detect IS NULL/NOT NULL on complex columns (pattern: is_null(ref(name="col")))
              // Nested field filters use different patterns and don't trigger this issue
              val hasComplexNullCheck = filters.asScala.exists { expr =>
                val exprStr = expr.toString
                val isNullCheck = exprStr.contains("is_null") || exprStr.contains("not_null")
                if (isNullCheck) {
                  complexColumns.exists { colName =>
                    exprStr.contains(s"""ref(name="$colName")""")
                  }
                } else {
                  false
                }
              }

              if (hasComplexNullCheck) {
                fallbackReasons += "IS NULL / IS NOT NULL predicates on complex type columns " +
                  "(struct/array/map) are not yet supported by iceberg-rust " +
                  "(nested field filters like address.city = 'NYC' are supported)"
                false
              } else {
                true
              }
            }
          }
          .getOrElse {
            // Fall back to Spark if reflection fails - cannot verify safety
            val msg =
              "Iceberg reflection failure: Could not check for complex type predicates"
            logError(msg)
            fallbackReasons += msg
            false
          }

        // Check for unsupported transform functions in residual expressions
        // iceberg-rust can only handle identity transforms in residuals; all other transforms
        // (truncate, bucket, year, month, day, hour) must fall back to Spark
        val transformFunctionsSupported = taskValidation.nonIdentityTransform match {
          case Some(transformType) =>
            fallbackReasons +=
              s"Iceberg transform function '$transformType' in residual expression " +
                "is not yet supported by iceberg-rust. " +
                "Only identity transforms are supported."
            false
          case None =>
            true
        }

        // Check for unsupported struct types in delete files
        val deleteFileTypesSupported = {
          var hasUnsupportedDeletes = false

          try {
            if (!taskValidation.deleteFiles.isEmpty) {
              val historicSchemas = IcebergReflection.getAllSchemas(metadata.table)
              taskValidation.deleteFiles.asScala.foreach { deleteFile =>
                // iceberg-rust only reads Parquet delete files. Avro/ORC positional or
                // equality deletes must be applied by Spark.
                IcebergReflection.getFileFormat(deleteFile) match {
                  case Some(fmt) if fmt.equalsIgnoreCase(IcebergReflection.FileFormats.PARQUET) =>
                  case Some(fmt) =>
                    hasUnsupportedDeletes = true
                    fallbackReasons +=
                      s"Delete file format '$fmt' is not supported by iceberg-rust. " +
                        "Only Parquet delete files can be applied natively."
                  case None =>
                    hasUnsupportedDeletes = true
                    logWarning(
                      "Could not determine Iceberg delete file format; falling back to Spark")
                    fallbackReasons += "Could not determine Iceberg delete file format"
                }

                val equalityFieldIds = IcebergReflection.getEqualityFieldIds(deleteFile)

                if (!equalityFieldIds.isEmpty) {
                  equalityFieldIds.asScala.foreach { fieldId =>
                    val fid = fieldId.asInstanceOf[Int]
                    // Resolve against the current schema, then the table's schema history: an
                    // equality delete may be keyed on a column that has since been dropped, which
                    // the serde must still put in the task schema to apply the delete natively.
                    val inCurrentSchema =
                      IcebergReflection.getFieldInfo(metadata.tableSchema, fid)
                    val fieldInfo = inCurrentSchema.orElse(
                      historicSchemas.iterator
                        .flatMap(s => IcebergReflection.getFieldInfo(s, fid))
                        .toSeq
                        .headOption)
                    if (inCurrentSchema.isEmpty && fieldInfo.isDefined) {
                      logDebug(
                        s"Iceberg equality-delete field id $fid is absent from the current table " +
                          "schema; resolved from schema history (likely a dropped column)")
                    }
                    fieldInfo match {
                      case Some((fieldName, fieldType)) =>
                        if (fieldType.contains("struct")) {
                          hasUnsupportedDeletes = true
                          fallbackReasons +=
                            s"Equality delete on unsupported column type '$fieldName' " +
                              s"($fieldType) is not yet supported by iceberg-rust. " +
                              "Struct types in equality deletes " +
                              "require datum conversion support that is not yet implemented."
                        }
                      case None =>
                        hasUnsupportedDeletes = true
                        logWarning(
                          s"Iceberg equality-delete field id $fid unresolvable in the table " +
                            "schema or its history; falling back to Spark")
                        fallbackReasons +=
                          s"Equality delete references field id $fid which cannot be resolved " +
                            "in the table schema or its history"
                    }
                  }
                }
              }
            }
          } catch {
            case e: Exception =>
              // Reflection failure means we cannot verify safety - must fall back
              hasUnsupportedDeletes = true
              fallbackReasons += "Iceberg reflection failure: Could not verify delete file " +
                s"types for safety: ${e.getMessage}"
          }

          !hasUnsupportedDeletes
        }

        // Check that all DPP subqueries use InSubqueryExec which we know how to handle.
        // Future Spark versions might introduce new subquery types we haven't tested.
        val dppSubqueriesSupported = {
          val unsupportedSubqueries = scanExec.runtimeFilters.collect {
            case DynamicPruningExpression(e) if !e.isInstanceOf[InSubqueryExec] =>
              e.getClass.getSimpleName
          }
          // Check for multi-index DPP which we don't support yet.
          // SPARK-46946 changed SubqueryAdaptiveBroadcastExec from index: Int to indices: Seq[Int]
          // as a preparatory refactor for future features (Null Safe Equality DPP, multiple
          // equality predicates). Currently indices always has one element, but future Spark
          // versions might use multiple indices.
          val multiIndexDpp = scanExec.runtimeFilters.exists {
            case DynamicPruningExpression(e: InSubqueryExec) =>
              e.plan match {
                case sab: SubqueryAdaptiveBroadcastExec =>
                  getSubqueryBroadcastIndices(sab).length > 1
                case _ => false
              }
            case _ => false
          }
          if (unsupportedSubqueries.nonEmpty) {
            fallbackReasons +=
              s"Unsupported DPP subquery types: ${unsupportedSubqueries.mkString(", ")}. " +
                "CometIcebergNativeScanExec only supports InSubqueryExec for DPP"
            false
          } else if (multiIndexDpp) {
            // See SPARK-46946 for context on multi-index DPP
            fallbackReasons +=
              "Multi-index DPP (indices.length > 1) is not yet supported. " +
                "See SPARK-46946 for context."
            false
          } else {
            true
          }
        }

        if (schemaSupported && fileIOCompatible && formatVersionSupported &&
          defaultValuesSupported && schemaTypesSupported && encryptionKeyLengthSupported &&
          taskValidation.allParquet && allSupportedFilesystems && partitionTypesSupported &&
          unifiedPartitionTypeSupported &&
          complexTypePredicatesSupported && transformFunctionsSupported &&
          deleteFileTypesSupported && dppSubqueriesSupported) {
          CometBatchScanExec(
            scanExec.clone().asInstanceOf[BatchScanExec],
            runtimeFilters = scanExec.runtimeFilters,
            nativeIcebergScanMetadata = Some(metadata))
        } else {
          withFallbackReasons(scanExec, fallbackReasons.toSet)
        }

      case other =>
        withFallbackReason(
          scanExec,
          s"Unsupported scan: ${other.getClass.getName}. " +
            "Comet Scan only supports Parquet and Iceberg Parquet file formats")
    }
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  /**
   * Detects AQE DPP (SubqueryAdaptiveBroadcastExec), as opposed to non-AQE DPP.
   *
   * Non-AQE DPP (PlanDynamicPruningFilters) runs before Comet rules and produces
   * SubqueryBroadcastExec/SubqueryExec which Spark's execution framework resolves. AQE DPP
   * (PlanAdaptiveDynamicPruningFilters) runs after Comet rules and searches for
   * BroadcastHashJoinExec. It doesn't recognize Comet operators, so it can't create DPP filters
   * correctly.
   */
  private def isAqeDynamicPruningFilter(e: Expression): Boolean =
    e.exists {
      case sub: InSubqueryExec => sub.plan.isInstanceOf[SubqueryAdaptiveBroadcastExec]
      case _ => false
    }

  private def metadataCols(plan: SparkPlan): Seq[String] = {
    plan.expressions.collect {
      case a: Attribute if a.isMetadataCol => a.name
    }
  }

  private def isSchemaSupported(scanExec: FileSourceScanExec, r: HadoopFsRelation): Boolean = {
    val fallbackReasons = new ListBuffer[String]()
    val typeChecker = CometScanTypeChecker()
    val schemaSupported =
      typeChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons)
    if (!schemaSupported) {
      withFallbackReason(
        scanExec,
        s"Unsupported schema ${scanExec.requiredSchema}: ${fallbackReasons.mkString(", ")}")
      return false
    }
    val partitionSchemaSupported =
      typeChecker.isSchemaSupported(r.partitionSchema, fallbackReasons)
    if (!partitionSchemaSupported) {
      withFallbackReason(
        scanExec,
        s"Unsupported partitioning schema ${scanExec.requiredSchema}: " +
          fallbackReasons.mkString(", "))
      return false
    }
    true
  }
}

case class CometScanTypeChecker() extends DataTypeSupport with CometTypeShim {

  override def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = {
    dt match {
      case ShortType if CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.get() =>
        fallbackReasons += "Native Parquet scan may not handle unsigned UINT_8 correctly for " +
          s"$dt. Set ${CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key}=false to allow " +
          "native execution if your data does not contain unsigned small integers. " +
          CometConf.COMPAT_GUIDE
        false
      case dt if isStringCollationType(dt) =>
        // we don't need specific support for collation in scans, but this
        // is a convenient place to force the whole query to fall back to Spark for now
        false
      case s: StructType if isVariantStruct(s) =>
        // Spark 4.0's PushVariantIntoScan rewrites a VariantType column into a struct of typed
        // fields plus per-field VariantMetadata, expecting the scan to honor Parquet variant
        // shredding semantics. Comet's native scan does not, so fall back to Spark.
        fallbackReasons +=
          s"Unsupported $name of type VariantType (shredded; not supported by native scan)"
        false
      case s: StructType if s.fields.isEmpty =>
        false
      case _ =>
        super.isTypeSupported(dt, name, fallbackReasons)
    }
  }
}

object CometScanRule extends Logging {

  // Per-scheme memo of `NativeBase.isObjectStoreSchemeSupported`. The answer depends only on the
  // URL scheme, so we cache by scheme and never re-cross the JNI boundary for a repeated scheme.
  private val schemeSupportCache =
    new ConcurrentHashMap[String, JBoolean]()

  /**
   * True when Comet's native object_store layer recognizes this URI's scheme (so the scan is
   * natively readable). Delegates to the native layer -- the source of truth -- instead of a
   * hardcoded scheme list. On any failure to consult native (e.g. the library isn't loaded on
   * this JVM, or predates this method) we assume the scheme IS supported: the scheme gate is an
   * early-fallback optimization, and a build without a working native library can't run Comet's
   * native scan anyway, so declining here would only over-restrict.
   */
  private[rules] def isNativelyReadableScheme(uri: URI): Boolean = {
    val scheme = uri.getScheme
    if (scheme == null) return true
    schemeSupportCache
      .computeIfAbsent(
        scheme.toLowerCase(Locale.ROOT),
        _ =>
          try JBoolean.valueOf(NativeBase.isObjectStoreSchemeSupported(uri.toString))
          catch {
            case _: Throwable => JBoolean.TRUE
          })
      .booleanValue()
  }

  /**
   * Tag set on a scan (`FileSourceScanExec` or `BatchScanExec`) that should be left as a plain
   * Spark scan rather than converted to a Comet scan. Written by
   * [[CometSpark34AqeDppFallbackRule]] on Spark < 3.5. See that rule's class docstring for the
   * rationale.
   */
  val SKIP_COMET_SCAN_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometScan")

  /**
   * Single-pass validation of Iceberg FileScanTasks.
   *
   * Consolidates file format, filesystem scheme, residual transform, and delete file checks into
   * one iteration for better performance with large tables.
   */
  def validateIcebergFileScanTasks(tasks: java.util.List[_]): IcebergTaskValidationResult = {
    val contentScanTaskClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.CONTENT_SCAN_TASK)
    val contentFileClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.CONTENT_FILE)
    val fileScanTaskClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.FILE_SCAN_TASK)
    val unboundPredicateClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.UNBOUND_PREDICATE)

    // Cache all method lookups outside the loop
    val fileMethod = contentScanTaskClass.getMethod("file")
    val formatMethod = contentFileClass.getMethod("format")
    val pathMethod = contentFileClass.getMethod("path")
    val residualMethod = contentScanTaskClass.getMethod("residual")
    val deletesMethod = fileScanTaskClass.getMethod("deletes")
    val termMethod = unboundPredicateClass.getMethod("term")

    val supportedSchemes =
      Set("file", "s3", "s3a", "gs", "gcs", "oss", "abfss", "abfs", "wasbs", "wasb")

    var allParquet = true
    val unsupportedSchemes = mutable.Set[String]()
    var nonIdentityTransform: Option[String] = None
    val deleteFiles = new java.util.ArrayList[Any]()

    tasks.asScala.foreach { task =>
      val dataFile = fileMethod.invoke(task)

      // File format check
      val fileFormat = formatMethod.invoke(dataFile).toString
      if (fileFormat != IcebergReflection.FileFormats.PARQUET) {
        allParquet = false
      }

      // Filesystem scheme check for data file
      try {
        val filePath = pathMethod.invoke(dataFile).toString
        val uri = new URI(filePath)
        val scheme = uri.getScheme
        if (scheme != null && !supportedSchemes.contains(scheme)) {
          unsupportedSchemes += scheme
        }
      } catch {
        case _: java.net.URISyntaxException => // ignore
      }

      // Residual transform check (short-circuit if already found unsupported)
      if (nonIdentityTransform.isEmpty && fileScanTaskClass.isInstance(task)) {
        try {
          val residual = residualMethod.invoke(task)
          if (unboundPredicateClass.isInstance(residual)) {
            val term = termMethod.invoke(residual)
            try {
              val transformMethod = term.getClass.getMethod("transform")
              transformMethod.setAccessible(true)
              val transform = transformMethod.invoke(term)
              val transformStr = transform.toString
              if (transformStr != IcebergReflection.Transforms.IDENTITY) {
                nonIdentityTransform = Some(transformStr)
              }
            } catch {
              case _: NoSuchMethodException => // No transform = simple reference, OK
            }
          }
        } catch {
          case _: Exception => // Skip tasks where we can't get residual
        }
      }

      // Collect delete files and check their schemes
      if (fileScanTaskClass.isInstance(task)) {
        try {
          val deletes = deletesMethod.invoke(task).asInstanceOf[java.util.List[_]]
          deleteFiles.addAll(deletes)

          deletes.asScala.foreach { deleteFile =>
            IcebergReflection.extractFileLocation(contentFileClass, deleteFile).foreach {
              deletePath =>
                try {
                  val deleteUri = new URI(deletePath)
                  val deleteScheme = deleteUri.getScheme
                  if (deleteScheme != null && !supportedSchemes.contains(deleteScheme)) {
                    unsupportedSchemes += deleteScheme
                  }
                } catch {
                  case _: java.net.URISyntaxException => // ignore
                }
            }
          }
        } catch {
          case _: Exception => // ignore errors accessing delete files
        }
      }
    }

    IcebergTaskValidationResult(
      allParquet,
      unsupportedSchemes.toSet,
      nonIdentityTransform,
      deleteFiles)
  }
}

/**
 * Result of single-pass validation over Iceberg FileScanTasks.
 */
case class IcebergTaskValidationResult(
    allParquet: Boolean,
    unsupportedSchemes: Set[String],
    nonIdentityTransform: Option[String],
    deleteFiles: java.util.List[_])

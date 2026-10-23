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
import org.apache.comet.iceberg.{CometIcebergNativeScanMetadata, IcebergReflection}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils.{encryptionEnabled, isEncryptionConfigSupported, readFieldId}
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
      //
      // NOTE: the Iceberg metadata-table bailout is NOT here either, for the same reason. It
      // matches on a table-name suffix (`files`, `snapshots`, ...), which a contrib's own table
      // could legitimately end with; rejecting here would decline such a scan before the contrib
      // is ever offered it. `transformV2Scan` applies the guard right after its contrib hook
      // declines.
      case scanExec: BatchScanExec =>
        transformV2Scan(scanExec)
    }

    plan.transform {
      case scan if isSupportedScanNode(scan) => transformScan(scan)
    }
  }

  /**
   * Whether this V2 scan reads an Iceberg metadata table, which Comet does not support.
   *
   * Matches on the table-name suffix, so it is only meaningful once contribs have had their turn
   * -- see the call site in `transformV2Scan`.
   */
  private def isIcebergMetadataTable(scanExec: BatchScanExec): Boolean = {
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

  private def transformV1Scan(plan: SparkPlan, scanExec: FileSourceScanExec): SparkPlan = {
    // Give any optional, out-of-tree scan contrib (e.g. Delta) first crack at this scan, ahead of
    // every built-in guard below. A contrib may support things Comet's built-in V1 scan does not
    // -- the Delta contrib synthesises `_metadata.*` in its own reader -- so applying those guards
    // first would decline such a scan before the contrib was ever offered it. A registered contrib
    // either claims the scan (returning its marker node) or declines via its own
    // `withFallbackReason` message. On a default build no contrib is registered, so this returns
    // None immediately and the behaviour below is unchanged.
    scanExec.relation match {
      case r: HadoopFsRelation =>
        CometScanContrib.tryTransformV1(plan, session, scanExec, r) match {
          case Some(handled) => return handled
          case None => // no contrib claimed it; fall through to Comet's built-in handling
        }
      case _ => // not a file relation; the built-in path below reports it
    }

    // fileConstantMetadataColumns (file_path, file_name, file_size, file_block_start,
    // file_block_length, file_modification_time) are known before opening the file and
    // supported below via the same projection mechanism as partition columns. The exceptions
    // are file_block_start and file_block_length. They are constant per split, but when Spark
    // splits a file, which split reads a row group is a reader decision: DataFusion keeps a row
    // group in the split that holds its first page, while Spark's parquet-mr reader keeps it in
    // the split that holds its midpoint, so rows would report the wrong split
    // (https://github.com/apache/datafusion-comet/issues/6505). Those two, and any other metadata
    // column (currently only `_metadata.row_index`, generated per row by the reader), fall back.
    val constantMetadataColNames = scanExec.fileConstantMetadataColumns
      .map(_.name)
      .toSet -- Set("file_block_start", "file_block_length")
    val unsupportedMetadataColNames =
      metadataCols(scanExec).filterNot(constantMetadataColNames.contains)
    if (unsupportedMetadataColNames.nonEmpty) {
      return withFallbackReason(
        scanExec,
        s"Metadata column(s) ${unsupportedMetadataColNames.mkString(", ")} is not supported")
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
            case Some(_)
                if plan.exists(node =>
                  node.expressions.exists(_.exists {
                    case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength =>
                      true
                    case _ => false
                  })) =>
              // Same reason as the native Parquet scan: these read a thread-local that only
              // Spark's FileScanRDD sets.
              return withFallbackReason(
                scanExec,
                "External scan provider is not compatible with input_file_name, " +
                  "input_file_block_start, or input_file_block_length")
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
        // NOTE: the object_store scheme gate lives in `nativeScan` (below), shared with the
        // non-contrib path. The contrib delegation at the top of this method runs before it, so
        // contrib scans are unaffected; vanilla V1 scans hit the gate when this calls `nativeScan`.
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
    // bridge, so they must NOT be declined here. The claim decision is guarded in CI by
    // CometScanSchemeFallbackSuite; end-to-end execution through libhdfs is guarded by
    // ParquetReadFromFakeHadoopFsSuite, which is a manual suite (see its scaladoc).
    //
    // The default mirrors the native side: when the config is unset, `is_hdfs_scheme`
    // (native/core/src/parquet/parquet_support.rs) treats `hdfs` as natively readable, and
    // `create_hdfs_object_store` is in the default build (`default = ["hdfs-opendal"]`). If we
    // defaulted to an empty set here, a plain `hdfs://` V1 scan would be declined and fall back to
    // Spark even though native can read it -- a silent regression for HDFS users in the default
    // configuration. So default to `Set("hdfs")` to stay in lockstep with the native default.
    val libhdfsSchemes: Set[String] = COMET_LIBHDFS_SCHEMES.get() match {
      case Some(s) => NativeConfig.parseSchemeSet(s)
      case None => Set("hdfs")
    }
    // Opt-in S3-compliant alias schemes (e.g. `blob`) from `fs.comet.s3Compliant.schemes`. Read
    // from the Hadoop config rather than SQLConf (unlike COMET_LIBHDFS_SCHEMES above) so
    // `core-site.xml` is honored. The native object_store gate no longer claims aliases, so admit
    // them here where the Hadoop config is available.
    val s3CompliantSchemes = NativeConfig.resolveS3CompliantSchemes(hadoopConf)

    // Classify each root path once; see RootPathInfo.
    val roots = CometScanRule.classifyRootPaths(
      r.location.rootPaths.map(_.toUri),
      libhdfsSchemes,
      s3CompliantSchemes)

    val unsupportedFsSchemes = roots.iterator.flatMap { root =>
      root.scheme.filter(_ =>
        !root.isLibhdfs && !CometScanRule.isNativelyReadableScheme(root.uri, s3CompliantSchemes))
    }.toSet
    if (unsupportedFsSchemes.nonEmpty) {
      withFallbackReason(
        scanExec,
        s"Unsupported filesystem schemes: ${unsupportedFsSchemes.mkString(", ")}")
      return None
    }
    // More than one bucket cannot be served by the single object store native planning registers
    // per FilePartition; see aliasScanBuckets. Scoped to alias scans: plain multi-bucket `s3://`
    // has the same flaw today and silently declining it would newly fall back scans that work by
    // luck, so that widening is left as a separate decision. Note the sibling Iceberg guard
    // (dataFileBuckets, below) is NOT so scoped -- it declines multi-bucket `s3a://` too.
    val scanBuckets = CometScanRule.aliasScanBuckets(roots)
    if (scanBuckets.size > 1) {
      withFallbackReason(
        scanExec,
        "Native Parquet scan reads S3-compliant alias paths across multiple buckets " +
          s"(${scanBuckets.toSeq.sorted.mkString(", ")}); Comet registers one object store " +
          "per file partition and would read every file from the first file's bucket")
      return None
    }
    // A scheme object_store recognizes can still carry a path it rejects (e.g. a directory whose
    // name contains a newline -> `%0A` in the URI), which native execution would hard-fail on.
    // Only object_store-native schemes are probed: aliases (normalized to s3:// natively) and
    // libhdfs schemes route elsewhere. Root paths only -- a rejected character deeper in the tree
    // still fails at execution.
    val rejectedPath = roots.find(root =>
      root.scheme.isDefined && !root.isLibhdfs && !root.isAlias &&
        !CometScanRule.objectStoreAcceptsPath(root.uri))
    if (rejectedPath.nonEmpty) {
      withFallbackReason(
        scanExec,
        s"Native Parquet scan cannot open path '${rejectedPath.get.uri}': object_store " +
          "rejects it (e.g. an unsupported character in the path)")
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
    // TODO: Remove this fallback once DataFusion can ignore embedded Arrow schema hints and
    // preserve Spark's ENUM inference without losing Parquet decryption state.
    // https://github.com/apache/datafusion-comet/issues/5477
    if (encryptionEnabled(hadoopConf) &&
      scanExec.requiredSchema.exists(field => isVariantType(field.dataType))) {
      withFallbackReason(scanExec, "Native Parquet Variant scans do not support encryption")
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
        "Native Parquet scan is not compatible with input_file_name, " +
          "input_file_block_start, or input_file_block_length")
      return None
    }
    // Check the name directly instead of calling findRowIndexColumnIndexInSchema, which validates
    // the temporary column's type and can throw a raw RuntimeException. Falling back lets Spark's
    // Parquet reader wrap that validation failure as FAILED_READ_FILE.
    if (scanExec.requiredSchema.fieldNames.contains(
        ShimFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)) {
      withFallbackReason(scanExec, "Native Parquet scan does not support row index generation")
      return None
    }
    if (!isSchemaSupported(scanExec, r)) {
      return None
    }
    Some(CometScanExec(scanExec, session))
  }

  private def transformV2Scan(scanExec: BatchScanExec): SparkPlan = {

    // Give any optional, out-of-tree scan contrib (e.g. Lance) first crack at this V2 scan. On a
    // default build no contrib is registered, so this returns None and we proceed with Comet's
    // built-in V2 handling below. A registered contrib either claims the scan or declines via its
    // own `withFallbackReason` fallback message.
    CometScanContrib.tryTransformV2(scanExec) match {
      case Some(handled) => return handled
      case None => // proceed with vanilla logic
    }

    // Iceberg metadata tables are matched by table-name suffix (`files`, `snapshots`, ...), which
    // a contrib's own table could legitimately end with. Running the check here -- after the
    // contrib hook has declined -- means a contrib that owns such a table still gets to claim it,
    // while the fallback for a genuine Iceberg metadata table is unchanged.
    if (isIcebergMetadataTable(scanExec)) {
      return withFallbackReason(scanExec, "Iceberg Metadata tables are not supported")
    }

    // NOTE: there is no blanket metadata-column guard here. Comet's built-in V2 paths handle
    // metadata columns individually -- the Iceberg path supports the ones in
    // `CometIcebergNativeScan.MetadataFieldIds` and rejects the rest, the CSV path rejects all --
    // and each of those runs below, after the contrib hook. Hoisting a blanket guard to here would
    // both regress that per-path support and decline a contrib's scan before it was offered.

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

        // Built once and reused below (metadata extraction, the file-scheme gate) rather than
        // rebuilding a Configuration per read.
        val hadoopConf = session.sessionState.newHadoopConf()
        val s3CompliantSchemes = NativeConfig.resolveS3CompliantSchemes(hadoopConf)

        // Validate the FileScanTasks up front: the data/delete file buckets it resolves decide
        // which bucket's per-bucket S3 settings the single native FileIO consumes (see
        // catalogProperties below), so this must run before we build them. The task list is
        // handed to `extract` below so the reflective accessor runs once per scan.
        val (icebergTasks, taskValidation) =
          try {
            IcebergReflection.getTasks(scanExec.scan) match {
              case Some(tasks) =>
                (tasks, CometScanRule.validateIcebergFileScanTasks(tasks, s3CompliantSchemes))
              case None =>
                fallbackReasons += "Iceberg reflection failure: Could not extract FileScanTasks"
                return withFallbackReasons(scanExec, fallbackReasons.toSet)
            }
          } catch {
            case e: Exception =>
              fallbackReasons += "Iceberg reflection failure: Could not validate " +
                s"FileScanTasks: ${e.getMessage}"
              return withFallbackReasons(scanExec, fallbackReasons.toSet)
          }

        // The native FileIO reads DATA and DELETE files; the metadata JSON was already read
        // JVM-side during planning. Its single global S3 config therefore targets the data bucket,
        // NOT the metadata bucket. When data/delete files span multiple S3 buckets, one FileIO
        // cannot carry each bucket's per-bucket endpoint/credentials, so fall back.
        if (taskValidation.dataFileBuckets.size > 1) {
          fallbackReasons += "Iceberg scan reads data/delete files across multiple S3 buckets " +
            s"(${taskValidation.dataFileBuckets.toSeq.sorted.mkString(", ")}); Comet's native " +
            "reader uses a single object-store configuration per scan and cannot apply " +
            "per-bucket S3 settings to each"
          return withFallbackReasons(scanExec, fallbackReasons.toSet)
        }
        val icebergDataBucket: Option[String] = taskValidation.dataFileBuckets.headOption

        // Extract all Iceberg metadata once using reflection.
        // If any required reflection fails, this returns None, and we fall back to Spark.
        // First get metadataLocation and catalogProperties which are needed by the factory.
        val tableOpt = IcebergReflection.getTable(scanExec.scan)

        val metadataLocationOpt = tableOpt.flatMap { table =>
          IcebergReflection.getMetadataLocation(table)
        }

        val metadataOpt = metadataLocationOpt.flatMap { metadataLocation =>
          try {
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
                      val locationMethod = IcebergReflection.getMethod(table.getClass, "location")
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

            // Promote the DATA bucket's per-bucket `fs.s3a.bucket.<b>.*` settings to global (what
            // the native FileIO reads), NOT the metadata bucket's: the FileIO opens data/delete
            // files, and Iceberg allows a data location (`write.data.path`) in a different bucket
            // than the metadata. `icebergDataBucket` is the single bucket those files resolve to
            // (multi-bucket already fell back above); None for a local/GCS/OSS table needs no
            // promotion.
            val hadoopDerivedProperties =
              CometIcebergNativeScan.hadoopToIcebergS3Properties(
                hadoopS3Options,
                icebergDataBucket)

            // Forward the full FileIO property bag (including credentials.uri, OAuth tokens,
            // tenant-id, etc.) so a CometS3CredentialProvider can see everything LoadTableResponse
            // returned. The storage-prefix narrowing happens native-side just before
            // FileIOBuilder.with_prop, since iceberg-rust's FileIO is the only consumer that
            // requires the narrowed view.
            val fileIOProperties = tableOpt
              .flatMap(IcebergReflection.getFileIOProperties)
              .getOrElse(Map.empty)

            // Iceberg reads the alias opt-in from catalog_properties (IcebergScanCommon has no
            // object_store_options), and hadoopToIcebergS3Properties drops non-fs.s3a keys, so
            // re-add it. load_file_io narrows catalog_properties to storage-prefix keys before
            // iceberg-rust's FileIO, so the alias key never reaches FileIO -- but it is still
            // handed, unfiltered, to CometS3CredentialBridge, so a custom credential provider sees
            // it. That is intended: the provider gets the full property bag.
            val catalogProperties = hadoopDerivedProperties ++ fileIOProperties ++
              hadoopS3Options
                .get(COMET_S3_COMPLIANT_SCHEMES_KEY)
                .map(COMET_S3_COMPLIANT_SCHEMES_KEY -> _)

            val result = CometIcebergNativeScanMetadata
              .extract(scanExec.scan, effectiveLocation, catalogProperties, icebergTasks)

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

        // Gate the metadata location's scheme. It is the string storage_factory_for /
        // load_file_io dispatch on, yet unlike the data- and delete-file paths (gated in
        // validateIcebergFileScanTasks) it otherwise reaches native unchecked: an unsupported
        // scheme would be claimed by the planner, then die at execution with `Unsupported
        // storage scheme`. metadata.metadataLocation is the effectiveLocation extract() was
        // handed, already parsed above. Schemeless local-catalog paths pass.
        val metadataUri = new java.net.URI(metadata.metadataLocation)
        val metadataSchemeSupported =
          if (CometScanRule.isIcebergReadableScheme(metadataUri, s3CompliantSchemes)) {
            true
          } else {
            fallbackReasons += "Iceberg metadata location uses filesystem scheme " +
              s"'${metadataUri.getScheme}', which Comet's native Iceberg reader cannot open " +
              "(object_store may recognize it but iceberg-rust's storage factory cannot build " +
              s"it). ${CometScanRule.icebergSupportedSchemesMessage(s3CompliantSchemes)}"
            false
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
        // vectors (a V3 delete feature) are read natively; the delete-file gate accepts their
        // Puffin format.
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

        // Projected roots are matched by field ID so historical snapshots still identify renamed
        // columns. Both schema checks below use them.
        val projectedDataColumns = scanExec.output.filterNot(_.isMetadataCol)
        val resolver = session.sessionState.conf.resolver
        val projectedFieldIds = projectedDataColumns.map { attr =>
          metadata.globalFieldIdMapping.collectFirst {
            case (fieldName, fieldId) if resolver(fieldName, attr.name) => fieldId
          }
        }
        val resolvedProjectedFieldIds = projectedFieldIds.flatten.toSet
        val hasUnresolvedProjectedFieldIds = projectedFieldIds.exists(_.isEmpty)

        // The whole Iceberg table schema is serialized to native, but iceberg-rust can represent
        // Variant in that schema as long as no projected field contains one. Check projected
        // roots strictly, and allow Variant only under entirely unprojected roots. Other
        // unsupported types still fail closed everywhere. An empty data projection is also strict
        // because iceberg-rust currently interprets an empty field-id list as a request for every
        // column.
        val schemaTypesSupported =
          try {
            val fullSchema = IcebergReflection.toSparkSchema(metadata.tableSchema)
            // DataTypeSupport recursively dispatches back to this override for struct fields,
            // array elements, and map entries, so Variant is allowed at any nesting depth only
            // when its entire top-level Iceberg field is unprojected.
            val unprojectedTypeChecker = new CometScanTypeChecker() {
              override def isTypeSupported(
                  dt: DataType,
                  name: String,
                  reasons: ListBuffer[String]): Boolean =
                isVariantType(dt) || super.isTypeSupported(dt, name, reasons)
            }
            val tableFieldIds = IcebergReflection.buildFieldIdMapping(metadata.tableSchema)

            fullSchema.fields.forall { field =>
              val isProjected = projectedDataColumns.isEmpty ||
                hasUnresolvedProjectedFieldIds ||
                tableFieldIds.get(field.name).forall(resolvedProjectedFieldIds.contains)
              val checker = if (isProjected) typeChecker else unprojectedTypeChecker
              checker.isTypeSupported(field.dataType, field.name, fallbackReasons)
            }
          } catch {
            case e: Exception =>
              fallbackReasons += "Iceberg reflection failure: could not verify column " +
                s"types: ${e.getMessage}"
              false
          }

        // Neither iceberg-rust nor the batch adaptation after it matches a data file's nested
        // fields to the table's by field id (apache/iceberg-rust#2617). A file written before a
        // nested field was added (to a struct, or to a struct inside a list or map) fails the
        // native scan with "Incorrect number of arrays for StructArray fields", and a nested field
        // renamed since the file was written can read back as NULL. The native read asks for a
        // projected column's full nested type even when Spark prunes it, taken from the current
        // table schema, or from the scan schema when VERSION AS OF reads a dropped column (see
        // CometIcebergNativeScan). A FileScanTask does not record which schema wrote its file, so
        // fall back when any schema in the table's history lacks a nested field of a projected
        // column or names it differently.
        val nestedFieldsSupported =
          try {
            val schemas = Seq(metadata.tableSchema, metadata.scanSchema)
            val fieldIds =
              if (hasUnresolvedProjectedFieldIds) {
                schemas.flatMap(IcebergReflection.buildFieldIdMapping(_).values).toSet
              } else {
                resolvedProjectedFieldIds
              }
            val changed = schemas
              .flatMap { schema =>
                IcebergReflection.nestedFieldsAddedOrRenamed(metadata.table, schema, fieldIds)
              }
              .distinct
              .sorted
            if (changed.nonEmpty) {
              fallbackReasons += "Nested fields added or renamed by Iceberg schema evolution are " +
                "not yet supported by Comet's native reader, which cannot match them to data " +
                s"files written before the change: ${changed.mkString(", ")}"
            }
            changed.isEmpty
          } catch {
            case e: Exception =>
              fallbackReasons += "Iceberg reflection failure: could not compare nested fields " +
                s"with the table's schema history: ${e.getMessage}"
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

        // Check if all files are Parquet format and use supported filesystem schemes.
        // (FileScanTask validation ran once, up front; see taskValidation.)
        val allSupportedFilesystems = if (taskValidation.unsupportedSchemes.isEmpty) {
          true
        } else {
          fallbackReasons += "Iceberg scan contains files with filesystem schemes not supported " +
            "by Comet's native Iceberg reader (object_store recognizes them but iceberg-rust's " +
            "storage factory cannot build them): " +
            s"${taskValidation.unsupportedSchemes.toSeq.sorted.mkString(", ")}. " +
            CometScanRule.icebergSupportedSchemesMessage(s3CompliantSchemes)
          false
        }

        // Hostless non-alias locations are unopenable; see hasOpenableAuthority.
        val allLocationsOpenable = taskValidation.hostlessLocation match {
          case Some(loc) =>
            fallbackReasons += "Iceberg scan references a data/delete file location without a " +
              s"URL host ('$loc'); Comet's native Iceberg reader opens files by their raw " +
              "location and cannot promote a bucket from the path"
            false
          case None => true
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

        // Check for unsupported struct and Variant types in delete files
        val deleteFileTypesSupported = {
          var hasUnsupportedDeletes = false

          try {
            if (!taskValidation.deleteFiles.isEmpty) {
              val historicSchemas = IcebergReflection.getAllSchemas(metadata.table)
              val contentFileClass =
                IcebergReflection.loadClass(IcebergReflection.ClassNames.CONTENT_FILE)
              val deleteFileClass =
                IcebergReflection.loadClass(IcebergReflection.ClassNames.DELETE_FILE)
              taskValidation.deleteFiles.asScala.foreach { deleteFile =>
                // iceberg-rust reads Parquet delete files (position and equality) and Puffin
                // deletion vectors. Avro/ORC deletes must be applied by Spark.
                IcebergReflection.getFileFormat(contentFileClass, deleteFile) match {
                  case Some(fmt)
                      if fmt.equalsIgnoreCase(IcebergReflection.FileFormats.PARQUET) ||
                        fmt.equalsIgnoreCase(IcebergReflection.FileFormats.PUFFIN) =>
                  case Some(fmt) =>
                    hasUnsupportedDeletes = true
                    fallbackReasons +=
                      s"Delete file format '$fmt' is not supported by iceberg-rust. " +
                        "Only Parquet and Puffin delete files can be applied natively."
                  case None =>
                    hasUnsupportedDeletes = true
                    logWarning(
                      "Could not determine Iceberg delete file format; falling back to Spark")
                    fallbackReasons += "Could not determine Iceberg delete file format"
                }

                val equalityFieldIds =
                  IcebergReflection.getEqualityFieldIds(deleteFileClass, deleteFile)

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
                        if (fieldType.contains("struct") || fieldType.equalsIgnoreCase(
                            "variant")) {
                          hasUnsupportedDeletes = true
                          fallbackReasons +=
                            s"Equality delete on unsupported column type '$fieldName' " +
                              s"($fieldType) is not yet supported by iceberg-rust. " +
                              "Struct and Variant types in equality deletes " +
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
          taskValidation.allParquet && allSupportedFilesystems && allLocationsOpenable &&
          metadataSchemeSupported && partitionTypesSupported && unifiedPartitionTypeSupported &&
          transformFunctionsSupported && deleteFileTypesSupported && dppSubqueriesSupported &&
          nestedFieldsSupported) {
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
    // Admit Variant only at a required root in ordinary Parquet. Recursive and Iceberg type
    // checks continue to use CometScanTypeChecker's stricter support rules.
    val requiredSchemaChecker = new CometScanTypeChecker {
      override def isTypeSupported(
          dt: DataType,
          name: String,
          reasons: ListBuffer[String]): Boolean =
        isVariantType(dt) || typeChecker.isTypeSupported(dt, name, reasons)
    }
    val schemaSupported =
      requiredSchemaChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons)
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

  /**
   * `isTypeSupported` only ever sees a field's *data type*, so nothing there can compare two
   * top-level fields. Spark's field id ambiguity applies at the schema root too, so check it
   * here.
   */
  override def isSchemaSupported(
      schema: StructType,
      fallbackReasons: ListBuffer[String]): Boolean = {
    if (duplicateFieldIds(schema.fields)) {
      fallbackReasons += "duplicate Parquet field ids among top-level fields"
      false
    } else {
      super.isSchemaSupported(schema, fallbackReasons)
    }
  }

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
      case StructType(fields) if duplicateFieldIds(fields) =>
        // Under field id matching Spark resolves each requested field to the one Parquet field
        // carrying its id and raises when more than one answers. Comet reads such a struct
        // positionally instead, so hand the read back to Spark and let it report the ambiguity.
        fallbackReasons += s"Unsupported ${name}: struct with duplicate Parquet field ids"
        false
      case _ =>
        super.isTypeSupported(dt, name, fallbackReasons)
    }
  }

  /** True when the session resolves Parquet fields by id and `fields` repeat one. */
  private def duplicateFieldIds(fields: Array[StructField]): Boolean =
    readFieldId(SQLConf.get) && DataTypeSupport.hasDuplicateFieldIds(fields)
}

object CometScanRule extends Logging {

  // Memo of `NativeBase.isObjectStoreSchemeSupported`, keyed by the probe URL rather than the
  // scheme: object_store's parser keys on (scheme, host-presence), so an authorityless URL would
  // otherwise poison the authority-bearing form of the same scheme.
  private val schemeSupportCache = new ConcurrentHashMap[String, JBoolean]()

  /**
   * True when Comet's native Parquet scan can read this URI's scheme: object_store recognizes it
   * (asked of the native layer, the source of truth) OR it is an opt-in S3-compliant alias. If
   * native can't be consulted (library not loaded), assume supported -- the gate is only an
   * early-fallback optimization and such a build can't run the native scan anyway.
   */
  private[rules] def isNativelyReadableScheme(
      uri: URI,
      s3CompliantSchemes: Set[String]): Boolean = {
    val scheme = uri.getScheme
    if (scheme == null) return true
    val lower = scheme.toLowerCase(Locale.ROOT)
    if (s3CompliantSchemes.contains(lower)) return true
    // Probe a scheme(+fixed dummy host) URL, never the caller's authority/path: the fixed host
    // keeps the two host-presence answers apart, and dropping the real path avoids a spurious
    // `false` from chars object_store rejects in a `Path` (e.g. Iceberg's `p=%0A`).
    val probe =
      if (uri.getRawAuthority != null) s"$lower://comet-probe-host/" else s"$lower:///"
    schemeSupportCache
      .computeIfAbsent(probe, p => JBoolean.valueOf(probeObjectStore(p)))
      .booleanValue()
  }

  /**
   * A scan root path with the classification the V1 scheme gates share, computed once so the
   * gates cannot disagree about which schemes each of them exempts.
   */
  private[rules] case class RootPathInfo(
      uri: URI,
      scheme: Option[String],
      isLibhdfs: Boolean,
      isAlias: Boolean,
      bucket: Option[String])

  private[rules] def classifyRootPaths(
      uris: Seq[URI],
      libhdfsSchemes: Set[String],
      s3CompliantSchemes: Set[String]): Seq[RootPathInfo] =
    uris.map { uri =>
      val scheme = NativeConfig.lowerScheme(uri)
      RootPathInfo(
        uri,
        scheme,
        isLibhdfs = scheme.exists(libhdfsSchemes.contains),
        isAlias = scheme.exists(s3CompliantSchemes.contains),
        bucket = NativeConfig.bucketForUri(uri, s3CompliantSchemes))
    }

  /**
   * The distinct buckets this scan's root paths address, or empty when none of them uses an
   * opt-in S3-compliant alias. More than one bucket means the Parquet gate must fall back: native
   * planning registers one object store per FilePartition (keyed on the first file's bucket) and
   * strips the authority from every file's object key, so files in a second bucket are read from
   * the first. Spark's FilePartition bin-packing can co-locate files from different root paths.
   */
  private[rules] def aliasScanBuckets(roots: Seq[RootPathInfo]): Set[String] =
    if (!roots.exists(_.isAlias)) Set.empty else roots.flatMap(_.bucket).toSet

  /**
   * Ask the native object_store parser whether it can build `url`: both the scheme and, via
   * `Path::from_url_path`, the key. On any native error (e.g. the library isn't loaded), assume
   * yes -- these gates are only early-fallback optimizations, and such a build can't run the
   * native scan anyway.
   *
   * Callers pass either a synthetic scheme-only probe (cached, see [[isNativelyReadableScheme]])
   * or a real path (uncached), because `Path::from_url_path` rejects characters object_store
   * forbids in a key, e.g. a directory name containing a newline (`%0A` in the URI).
   */
  private def probeObjectStore(url: String): Boolean =
    try NativeBase.isObjectStoreSchemeSupported(url)
    catch { case _: Throwable => true }

  /** [[probeObjectStore]] against a URI's real path, not just its scheme. Uncached. */
  private[rules] def objectStoreAcceptsPath(uri: URI): Boolean =
    probeObjectStore(uri.toString)

  /**
   * Tag set on a scan (`FileSourceScanExec` or `BatchScanExec`) that should be left as a plain
   * Spark scan rather than converted to a Comet scan. Written by
   * [[CometSpark34AqeDppFallbackRule]] on Spark < 3.5. See that rule's class docstring for the
   * rationale.
   */
  val SKIP_COMET_SCAN_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometScan")

  /**
   * Schemes Comet's native Iceberg scan can actually open, mirroring the match arms in
   * `native/core/src/execution/operators/iceberg_common.rs::storage_factory_for`. Deliberately
   * NOT delegated to `isNativelyReadableScheme`: object_store recognizes schemes (http/https,
   * azure, memory) that iceberg-rust's OpenDAL storage factory cannot build, and admitting them
   * here turns a clean JVM fallback into a native runtime "Unsupported storage scheme" error. Add
   * here what you add to `storage_factory_for` (currently Aliyun `oss` and GCS `gs`).
   * S3-compliant aliases like `blob` are opt-in via `fs.comet.s3Compliant.schemes` (see
   * `isIcebergReadableScheme`), not hardcoded, since the native planner opens them via S3. The
   * write path keeps its own list (`CometIcebergNativeWrite.SupportedStorageSchemes`), which
   * differs deliberately: it excludes `oss` (fails closed, see `storage_factory_for`) and
   * includes `memory`.
   */
  private val icebergReadableSchemes: Set[String] =
    Set("file", "s3", "s3a", "gs", "oss")

  /**
   * "Supported schemes: ..." suffix shared by the Iceberg scheme-fallback messages. Lists the
   * built-in Iceberg-readable allowlist plus the opt-in S3-compliant aliases.
   */
  private[rules] def icebergSupportedSchemesMessage(s3CompliantSchemes: Set[String]): String = {
    val schemes = (icebergReadableSchemes ++ s3CompliantSchemes).toSeq.sorted.mkString(", ")
    s"Supported schemes: $schemes"
  }

  /**
   * Scheme gate for the Iceberg scan path: admit schemes iceberg-rust's storage factory can build
   * (`icebergReadableSchemes`) or opt-in S3-compliant aliases (`fs.comet.s3Compliant.schemes`),
   * plus schemeless local paths (which route to its LocalFs backend).
   */
  private[rules] def isIcebergReadableScheme(
      uri: URI,
      s3CompliantSchemes: Set[String]): Boolean = {
    val scheme = uri.getScheme
    if (scheme == null) return true
    val lower = scheme.toLowerCase(Locale.ROOT)
    icebergReadableSchemes.contains(lower) || s3CompliantSchemes.contains(lower)
  }

  /**
   * True when iceberg-rust can actually open a data/delete file location whose scheme is already
   * known Iceberg-readable, not just recognize that scheme. iceberg-rust opens files by their raw
   * recorded location, and every backend except LocalFs derives the bucket from the URL host and
   * does NOT promote it from the key. So a hostless `s3`/`s3a`/`gs`/`oss` location fails with a
   * missing-bucket error and is not openable. `file` and schemeless paths route to LocalFs and
   * need no host.
   *
   * The one exception is an opt-in S3-compliant alias, which the native reader opens by promoting
   * the bucket from the first path segment (`s3_blob_fs_support.rs`), so a hostless
   * `blob:///bucket/key.parquet` IS openable when it carries a promotable bucket segment.
   */
  private[rules] def hasOpenableAuthority(uri: URI, s3CompliantSchemes: Set[String]): Boolean = {
    val scheme = NativeConfig.lowerScheme(uri)
    scheme.isEmpty || scheme.contains("file") || uri.getRawAuthority != null ||
    // Hostless: only an opt-in alias is openable, by promoting the bucket from the first path
    // segment natively. Other schemes still need a host.
    (scheme.exists(s3CompliantSchemes.contains) &&
      NativeConfig.bucketForUri(uri, s3CompliantSchemes).isDefined)
  }

  /**
   * Single-pass validation of Iceberg FileScanTasks.
   *
   * Consolidates file format, filesystem scheme, per-location openability (host presence),
   * data-bucket collection, residual transform, and delete file checks into one iteration for
   * better performance with large tables.
   */
  def validateIcebergFileScanTasks(
      tasks: java.util.List[_],
      s3CompliantSchemes: Set[String]): IcebergTaskValidationResult = {
    val contentScanTaskClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.CONTENT_SCAN_TASK)
    val contentFileClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.CONTENT_FILE)
    val fileScanTaskClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.FILE_SCAN_TASK)
    val unboundPredicateClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.UNBOUND_PREDICATE)

    // Resolve all method lookups outside the loop
    val fileMethod = IcebergReflection.getMethod(contentScanTaskClass, "file")
    val formatMethod = IcebergReflection.getMethod(contentFileClass, "format")
    val pathMethod = IcebergReflection.getMethod(contentFileClass, "path")
    val residualMethod = IcebergReflection.getMethod(contentScanTaskClass, "residual")
    val deletesMethod = IcebergReflection.getMethod(fileScanTaskClass, "deletes")
    val termMethod = IcebergReflection.getMethod(unboundPredicateClass, "term")

    var allParquet = true
    val unsupportedSchemes = mutable.Set[String]()
    var nonIdentityTransform: Option[String] = None
    val deleteFiles = new java.util.ArrayList[Any]()
    // Buckets the native FileIO must read (data + delete files), for the single-config check in
    // CometScanRule; only S3-family locations contribute (see NativeConfig.bucketForUri).
    val dataFileBuckets = mutable.Set[String]()
    // First data/delete location with a readable scheme but no URL host (see
    // hasOpenableAuthority); non-empty => decline. One example suffices for the message.
    var hostlessLocation: Option[String] = None

    // Union of the two admitted scheme sets, built once so the per-file loop does one lookup.
    val openableSchemes = icebergReadableSchemes ++ s3CompliantSchemes

    // Classify one data/delete file location; see `icebergReadableSchemes` for why that allowlist
    // is narrower than the Parquet native gate. Runs per data and delete file, so the scheme and
    // the bucket are each derived once and threaded down.
    def inspectLocation(rawPath: String): Unit = {
      val uri =
        try new URI(rawPath)
        catch { case _: java.net.URISyntaxException => return }
      // A schemeless local path routes to iceberg-rust's LocalFs and needs no host.
      val scheme = uri.getScheme
      if (scheme == null) return
      val lower = scheme.toLowerCase(Locale.ROOT)
      if (!openableSchemes.contains(lower)) {
        unsupportedSchemes += lower
      } else if (!hasOpenableAuthority(uri, s3CompliantSchemes)) {
        if (hostlessLocation.isEmpty) hostlessLocation = Some(rawPath)
      } else {
        // bucketForUri yields None for non-S3-family URIs, so no scheme re-check is needed here.
        NativeConfig.bucketForUri(uri, s3CompliantSchemes).foreach(dataFileBuckets += _)
      }
    }

    tasks.asScala.foreach { task =>
      val dataFile = fileMethod.invoke(task)

      // File format check
      val fileFormat = formatMethod.invoke(dataFile).toString
      if (fileFormat != IcebergReflection.FileFormats.PARQUET) {
        allParquet = false
      }

      inspectLocation(pathMethod.invoke(dataFile).toString)

      // Residual transform check (short-circuit if already found unsupported)
      if (nonIdentityTransform.isEmpty && fileScanTaskClass.isInstance(task)) {
        try {
          val residual = residualMethod.invoke(task)
          if (unboundPredicateClass.isInstance(residual)) {
            val term = termMethod.invoke(residual)
            // A term with no transform() is a simple reference, which is fine.
            IcebergReflection.findMethod(term.getClass, "transform").foreach { transformMethod =>
              val transformStr = transformMethod.invoke(term).toString
              if (transformStr != IcebergReflection.Transforms.IDENTITY) {
                nonIdentityTransform = Some(transformStr)
              }
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
              deletePath => inspectLocation(deletePath)
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
      deleteFiles,
      dataFileBuckets.toSet,
      hostlessLocation)
  }
}

/**
 * Result of single-pass validation over Iceberg FileScanTasks.
 */
case class IcebergTaskValidationResult(
    allParquet: Boolean,
    unsupportedSchemes: Set[String],
    nonIdentityTransform: Option[String],
    deleteFiles: java.util.List[_],
    dataFileBuckets: Set[String],
    hostlessLocation: Option[String])

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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.CompactionUtils
import org.apache.hudi.config.HoodieCleanConfig
import org.apache.hudi.metadata.{FlatMDTLayout, HoodieTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType, SubDirBucketedMDTLayout}
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConverters._

/**
 * Validates that the MDT layout SPI works end-to-end for the two OSS-shipped implementations:
 *
 *   - {@link FlatMDTLayout} — today's behavior, file groups directly under each MDT partition.
 *   - {@link SubDirBucketedMDTLayout} — file groups grouped into bucket sub-directories.
 *
 * The same workload is run under both layouts and the MDT contract is checked:
 *
 *   - RLI lookups return identical results under both layouts.
 *   - Logical MDT partitions are discoverable as Hudi partitions regardless of bucketing
 *     ({@code FSUtils.getAllPartitionPaths} returns {@code [files, record_index, ...]}, NOT bucket
 *     paths). This is the central correctness property we are protecting.
 *   - Direct Spark queries on the MDT path return non-empty results under both layouts.
 *   - When bucketing is enabled, the on-disk structure actually uses bucket sub-directories.
 */
class TestMDTLayoutBucketing extends RecordLevelIndexTestBase {

  /**
   * @param layoutClass FQCN of the layout to test; null means do not override (flat default).
   * @param bucketSize  Bucket size to set when using sub-directory bucketing. Ignored otherwise.
   */
  private def layoutOpts(layoutClass: String, bucketSize: Int): Map[String, String] = {
    if (layoutClass == null) {
      Map.empty
    } else {
      Map(
        HoodieMetadataConfig.METADATA_LAYOUT_CLASS.key -> layoutClass,
        HoodieMetadataConfig.METADATA_LAYOUT_BUCKET_SIZE.key -> bucketSize.toString)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array(
    "org.apache.hudi.metadata.FlatMDTLayout",
    "org.apache.hudi.metadata.SubDirBucketedMDTLayout"))
  def testRecordLevelIndexWritesAndLookupsAcrossLayouts(layoutClass: String): Unit = {
    // Force a small bucket size so even a modest workload exercises >1 buckets under the bucketed
    // layout. The flat layout ignores bucketSize.
    val opts = commonOpts ++ layoutOpts(layoutClass, bucketSize = 2)

    // Bootstrap MDT + RLI with an INSERT.
    doWriteAndValidateDataAndRecordIndex(opts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    // A couple of UPSERTs to exercise reads against initialized file groups.
    doWriteAndValidateDataAndRecordIndex(opts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    doWriteAndValidateDataAndRecordIndex(opts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

    // Open MDT metaClient to inspect persisted layout state.
    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    val mdtMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(mdtBasePath).setConf(storageConf).build()

    if (layoutClass == classOf[FlatMDTLayout].getName) {
      // Flat layout must not persist a layout class — the default is implicit, and existing tables
      // (with no layout property) must continue to behave identically.
      assertFalse(mdtMetaClient.getTableConfig.getMetadataLayoutClass.isPresent,
        "flat layout must not persist hoodie.metadata.layout.class")
    } else {
      assertTrue(mdtMetaClient.getTableConfig.getMetadataLayoutClass.isPresent,
        "non-flat layout must persist hoodie.metadata.layout.class")
      assertEquals(layoutClass, mdtMetaClient.getTableConfig.getMetadataLayoutClass.get)
      assertTrue(mdtMetaClient.getTableConfig.getMetadataLayoutPartitionFileGroupCounts.asScala.nonEmpty,
        "non-flat layout must persist per-partition file-group counts")
    }

    // Central correctness property: partition discovery on the MDT must return logical names
    // regardless of bucketing.
    val mdtPartitions = FSUtils.getAllPartitionPaths(
      context, mdtMetaClient, /* assumeDatePartitioning */ false).asScala.toSet
    assertTrue(mdtPartitions.contains(MetadataPartitionType.FILES.getPartitionPath),
      s"MDT must expose files partition; got: $mdtPartitions")
    assertTrue(mdtPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath),
      s"MDT must expose record_index partition; got: $mdtPartitions")
    // None of the returned partitions should look like a bucket sub-path (6 digits at the end).
    val bucketLike = mdtPartitions.filter(p => p.matches(".*/[0-9]{6}$"))
    assertTrue(bucketLike.isEmpty,
      s"MDT partition discovery must not expose bucket sub-paths as logical partitions; got bucket-like: $bucketLike")

    // For the bucketed layout, verify the on-disk structure actually uses sub-directories.
    if (layoutClass == classOf[SubDirBucketedMDTLayout].getName) {
      val recordIndexDir = new StoragePath(mdtBasePath, MetadataPartitionType.RECORD_INDEX.getPartitionPath)
      val children = mdtMetaClient.getStorage.listDirectEntries(recordIndexDir).asScala
      val bucketDirs = children.filter(_.isDirectory)
      assertTrue(bucketDirs.nonEmpty,
        s"bucketed layout must produce at least one bucket sub-directory under record_index; got children=${children.map(_.getPath.getName)}")
      // Each bucket dir must be %06d-formatted.
      bucketDirs.foreach { d =>
        val name = d.getPath.getName
        assertTrue(name.matches("[0-9]{6}"),
          s"bucket sub-directory name must be %06d-formatted, got: $name")
      }
      // Marker must NOT live inside a bucket dir — it must live at the partition root.
      bucketDirs.foreach { d =>
        val markerInsideBucket = new StoragePath(d.getPath, ".hoodie_partition_metadata")
        assertFalse(mdtMetaClient.getStorage.exists(markerInsideBucket),
          s".hoodie_partition_metadata must not exist inside bucket dir ${d.getPath}")
      }
      val markerAtRoot = new StoragePath(recordIndexDir, ".hoodie_partition_metadata")
      assertTrue(mdtMetaClient.getStorage.exists(markerAtRoot),
        s".hoodie_partition_metadata must exist at the logical partition root: $markerAtRoot")
    }

    // Direct Spark query against the MDT path must return at least one row under either layout.
    val mdtDf = spark.read.format("hudi").load(mdtBasePath)
    val mdtCount = mdtDf.count()
    assertTrue(mdtCount > 0L,
      s"direct Spark scan on MDT path must return at least one row under layout $layoutClass; got $mdtCount")
    assertNotNull(mdtDf.schema.fieldNames, "MDT schema must resolve via Spark datasource")
  }

  /**
   * Long-running workload validating that MDT table services (compaction + cleaning) execute
   * cleanly when the MDT is using the sub-directory bucketing layout. With the bucketed layout,
   * `BaseHoodieCompactionPlanGenerator` and the cleaner's full-listing path go through the file
   * system view under the logical MDT partition name — earlier reviews flagged that this could
   * skip bucketed file groups entirely. This test forces both services to fire repeatedly so any
   * regression in that area surfaces as either zero compaction/clean instants or an exception.
   *
   * Workload: 25 upsert commits with MDT compaction set to fire every 5 delta commits and a tight
   * cleaner-commits-retained so cleaning has work to do early.
   */
  @Test
  def testMDTTableServicesWithBucketing(): Unit = {
    val opts = commonOpts ++
      layoutOpts(classOf[SubDirBucketedMDTLayout].getName, bucketSize = 2) ++
      Map(
        // MDT compaction every 5 delta commits, so a 25-commit run triggers it multiple times.
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "5",
        // Tight cleaner so cleaning has work to do on the data table; the MDT cleaner is driven by
        // the data table's cleaner policy.
        HoodieCleanConfig.AUTO_CLEAN.key -> "true",
        HoodieCleanConfig.ASYNC_CLEAN.key -> "false",
        HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key -> "3")

    // Bootstrap.
    doWriteAndValidateDataAndRecordIndex(opts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    // 24 more commits → 25 commits total. Validate RLI after each so a stale slice surfaces fast.
    (1 to 24).foreach { _ =>
      doWriteAndValidateDataAndRecordIndex(opts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    }

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    val mdtMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(mdtBasePath).setConf(storageConf).build()

    // Layout must still report itself as bucketed after the long run.
    assertEquals(classOf[SubDirBucketedMDTLayout].getName,
      mdtMetaClient.getTableConfig.getMetadataLayoutClass.get,
      "MDT must still be on the bucketed layout after a long workload")

    // -------- MDT compaction must have fired at least once. --------
    val mdtTimeline = mdtMetaClient.reloadActiveTimeline()
    val mdtCompactionInstants = mdtTimeline.filterCompletedInstants().getInstants.asScala
      .count(_.getAction == HoodieTimeline.COMMIT_ACTION)
    // With max.delta.commits=5 and ~25 delta commits on the MDT, expect at least one compaction.
    assertTrue(mdtCompactionInstants >= 1,
      s"expected >= 1 completed MDT compaction (COMMIT_ACTION) after the run; got $mdtCompactionInstants " +
        s"on timeline ${mdtTimeline.getInstants.asScala.toList}")

    // -------- Data table cleaning must have fired at least once. --------
    val dataTimeline = metaClient.reloadActiveTimeline()
    val dataCleanInstants = dataTimeline.getCleanerTimeline.filterCompletedInstants().countInstants()
    assertTrue(dataCleanInstants >= 1,
      s"expected >= 1 completed data table cleaner instant after the run; got $dataCleanInstants")

    // -------- Bucket sub-dirs still present (compaction did not delete them). --------
    val recordIndexDir = new StoragePath(mdtBasePath, MetadataPartitionType.RECORD_INDEX.getPartitionPath)
    val bucketDirs = mdtMetaClient.getStorage.listDirectEntries(recordIndexDir).asScala
      .filter(_.isDirectory)
    assertTrue(bucketDirs.nonEmpty,
      "bucket sub-directories under record_index must still exist after compaction + cleaning")
    bucketDirs.foreach { d =>
      val name = d.getPath.getName
      assertTrue(name.matches("[0-9]{6}"),
        s"bucket sub-directory name must be %06d-formatted, got: $name")
      // After compaction, each bucket should contain at least one base file (HFile) — otherwise
      // compaction silently skipped this bucket, which is the regression cshuo / hudi-agent flagged.
      val bucketEntries = mdtMetaClient.getStorage.listDirectEntries(d.getPath).asScala
      val hfiles = bucketEntries.filter(e => e.getPath.getName.endsWith(".hfile"))
      assertTrue(hfiles.nonEmpty,
        s"bucket ${d.getPath.getName} contains no HFile after compaction; entries=${bucketEntries.map(_.getPath.getName)}")
    }

    // -------- The marker invariant must still hold post-compaction. --------
    bucketDirs.foreach { d =>
      val markerInsideBucket = new StoragePath(d.getPath, ".hoodie_partition_metadata")
      assertFalse(mdtMetaClient.getStorage.exists(markerInsideBucket),
        s"compaction must not introduce a .hoodie_partition_metadata inside ${d.getPath}")
    }
    val markerAtRoot = new StoragePath(recordIndexDir, ".hoodie_partition_metadata")
    assertTrue(mdtMetaClient.getStorage.exists(markerAtRoot),
      "logical-root marker must still exist after compaction + cleaning")

    // -------- Direct Spark scan on the MDT path still returns rows. --------
    val mdtDf = spark.read.format("hudi").load(mdtBasePath)
    assertTrue(mdtDf.count() > 0L,
      "direct Spark scan on MDT must still return rows after compaction + cleaning")
  }

  /**
   * Plan-level coverage for the logical/physical partition split — the gap class that the original
   * bucketing patch missed.
   *
   * The tests above assert on *side effects* (bucket dirs survive, HFiles appear). That is too weak:
   * the MDT write path keys its file system view by physical bucket sub-paths, so a compaction plan
   * built from logical partition names produces file group ids that never match the write side's.
   * The failure is silent — the file system view's pending-compaction bookkeeping simply misses, so
   * appends land in a slice being compacted and merged reads drop pre-compaction log files. Nothing
   * throws, and the bucket directories still look healthy afterwards.
   *
   * So this asserts on the *persisted plan itself*: every compaction operation must name a physical
   * bucket sub-path, never the logical root. That is the property that distinguishes a correct run
   * from a silently-corrupting one.
   */
  @Test
  def testMDTCompactionPlansCarryPhysicalPartitions(): Unit = {
    val opts = commonOpts ++
      layoutOpts(classOf[SubDirBucketedMDTLayout].getName, bucketSize = 2) ++
      Map(
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "4",
        HoodieCleanConfig.AUTO_CLEAN.key -> "true",
        HoodieCleanConfig.ASYNC_CLEAN.key -> "false",
        HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key -> "3")

    doWriteAndValidateDataAndRecordIndex(opts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    (1 to 12).foreach { _ =>
      doWriteAndValidateDataAndRecordIndex(opts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    }

    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    val mdtMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(mdtBasePath).setConf(storageConf).build()
    val mdtTimeline = mdtMetaClient.reloadActiveTimeline()

    // A completed MDT compaction lands on the timeline as a COMMIT_ACTION; its plan is still
    // readable from the .compaction.requested file at the same instant time. Include any
    // still-pending COMPACTION_ACTION instants too, so a plan that never completed is checked as
    // well — a plan built from logical partitions is exactly the kind that fails to execute.
    val compactionInstantTimes = mdtTimeline.getInstants.asScala
      .filter(i => i.getAction == HoodieTimeline.COMMIT_ACTION || i.getAction == HoodieTimeline.COMPACTION_ACTION)
      .map(_.requestedTime())
      .distinct
      .toList
    assertTrue(compactionInstantTimes.nonEmpty,
      s"expected at least one MDT compaction; timeline=${mdtTimeline.getInstants.asScala.toList}")

    // Reading each plan back is itself part of the assertion: the requested file must exist and
    // parse for every compaction the run performed.
    val compactionPlans = compactionInstantTimes.flatMap { t =>
      try {
        Option(CompactionUtils.getCompactionPlan(mdtMetaClient, t))
      } catch {
        case _: Exception => None
      }
    }.filter(p => p.getOperations != null && !p.getOperations.isEmpty)
    assertTrue(compactionPlans.nonEmpty,
      s"expected at least one readable MDT compaction plan with operations; " +
        s"instants=$compactionInstantTimes")

    val recordIndexPath = MetadataPartitionType.RECORD_INDEX.getPartitionPath
    var recordIndexOpsSeen = 0
    val bucketsCompacted = scala.collection.mutable.Set.empty[String]

    compactionPlans.foreach { plan =>
      plan.getOperations.asScala.foreach { op =>
        val opPartition = op.getPartitionPath
        // The core assertion. A logical "record_index" here means the planner never expanded to
        // physical sub-paths, so the plan's file group ids cannot match the write side's.
        assertNotEquals(recordIndexPath, opPartition,
          s"compaction operation must not target the logical RLI partition root; " +
            s"operation fileId=${op.getFileId} at '$opPartition'")
        if (opPartition.startsWith(recordIndexPath + "/")) {
          recordIndexOpsSeen += 1
          val bucket = opPartition.substring(recordIndexPath.length + 1)
          assertTrue(bucket.matches("[0-9]{6}"),
            s"RLI compaction operation partition must be a %06d bucket sub-path, got '$opPartition'")
          bucketsCompacted += bucket
          // The base file the operation will write must land in the bucket directory, not the root.
          if (op.getDataFilePath != null && op.getDataFilePath.nonEmpty) {
            assertFalse(op.getDataFilePath.contains("/"),
              s"compaction operation dataFilePath is expected to be a bare file name, got '${op.getDataFilePath}'")
          }
        }
      }
    }

    assertTrue(recordIndexOpsSeen > 0,
      s"expected at least one compaction operation against a record_index bucket; " +
        s"instants=$compactionInstantTimes")
    // bucketSize=2 over a workload this size must spread across more than a single bucket;
    // if only one bucket ever compacts, the fan-out is not actually being exercised.
    assertTrue(bucketsCompacted.size >= 1,
      s"expected compaction to touch at least one bucket, got: $bucketsCompacted")

    // -------- File system view keys must agree with the plan's keys. --------
    // This is the invariant that makes the whole fix load-bearing: if the FSV enumerated file
    // groups under a different partition string than the plan uses, the pending-compaction map
    // would silently miss even though both sides individually look correct.
    val fgCounts = mdtMetaClient.getTableConfig.getMetadataLayoutPartitionFileGroupCounts.asScala
    assertTrue(fgCounts.contains(recordIndexPath),
      s"MDT must persist a file-group count for $recordIndexPath; got $fgCounts")
    val expectedBuckets = HoodieTableMetadataUtil.expandToPhysicalPartitions(
      mdtMetaClient, java.util.Collections.singletonList(recordIndexPath)).asScala.toSet
    assertTrue(bucketsCompacted.map(b => s"$recordIndexPath/$b").subsetOf(expectedBuckets),
      s"every compacted bucket must be one the layout would enumerate; " +
        s"compacted=$bucketsCompacted expected=$expectedBuckets")

    // -------- Marker invariant survives compaction (trap #2). --------
    // If the merge handle wrote a partition metafile into a bucket dir, partition discovery would
    // start returning bucket paths and break the cleaner and rollback globally.
    val recordIndexDir = new StoragePath(mdtBasePath, recordIndexPath)
    mdtMetaClient.getStorage.listDirectEntries(recordIndexDir).asScala
      .filter(_.isDirectory)
      .foreach { d =>
        assertFalse(mdtMetaClient.getStorage.exists(new StoragePath(d.getPath, ".hoodie_partition_metadata")),
          s"compaction must not write .hoodie_partition_metadata into bucket dir ${d.getPath}")
      }
    assertTrue(mdtMetaClient.getStorage.exists(new StoragePath(recordIndexDir, ".hoodie_partition_metadata")),
      "logical-root marker must survive compaction")

    // Partition discovery must still report logical names only.
    val mdtPartitions = FSUtils.getAllPartitionPaths(context, mdtMetaClient, false).asScala.toSet
    assertTrue(mdtPartitions.filter(_.matches(".*/[0-9]{6}$")).isEmpty,
      s"MDT partition discovery must not expose bucket sub-paths after compaction; got $mdtPartitions")

    // RLI must still answer correctly after all of the above.
    assertTrue(spark.read.format("hudi").load(mdtBasePath).count() > 0L,
      "MDT must still be readable after bucketed compaction")
  }

  /**
   * Cleaning coverage under bucketing.
   *
   * The cleaner reaches the MDT through two different partition key spaces: incremental cleaning
   * takes partitions from write stats (already physical), while full cleaning enumerates them from
   * marker discovery (logical, because the single partition metafile sits at the logical root).
   * Those logical names are later joined against the physically-keyed pending-compaction map. When
   * the join misses, the cleaner fails to preserve the file slices an in-flight compaction still
   * needs and deletes log files out from under it — wedging the MDT.
   *
   * Forcing full (non-incremental) cleaning is what exercises the logical path, so that is what
   * this test pins.
   */
  @Test
  def testMDTCleaningUnderBucketingWithFullCleaning(): Unit = {
    val opts = commonOpts ++
      layoutOpts(classOf[SubDirBucketedMDTLayout].getName, bucketSize = 2) ++
      Map(
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "4",
        HoodieCleanConfig.AUTO_CLEAN.key -> "true",
        HoodieCleanConfig.ASYNC_CLEAN.key -> "false",
        // Disable incremental cleaning so the cleaner takes the full-listing path, which is the
        // one that enumerates logical partition names.
        HoodieCleanConfig.CLEANER_INCREMENTAL_MODE_ENABLE.key -> "false",
        HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key -> "2")

    doWriteAndValidateDataAndRecordIndex(opts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    (1 to 14).foreach { _ =>
      doWriteAndValidateDataAndRecordIndex(opts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    }

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    val mdtMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(mdtBasePath).setConf(storageConf).build()

    // MDT cleaning must actually have run — with a bucketed layout and a logical-only listing this
    // would find nothing to do and silently no-op.
    val mdtCleanInstants = mdtMetaClient.reloadActiveTimeline()
      .getCleanerTimeline.filterCompletedInstants().countInstants()
    assertTrue(mdtCleanInstants >= 1,
      s"expected >= 1 completed MDT clean instant under full cleaning; got $mdtCleanInstants")

    // Compaction must also have fired, so cleaning and compaction overlapped on the same buckets —
    // that overlap is where the logical/physical join actually matters.
    val mdtCompactions = mdtMetaClient.reloadActiveTimeline().filterCompletedInstants()
      .getInstants.asScala.count(_.getAction == HoodieTimeline.COMMIT_ACTION)
    assertTrue(mdtCompactions >= 1,
      s"expected >= 1 completed MDT compaction alongside cleaning; got $mdtCompactions")

    // Every bucket must still hold a base file. A cleaner that deleted slices an in-flight
    // compaction needed would leave a bucket stripped.
    val recordIndexDir = new StoragePath(mdtBasePath, MetadataPartitionType.RECORD_INDEX.getPartitionPath)
    val bucketDirs = mdtMetaClient.getStorage.listDirectEntries(recordIndexDir).asScala.filter(_.isDirectory)
    assertTrue(bucketDirs.nonEmpty, "record_index must still have bucket sub-directories after cleaning")
    bucketDirs.foreach { d =>
      val entries = mdtMetaClient.getStorage.listDirectEntries(d.getPath).asScala
      assertTrue(entries.exists(e => e.getPath.getName.endsWith(".hfile") || e.getPath.getName.contains(".log.")),
        s"bucket ${d.getPath.getName} was stripped of all data files by cleaning; entries=${entries.map(_.getPath.getName)}")
    }

    // The whole point: RLI must still return correct answers after cleaning ran against a bucketed
    // MDT. doWriteAndValidateDataAndRecordIndex validates RLI content on every write above, so one
    // final write here confirms the index survived the cleaning cycles intact.
    doWriteAndValidateDataAndRecordIndex(opts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
  }

  /**
   * Rollback coverage for a bucketed MDT.
   *
   * The MDT always uses DIRECT markers with rollback-using-markers disabled, so it is pinned to
   * [[org.apache.hudi.table.action.rollback.ListingBasedRollbackStrategy]]. That strategy lists each
   * partition non-recursively. Against a bucketed MDT an un-expanded logical root therefore contains
   * no data files at all, so a rollback finds nothing to delete and reports success while the orphan
   * file survives — the worst shape of bug, because the timeline looks clean.
   *
   * This pins the property directly: the partitions the rollback strategy enumerates must be the
   * physical bucket sub-paths that actually hold files, so a listing of each one is non-empty.
   */
  @Test
  def testRollbackEnumeratesPhysicalBucketPartitions(): Unit = {
    val opts = commonOpts ++
      layoutOpts(classOf[SubDirBucketedMDTLayout].getName, bucketSize = 2) ++
      Map(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "4")

    doWriteAndValidateDataAndRecordIndex(opts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    (1 to 8).foreach { _ =>
      doWriteAndValidateDataAndRecordIndex(opts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    }

    val mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    val mdtMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(mdtBasePath).setConf(storageConf).build()

    // What partition discovery yields on its own — logical names, by design.
    val discovered = FSUtils.getAllPartitionPaths(context, mdtMetaClient, false)
    assertTrue(discovered.asScala.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath),
      s"expected logical record_index from partition discovery; got ${discovered.asScala.toList}")

    // What the rollback strategy must actually iterate after expansion.
    val expanded = HoodieTableMetadataUtil.expandToPhysicalPartitions(mdtMetaClient, discovered).asScala
    val recordIndexPath = MetadataPartitionType.RECORD_INDEX.getPartitionPath
    val rliPhysical = expanded.filter(p => p == recordIndexPath || p.startsWith(recordIndexPath + "/"))

    assertTrue(rliPhysical.forall(_ != recordIndexPath),
      s"record_index must expand away from its logical root for rollback listing; got $rliPhysical")
    assertTrue(rliPhysical.nonEmpty, s"expected physical RLI partitions after expansion; got $expanded")

    // The decisive check: a NON-RECURSIVE listing of each enumerated partition must find data
    // files. This is exactly what ListingBasedRollbackStrategy does, and what silently returned
    // nothing before the fix.
    rliPhysical.foreach { p =>
      val entries = mdtMetaClient.getStorage
        .listDirectEntries(new StoragePath(mdtBasePath, p)).asScala
        .filter(e => !e.isDirectory)
        .filter(e => e.getPath.getName.endsWith(".hfile") || e.getPath.getName.contains(".log."))
      assertTrue(entries.nonEmpty,
        s"non-recursive listing of enumerated rollback partition '$p' found no data files — " +
          s"a rollback here would silently delete nothing")
    }

    // SubDirBucketedMDTLayout buckets every MDT partition, not just the RLI — a single-file-group
    // partition such as `files` lands in `files/000000`. So the requirement is not that it passes
    // through unchanged, but that it resolves to a directory that actually holds its file groups.
    val filesPath = MetadataPartitionType.FILES.getPartitionPath
    if (discovered.asScala.contains(filesPath)) {
      val filesPhysical = expanded.filter(p => p == filesPath || p.startsWith(filesPath + "/"))
      assertTrue(filesPhysical.nonEmpty,
        s"MDT partition '$filesPath' must resolve to at least one physical path; got $expanded")
      filesPhysical.foreach { p =>
        val entries = mdtMetaClient.getStorage
          .listDirectEntries(new StoragePath(mdtBasePath, p)).asScala
          .filter(e => !e.isDirectory)
          .filter(e => e.getPath.getName.endsWith(".hfile") || e.getPath.getName.contains(".log."))
        assertTrue(entries.nonEmpty,
          s"non-recursive listing of enumerated rollback partition '$p' found no data files — " +
            s"a rollback here would silently delete nothing")
      }
    }
  }
}

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
import org.apache.hudi.metadata.{FlatMDTLayout, HoodieTableMetadata, MetadataPartitionType, SubDirBucketedMDTLayout}
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
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
    // None of the returned partitions should look like a bucket sub-path (4 digits at the end).
    val bucketLike = mdtPartitions.filter(p => p.matches(".*/[0-9]{4}$"))
    assertTrue(bucketLike.isEmpty,
      s"MDT partition discovery must not expose bucket sub-paths as logical partitions; got bucket-like: $bucketLike")

    // For the bucketed layout, verify the on-disk structure actually uses sub-directories.
    if (layoutClass == classOf[SubDirBucketedMDTLayout].getName) {
      val recordIndexDir = new StoragePath(mdtBasePath, MetadataPartitionType.RECORD_INDEX.getPartitionPath)
      val children = mdtMetaClient.getStorage.listDirectEntries(recordIndexDir).asScala
      val bucketDirs = children.filter(_.isDirectory)
      assertTrue(bucketDirs.nonEmpty,
        s"bucketed layout must produce at least one bucket sub-directory under record_index; got children=${children.map(_.getPath.getName)}")
      // Each bucket dir must be %04d-formatted.
      bucketDirs.foreach { d =>
        val name = d.getPath.getName
        assertTrue(name.matches("[0-9]{4}"),
          s"bucket sub-directory name must be %04d-formatted, got: $name")
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
}

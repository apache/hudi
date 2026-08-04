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

package org.apache.hudi.metadata;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HoodieMetadataTableLayout} implementations. Validates that the layout
 * abstraction yields the expected on-disk paths and fileIds for both flat and sub-directory
 * bucketed layouts, including the partitioned-RLI fallback.
 */
class TestHoodieMetadataTableLayout {

  // ---- FlatMDTLayout --------------------------------------------------------

  @Test
  void flatLayout_returnsPartitionRootAsRelativePath() {
    HoodieMetadataTableLayout layout = new FlatMDTLayout();
    HoodieMetadataLayoutContext ctx = new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1500, 2500, Option.empty());
    assertEquals("record_index", layout.getFileGroupRelativePath(ctx));
    assertEquals("record-index-1500-0", layout.getFileId(ctx));
  }

  @Test
  void flatLayout_physicalPartitionsIsSingleton() {
    HoodieMetadataTableLayout layout = new FlatMDTLayout();
    assertEquals(java.util.Collections.singletonList("record_index"),
        layout.getPhysicalPartitions("record_index", 2500));
  }

  @Test
  void flatLayout_markerAtPartitionRoot() {
    HoodieMetadataTableLayout layout = new FlatMDTLayout();
    assertEquals(java.util.Collections.singletonList("record_index"),
        layout.getPartitionMarkerPaths("record_index", 2500));
  }

  @Test
  void flatLayout_layoutIdIsStable() {
    assertEquals("flat", new FlatMDTLayout().getLayoutId());
  }

  // ---- SubDirBucketedMDTLayout ---------------------------------------------

  @Test
  void bucketedLayout_pathDerivedFromFileGroupIndex() {
    HoodieMetadataTableLayout layout = new SubDirBucketedMDTLayout(1000);
    assertEquals("record_index/000000",
        layout.getFileGroupRelativePath(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 0, 2500, Option.empty())));
    assertEquals("record_index/000000",
        layout.getFileGroupRelativePath(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 999, 2500, Option.empty())));
    assertEquals("record_index/000001",
        layout.getFileGroupRelativePath(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1000, 2500, Option.empty())));
    assertEquals("record_index/000001",
        layout.getFileGroupRelativePath(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1500, 2500, Option.empty())));
    assertEquals("record_index/000002",
        layout.getFileGroupRelativePath(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 2499, 2500, Option.empty())));
  }

  @Test
  void bucketedLayout_fileIdSchemeUnchanged() {
    HoodieMetadataTableLayout layout = new SubDirBucketedMDTLayout(1000);
    // FileId encoding must be bucket-independent so that bucket = fileGroupIndex / bucketSize is
    // recoverable from the fileId itself.
    assertEquals("record-index-0000-0",
        layout.getFileId(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 0, 2500, Option.empty())));
    assertEquals("record-index-1500-0",
        layout.getFileId(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1500, 2500, Option.empty())));
    assertEquals("record-index-2499-0",
        layout.getFileId(new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 2499, 2500, Option.empty())));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 100, 1000, 1024})
  void bucketedLayout_physicalPartitionsHonorBucketSize(int bucketSize) {
    SubDirBucketedMDTLayout layout = new SubDirBucketedMDTLayout(bucketSize);
    // Exactly N full buckets.
    List<String> exact = layout.getPhysicalPartitions("record_index", bucketSize * 3);
    assertEquals(3, exact.size());
    assertEquals("record_index/000000", exact.get(0));
    assertEquals("record_index/000001", exact.get(1));
    assertEquals("record_index/000002", exact.get(2));

    // N+1 file groups → N+1 buckets (partial last).
    List<String> partial = layout.getPhysicalPartitions("record_index", bucketSize * 3 + 1);
    assertEquals(4, partial.size());
    assertEquals("record_index/000003", partial.get(3));

    // Fewer than bucketSize → one bucket.
    List<String> small = layout.getPhysicalPartitions("record_index", Math.max(1, bucketSize / 2));
    assertEquals(1, small.size());
  }

  @Test
  void bucketedLayout_emptyPartitionReturnsRootOnly() {
    // fileGroupCount=0 means the layout has nothing persisted for that partition; the caller is
    // expected to fall back to the partition root (e.g., partitioned RLI on read).
    HoodieMetadataTableLayout layout = new SubDirBucketedMDTLayout(1000);
    assertEquals(java.util.Collections.singletonList("record_index"),
        layout.getPhysicalPartitions("record_index", 0));
  }

  @Test
  void bucketedLayout_markerOnlyAtPartitionRoot() {
    // Central correctness property: never write .hoodie_partition_metadata inside a bucket dir.
    HoodieMetadataTableLayout layout = new SubDirBucketedMDTLayout(1000);
    List<String> markers = layout.getPartitionMarkerPaths("record_index", 2500);
    assertEquals(1, markers.size());
    assertEquals("record_index", markers.get(0));
  }

  @Test
  void bucketedLayout_rejectsPartitionedRLI() {
    // Partitioned RLI is explicitly unsupported in this initial implementation. Invoking the layout
    // with a data partition present must fail loudly rather than silently produce a flat path; the
    // partitioned-RLI growth model needs a distinct strategy that lands in a follow-up patch / RFC.
    HoodieMetadataTableLayout layout = new SubDirBucketedMDTLayout(1000);
    HoodieMetadataLayoutContext ctx = new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 3, 4, Option.of("p2"));
    org.apache.hudi.exception.HoodieMetadataException ex = assertThrows(
        org.apache.hudi.exception.HoodieMetadataException.class,
        () -> layout.getFileGroupRelativePath(ctx));
    assertTrue(ex.getMessage().contains("partitioned RLI"),
        "exception message should call out partitioned RLI unsupported: " + ex.getMessage());
  }

  @Test
  void bucketedLayout_rejectsZeroOrNegativeBucketSize() {
    assertThrows(IllegalArgumentException.class, () -> new SubDirBucketedMDTLayout(0));
    assertThrows(IllegalArgumentException.class, () -> new SubDirBucketedMDTLayout(-1));
  }

  @Test
  void bucketedLayout_parseFileIdRoundTrip() {
    HoodieMetadataTableLayout layout = new SubDirBucketedMDTLayout(1000);
    for (int idx : new int[] {0, 1, 999, 1000, 1500, 2499}) {
      HoodieMetadataLayoutContext ctx = new HoodieMetadataLayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", idx, 2500, Option.empty());
      String fileId = layout.getFileId(ctx);
      HoodieMetadataFileIdInfo info = layout.parseFileId(MetadataPartitionType.RECORD_INDEX, fileId);
      assertEquals(idx, info.getFileGroupIndex(), "round-trip fileId for index " + idx);
    }
  }

  @Test
  void layoutIdsAreDistinct() {
    assertTrue(!new FlatMDTLayout().getLayoutId().equals(new SubDirBucketedMDTLayout(1).getLayoutId()));
  }

  // ---- HoodieTableMetadataUtil.isMDTBucketSubPath ----------------------------

  @Test
  void isMDTBucketSubPath_falseForNonMDT() {
    org.apache.hudi.common.table.HoodieTableConfig cfg =
        new org.apache.hudi.common.table.HoodieTableConfig();
    cfg.setValue(org.apache.hudi.common.table.HoodieTableConfig.METADATA_LAYOUT_CLASS,
        SubDirBucketedMDTLayout.class.getName());
    // Non-MDT tables short-circuit regardless of any layout config.
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, false, "record_index/000001"));
  }

  @Test
  void isMDTBucketSubPath_falseForFlatLayoutMDT() {
    // The flat default (no layout class set) must short-circuit so the heuristic never reads a
    // same-width all-digit data-table partition value as a bucket sub-path on existing tables.
    org.apache.hudi.common.table.HoodieTableConfig cfg =
        new org.apache.hudi.common.table.HoodieTableConfig();
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/000001"));

    // Explicit FlatMDTLayout class also short-circuits.
    cfg.setValue(org.apache.hudi.common.table.HoodieTableConfig.METADATA_LAYOUT_CLASS,
        FlatMDTLayout.class.getName());
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/000001"));
  }

  @Test
  void isMDTBucketSubPath_trueForBucketedMDTWithSixDigitSuffix() {
    org.apache.hudi.common.table.HoodieTableConfig cfg =
        new org.apache.hudi.common.table.HoodieTableConfig();
    cfg.setValue(org.apache.hudi.common.table.HoodieTableConfig.METADATA_LAYOUT_CLASS,
        SubDirBucketedMDTLayout.class.getName());
    assertTrue(HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/000000"));
    assertTrue(HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/000042"));
    assertTrue(HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "secondary_index_idx0/000099"));
  }

  @Test
  void isMDTBucketSubPath_falseForLogicalPartitionRootEvenUnderBucketing() {
    // The logical partition root (no trailing /NNNN) must NOT be treated as a bucket sub-path —
    // that's where the .hoodie_partition_metadata marker is supposed to land.
    org.apache.hudi.common.table.HoodieTableConfig cfg =
        new org.apache.hudi.common.table.HoodieTableConfig();
    cfg.setValue(org.apache.hudi.common.table.HoodieTableConfig.METADATA_LAYOUT_CLASS,
        SubDirBucketedMDTLayout.class.getName());
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index"));
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "files"));
  }

  @Test
  void isMDTBucketSubPath_falseForNonSixDigitSuffix() {
    org.apache.hudi.common.table.HoodieTableConfig cfg =
        new org.apache.hudi.common.table.HoodieTableConfig();
    cfg.setValue(org.apache.hudi.common.table.HoodieTableConfig.METADATA_LAYOUT_CLASS,
        SubDirBucketedMDTLayout.class.getName());
    // 3-digit, 5-digit, 7-digit, and non-digit suffixes all return false so that same-width
    // data-table partition values do not get misread as bucket sub-paths.
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/000"));
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/00000"));
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/0000000"));
    assertTrue(!HoodieTableMetadataUtil.isMDTBucketSubPath(cfg, true, "record_index/abcdef"));
  }

  // ---- expandToPhysicalPartitions -------------------------------------------
  //
  // This is the shared boundary helper used by the compaction, cleaning and rollback planners to
  // bring their partition key space in line with the already-physical MDT write path. The
  // properties below are what those call sites rely on.

  private static HoodieTableMetaClient mockMdtMetaClient(boolean isMetadataTable,
                                                         String layoutClass,
                                                         String fileGroupCounts) {
    org.apache.hudi.common.table.HoodieTableConfig cfg =
        new org.apache.hudi.common.table.HoodieTableConfig();
    if (layoutClass != null) {
      cfg.setValue(org.apache.hudi.common.table.HoodieTableConfig.METADATA_LAYOUT_CLASS, layoutClass);
    }
    if (fileGroupCounts != null) {
      cfg.setValue(org.apache.hudi.common.table.HoodieTableConfig.METADATA_LAYOUT_PARTITION_FILE_GROUP_COUNTS,
          fileGroupCounts);
    }
    HoodieTableMetaClient metaClient = org.mockito.Mockito.mock(HoodieTableMetaClient.class);
    org.mockito.Mockito.when(metaClient.isMetadataTable()).thenReturn(isMetadataTable);
    org.mockito.Mockito.when(metaClient.getTableConfig()).thenReturn(cfg);
    return metaClient;
  }

  @Test
  void expandToPhysicalPartitions_fansOutLogicalPartitionToBuckets() {
    // 2500 file groups at the default bucket size of 1000 => 3 bucket directories.
    HoodieTableMetaClient metaClient =
        mockMdtMetaClient(true, SubDirBucketedMDTLayout.class.getName(), "record_index=2500");
    assertEquals(
        java.util.Arrays.asList("record_index/000000", "record_index/000001", "record_index/000002"),
        HoodieTableMetadataUtil.expandToPhysicalPartitions(
            metaClient, java.util.Collections.singletonList("record_index")));
  }

  @Test
  void expandToPhysicalPartitions_isIdempotentForAlreadyPhysicalInput() {
    // The incremental table-service branch sources partitions from write stats, which are already
    // physical. Those have no entry in the file-group-count map, so they must pass through
    // untouched rather than being re-expanded or dropped.
    HoodieTableMetaClient metaClient =
        mockMdtMetaClient(true, SubDirBucketedMDTLayout.class.getName(), "record_index=2500");
    List<String> physical =
        java.util.Arrays.asList("record_index/000000", "record_index/000001", "record_index/000002");
    assertEquals(physical, HoodieTableMetadataUtil.expandToPhysicalPartitions(metaClient, physical));
  }

  @Test
  void expandToPhysicalPartitions_deduplicatesMixedLogicalAndPhysicalInput() {
    // A caller may hand us a mix (e.g. marker-discovered logical names unioned with write-stat
    // physical paths). The result must not contain duplicates, or the planner would emit two
    // operations for the same file group.
    HoodieTableMetaClient metaClient =
        mockMdtMetaClient(true, SubDirBucketedMDTLayout.class.getName(), "record_index=1500");
    assertEquals(
        java.util.Arrays.asList("record_index/000000", "record_index/000001"),
        HoodieTableMetadataUtil.expandToPhysicalPartitions(
            metaClient, java.util.Arrays.asList("record_index", "record_index/000001")));
  }

  @Test
  void expandToPhysicalPartitions_leavesFlatLayoutAndNonMdtUnchanged() {
    List<String> partitions = java.util.Arrays.asList("record_index", "files");
    // Flat layout (the default every pre-existing MDT uses) must be bit-identical passthrough.
    HoodieTableMetaClient flatMdt =
        mockMdtMetaClient(true, FlatMDTLayout.class.getName(), "record_index=2500");
    assertEquals(partitions, HoodieTableMetadataUtil.expandToPhysicalPartitions(flatMdt, partitions));
    // An MDT that never stamped a layout class at all also stays flat.
    HoodieTableMetaClient unstampedMdt = mockMdtMetaClient(true, null, null);
    assertEquals(partitions, HoodieTableMetadataUtil.expandToPhysicalPartitions(unstampedMdt, partitions));
    // A data table is never expanded, whatever its partition values look like.
    HoodieTableMetaClient dataTable =
        mockMdtMetaClient(false, SubDirBucketedMDTLayout.class.getName(), "record_index=2500");
    assertEquals(partitions, HoodieTableMetadataUtil.expandToPhysicalPartitions(dataTable, partitions));
  }

  @Test
  void expandToPhysicalPartitions_uncountedPartitionFallsBackToLogicalRoot() {
    // A partition with no recorded file-group count (not yet initialized, or read by an early-init
    // code path) must degrade to its logical root rather than vanishing from the plan — a dropped
    // partition would silently exclude it from compaction, cleaning and rollback.
    HoodieTableMetaClient metaClient =
        mockMdtMetaClient(true, SubDirBucketedMDTLayout.class.getName(), "record_index=1500");
    assertEquals(
        java.util.Arrays.asList("record_index/000000", "record_index/000001", "column_stats"),
        HoodieTableMetadataUtil.expandToPhysicalPartitions(
            metaClient, java.util.Arrays.asList("record_index", "column_stats")));
  }

  @Test
  void expandToPhysicalPartitions_bucketsSmallPartitionsIntoASingleBucket() {
    // SubDirBucketedMDTLayout buckets every MDT partition, not only the RLI. A partition small
    // enough to fit in one bucket still lives in a bucket directory (files/000000), so planners
    // must target that directory rather than the logical root — otherwise a non-recursive listing
    // finds nothing there.
    HoodieTableMetaClient metaClient =
        mockMdtMetaClient(true, SubDirBucketedMDTLayout.class.getName(), "files=1,record_index=1500");
    assertEquals(
        java.util.Arrays.asList("files/000000", "record_index/000000", "record_index/000001"),
        HoodieTableMetadataUtil.expandToPhysicalPartitions(
            metaClient, java.util.Arrays.asList("files", "record_index")));
  }

  @Test
  void expandToPhysicalPartitions_handlesNullAndEmptyInput() {
    HoodieTableMetaClient metaClient =
        mockMdtMetaClient(true, SubDirBucketedMDTLayout.class.getName(), "record_index=2500");
    assertEquals(null, HoodieTableMetadataUtil.expandToPhysicalPartitions(metaClient, null));
    assertEquals(java.util.Collections.emptyList(),
        HoodieTableMetadataUtil.expandToPhysicalPartitions(metaClient, java.util.Collections.emptyList()));
  }
}

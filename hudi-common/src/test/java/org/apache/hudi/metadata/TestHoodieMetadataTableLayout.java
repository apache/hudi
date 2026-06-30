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
    LayoutContext ctx = new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1500, 2500, Option.empty());
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
    assertEquals("record_index/0000",
        layout.getFileGroupRelativePath(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 0, 2500, Option.empty())));
    assertEquals("record_index/0000",
        layout.getFileGroupRelativePath(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 999, 2500, Option.empty())));
    assertEquals("record_index/0001",
        layout.getFileGroupRelativePath(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1000, 2500, Option.empty())));
    assertEquals("record_index/0001",
        layout.getFileGroupRelativePath(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1500, 2500, Option.empty())));
    assertEquals("record_index/0002",
        layout.getFileGroupRelativePath(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 2499, 2500, Option.empty())));
  }

  @Test
  void bucketedLayout_fileIdSchemeUnchanged() {
    HoodieMetadataTableLayout layout = new SubDirBucketedMDTLayout(1000);
    // FileId encoding must be bucket-independent so that bucket = fileGroupIndex / bucketSize is
    // recoverable from the fileId itself.
    assertEquals("record-index-0000-0",
        layout.getFileId(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 0, 2500, Option.empty())));
    assertEquals("record-index-1500-0",
        layout.getFileId(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 1500, 2500, Option.empty())));
    assertEquals("record-index-2499-0",
        layout.getFileId(new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 2499, 2500, Option.empty())));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 100, 1000, 1024})
  void bucketedLayout_physicalPartitionsHonorBucketSize(int bucketSize) {
    SubDirBucketedMDTLayout layout = new SubDirBucketedMDTLayout(bucketSize);
    // Exactly N full buckets.
    List<String> exact = layout.getPhysicalPartitions("record_index", bucketSize * 3);
    assertEquals(3, exact.size());
    assertEquals("record_index/0000", exact.get(0));
    assertEquals("record_index/0001", exact.get(1));
    assertEquals("record_index/0002", exact.get(2));

    // N+1 file groups → N+1 buckets (partial last).
    List<String> partial = layout.getPhysicalPartitions("record_index", bucketSize * 3 + 1);
    assertEquals(4, partial.size());
    assertEquals("record_index/0003", partial.get(3));

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
    LayoutContext ctx = new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", 3, 4, Option.of("p2"));
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
      LayoutContext ctx = new LayoutContext(MetadataPartitionType.RECORD_INDEX, "record_index", idx, 2500, Option.empty());
      String fileId = layout.getFileId(ctx);
      FileIdInfo info = layout.parseFileId(MetadataPartitionType.RECORD_INDEX, fileId);
      assertEquals(idx, info.getFileGroupIndex(), "round-trip fileId for index " + idx);
    }
  }

  @Test
  void layoutIdsAreDistinct() {
    assertTrue(!new FlatMDTLayout().getLayoutId().equals(new SubDirBucketedMDTLayout(1).getLayoutId()));
  }
}

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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.storage.StoragePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Opt-in MDT layout that distributes file groups into bucket sub-directories so a
 * single MDT partition directory does not exceed per-directory file-count limits
 * common on HDFS-style filesystems.
 *
 * <p>On-disk shape for {@code record_index} with {@code fileGroupCount=2500} and
 * {@code bucketSize=1000}:
 * <pre>
 *   .hoodie/metadata/record_index/
 *   ├── .hoodie_partition_metadata               ← single marker at the partition root
 *   ├── 0000/
 *   │   ├── .record-index-0000-0_&lt;instant&gt;.log
 *   │   ├── ...
 *   │   └── .record-index-0999-0_&lt;instant&gt;.log
 *   ├── 0001/
 *   │   └── ...
 *   └── 0002/
 *       └── ... (partial)
 * </pre>
 *
 * <p>Notes:
 * <ul>
 *   <li>The fileId scheme is unchanged. The bucket for a given file group is
 *   recoverable from the file-group index, which is itself encoded in the
 *   fileId. No additional state is required to map a fileId to its bucket.</li>
 *   <li>A single {@code .hoodie_partition_metadata} marker lives at the logical
 *   MDT partition root. None are written inside bucket sub-directories. This
 *   preserves MDT-as-Hudi-table semantics so {@code FSUtils.getAllPartitionPaths}
 *   on the MDT returns logical partition names rather than physical bucket
 *   paths.</li>
 *   <li>Reads enumerate the physical sub-paths via
 *   {@link #getPhysicalPartitions(String, int)} sourced from the MDT's persisted
 *   {@code hoodie.metadata.layout.partition.file.group.counts} property. No
 *   filesystem listing is needed on the read path.</li>
 * </ul>
 *
 * <p><b>Scope:</b> this initial implementation supports the non-partitioned MDT
 * partitions (files, column_stats, bloom_filters, expression_index,
 * secondary_index, and the global RLI). Partitioned RLI is rejected up front: if
 * a writer enables this layout on a table whose RLI is in the partitioned mode,
 * {@link #getFileGroupRelativePath} throws {@link HoodieMetadataException}.
 * Partitioned-RLI bucketing requires a different growth model and lands in a
 * separate follow-up.
 */
public final class SubDirBucketedMDTLayout implements HoodieMetadataTableLayout {

  public static final String LAYOUT_ID = "subdir-bucketed";
  public static final int DEFAULT_BUCKET_SIZE = 1000;

  private final int bucketSize;

  public SubDirBucketedMDTLayout() {
    this(DEFAULT_BUCKET_SIZE);
  }

  public SubDirBucketedMDTLayout(int bucketSize) {
    ValidationUtils.checkArgument(bucketSize > 0,
        "SubDirBucketedMDTLayout bucketSize must be > 0, got " + bucketSize);
    this.bucketSize = bucketSize;
  }

  @Override
  public String getLayoutId() {
    return LAYOUT_ID;
  }

  public int getBucketSize() {
    return bucketSize;
  }

  @Override
  public String getFileGroupRelativePath(LayoutContext ctx) {
    if (ctx.getDataPartitionName().isPresent()) {
      throw new HoodieMetadataException(
          "SubDirBucketedMDTLayout does not support partitioned RLI. The MDT was opened with "
              + "a data-partition-keyed file group for partition '"
              + ctx.getRelativePartitionPath() + "' (data partition '"
              + ctx.getDataPartitionName().get() + "'). Disable hoodie.metadata.layout.class "
              + "or switch the RLI to the global (non-partitioned) mode.");
    }
    int bucket = ctx.getFileGroupIndex() / bucketSize;
    return bucketRelativePath(ctx.getRelativePartitionPath(), bucket);
  }

  @Override
  public String getFileId(LayoutContext ctx) {
    // FileId scheme is bucket-independent. The bucket is recoverable from the
    // file-group index, which is encoded in the fileId itself.
    return HoodieTableMetadataUtil.getFileIDForFileGroup(
        ctx.getPartitionType(),
        ctx.getFileGroupIndex(),
        ctx.getRelativePartitionPath(),
        ctx.getDataPartitionName());
  }

  @Override
  public FileIdInfo parseFileId(MetadataPartitionType partitionType, String fileId) {
    int idx = HoodieTableMetadataUtil.getFileGroupIndexFromFileId(fileId);
    return new FileIdInfo(idx, Option.empty());
  }

  @Override
  public List<String> getPhysicalPartitions(String logicalPartition, int fileGroupCount) {
    if (fileGroupCount <= 0) {
      // Caller has no recorded count for this logical partition (e.g., the partition
      // is not yet initialized or is being read by an early-init code path). Fall
      // back to the partition root; the FS view will return zero file groups, which
      // is the correct empty-state for callers that pre-check existence themselves.
      return Collections.singletonList(logicalPartition);
    }
    int numBuckets = (int) Math.ceil((double) fileGroupCount / bucketSize);
    List<String> paths = new ArrayList<>(numBuckets);
    for (int b = 0; b < numBuckets; b++) {
      paths.add(bucketRelativePath(logicalPartition, b));
    }
    return paths;
  }

  @Override
  public List<String> getPartitionMarkerPaths(String logicalPartition, int fileGroupCount) {
    // Single marker at the logical partition root. Never inside a bucket dir;
    // that is the central correctness property that lets partition discovery on
    // the MDT still return logical names rather than physical sub-paths.
    return Collections.singletonList(logicalPartition);
  }

  private static String bucketRelativePath(String partitionRelativePath, int bucketIndex) {
    return String.format("%s%s%04d", partitionRelativePath, StoragePath.SEPARATOR, bucketIndex);
  }
}

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

import java.io.Serializable;
import java.util.List;

/**
 * Strategy that controls how MDT file groups are organized on disk.
 *
 * <p>The layout is selected at MDT initialization via the
 * {@code hoodie.metadata.layout.class} table property and is immutable for
 * the lifetime of the MDT. Implementations must be safe to serialize across
 * executors and must be deterministic for given inputs.
 *
 * <p>Two implementations ship in OSS:
 * <ul>
 *   <li>{@link FlatMDTLayout} (default) — every file group lives directly
 *   under its MDT partition directory; this is the existing behavior.</li>
 *   <li>{@link SubDirBucketedMDTLayout} (opt-in) — file groups for
 *   non-partitioned MDT partitions are grouped into bucket sub-directories
 *   (e.g. {@code record_index/0000/}, {@code record_index/0001/}) to lift the
 *   per-directory file-count limit on HDFS-like filesystems. Partitioned RLI
 *   falls back to the flat layout in this implementation.</li>
 * </ul>
 *
 * <p>Third parties may ship custom implementations and wire them in via the
 * {@code hoodie.metadata.layout.class} property.
 */
public interface HoodieMetadataTableLayout extends Serializable {

  /**
   * Stable identifier for this layout. Persisted on the MDT at init time and
   * asserted on every open as a guard against the configured class being
   * swapped to one with mismatched on-disk semantics.
   */
  String getLayoutId();

  /**
   * Return the path (relative to the MDT base path) where the file group
   * described by {@code ctx} should live.
   *
   * <p>For the flat layout this is always the partition's path. For
   * sub-directory bucketing this includes the bucket sub-directory.
   */
  String getFileGroupRelativePath(LayoutContext ctx);

  /**
   * Return the fileId to use for the file group described by {@code ctx}.
   *
   * <p>Most layouts will delegate to
   * {@link HoodieTableMetadataUtil#getFileIDForFileGroup} since the fileId
   * scheme is independent of the on-disk directory layout.
   */
  String getFileId(LayoutContext ctx);

  /**
   * Inverse of {@link #getFileId}: given a fileId observed on disk, recover
   * its global file-group index and (for partitioned RLI) its data-partition
   * name.
   */
  FileIdInfo parseFileId(MetadataPartitionType partitionType, String fileId);

  /**
   * Given a logical MDT partition name and the total number of file groups
   * known to live under it, return the list of sub-paths to scan to discover
   * all file groups.
   *
   * <p>For the flat layout this is {@code [logicalPartition]}. For
   * sub-directory bucketing this returns the list of bucket directories
   * (e.g. {@code ["record_index/0000", "record_index/0001", ...]}).
   *
   * <p>The {@code fileGroupCount} is supplied by the caller (sourced from MDT
   * properties) so this method must not perform any filesystem listing.
   */
  List<String> getPhysicalPartitions(String logicalPartition, int fileGroupCount);

  /**
   * Return the paths (relative to the MDT base path) where
   * {@code .hoodie_partition_metadata} markers must be written for this
   * logical partition.
   *
   * <p>To preserve MDT-as-Hudi-table semantics, layouts should place a single
   * marker at the logical partition root, never inside a bucket
   * sub-directory.
   */
  List<String> getPartitionMarkerPaths(String logicalPartition, int fileGroupCount);
}

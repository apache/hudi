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

import java.util.Collections;
import java.util.List;

/**
 * Default MDT layout: every file group lives directly under its metadata
 * partition directory, and a single {@code .hoodie_partition_metadata} marker
 * lives at the partition root. This is the layout that existed before the
 * layout SPI was introduced and remains the default for all tables that have
 * not explicitly opted into a different layout.
 */
public final class FlatMDTLayout implements HoodieMetadataTableLayout {

  public static final String LAYOUT_ID = "flat";

  @Override
  public String getLayoutId() {
    return LAYOUT_ID;
  }

  @Override
  public String getFileGroupRelativePath(LayoutContext ctx) {
    return ctx.getPartitionType().getPartitionPath();
  }

  @Override
  public String getFileId(LayoutContext ctx) {
    return HoodieTableMetadataUtil.getFileIDForFileGroup(
        ctx.getPartitionType(),
        ctx.getFileGroupIndex(),
        ctx.getPartitionType().getPartitionPath(),
        ctx.getDataPartitionName());
  }

  @Override
  public FileIdInfo parseFileId(MetadataPartitionType partitionType, String fileId) {
    int idx = HoodieTableMetadataUtil.getFileGroupIndexFromFileId(fileId);
    return new FileIdInfo(idx, Option.empty());
  }

  @Override
  public List<String> getPhysicalPartitions(String logicalPartition, int fileGroupCount) {
    return Collections.singletonList(logicalPartition);
  }

  @Override
  public List<String> getPartitionMarkerPaths(String logicalPartition, int fileGroupCount) {
    return Collections.singletonList(logicalPartition);
  }
}

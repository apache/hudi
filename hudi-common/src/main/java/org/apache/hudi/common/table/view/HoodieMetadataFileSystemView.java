/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.view;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.io.IOException;

/**
 * {@code HoodieTableFileSystemView} implementation that retrieved partition listings from the Metadata Table.
 */
public class HoodieMetadataFileSystemView extends HoodieTableFileSystemView {

  private final HoodieTableMetadata tableMetadata;

  public HoodieMetadataFileSystemView(HoodieTableMetaClient metaClient, HoodieTableMetadata tableMetadata,
                                      HoodieTimeline visibleActiveTimeline, boolean enableIncrementalTimelineSync) {
    super(metaClient, visibleActiveTimeline, enableIncrementalTimelineSync);
    this.tableMetadata = tableMetadata;
  }

  public HoodieMetadataFileSystemView(HoodieTableMetaClient metaClient,
                                      HoodieTimeline visibleActiveTimeline,
                                      boolean useFileListingFromMetadata,
                                      boolean verifyListings) {
    super(metaClient, visibleActiveTimeline);
    this.tableMetadata = HoodieTableMetadata.create(metaClient.getHadoopConf(), metaClient.getBasePath(),
            FileSystemViewStorageConfig.DEFAULT_VIEW_SPILLABLE_DIR, useFileListingFromMetadata, verifyListings,
            false, false);
  }

  /**
   * Return all the files in the partition by reading from the Metadata Table.
   *
   * @param partitionPath The absolute path of the partition
   * @throws IOException
   */
  @Override
  protected FileStatus[] listPartition(Path partitionPath) throws IOException {
    return tableMetadata.getAllFilesInPartition(partitionPath);
  }
}

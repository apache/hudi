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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@code HoodieTableFileSystemView} implementation that retrieved partition listings from the Metadata Table.
 */
public class HoodieMetadataFileSystemView extends HoodieTableFileSystemView {

  private final HoodieTableMetadata tableMetadata;

  public HoodieMetadataFileSystemView(HoodieTableMetaClient metaClient,
                                      HoodieTimeline visibleActiveTimeline,
                                      HoodieTableMetadata tableMetadata) {
    super(metaClient, visibleActiveTimeline);
    this.tableMetadata = tableMetadata;
  }

  public HoodieMetadataFileSystemView(HoodieEngineContext engineContext,
                                      HoodieTableMetaClient metaClient,
                                      HoodieTimeline visibleActiveTimeline,
                                      HoodieMetadataConfig metadataConfig) {
    this(metaClient, visibleActiveTimeline, HoodieTableMetadata.create(engineContext, metadataConfig, metaClient.getBasePath(), true));
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

  @Override
  protected List<String> getAllPartitionPaths() throws IOException {
    return tableMetadata.getAllPartitionPaths();
  }
  
  @Override
  protected Map<Pair<String, Path>, FileStatus[]> listPartitions(List<Pair<String, Path>> partitionPathList) throws IOException {
    Map<String, Pair<String, Path>> absoluteToPairMap = partitionPathList.stream()
        .collect(Collectors.toMap(
            pair -> pair.getRight().toString(),
            Function.identity()
        ));
    return tableMetadata.getAllFilesInPartitions(
            partitionPathList.stream().map(pair -> pair.getRight().toString()).collect(Collectors.toList()))
        .entrySet().stream().collect(Collectors.toMap(
            entry -> absoluteToPairMap.get(entry.getKey()),
            Map.Entry::getValue
        ));
  }

  @Override
  public void reset() {
    try {
      writeLock.lock();
      clear();
      // Initialize with new Hoodie timeline.
      init(metaClient, getTimeline());
      tableMetadata.reset();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void sync() {
    try {
      writeLock.lock();
      maySyncIncrementally();
      tableMetadata.reset();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void close() {
    try {
      tableMetadata.close();
    } catch (Exception e) {
      throw new HoodieException("Error closing metadata file system view.", e);
    }
  }
}

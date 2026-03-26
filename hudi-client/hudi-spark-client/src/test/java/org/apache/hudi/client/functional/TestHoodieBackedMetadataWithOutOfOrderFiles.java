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

package org.apache.hudi.client.functional;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TestFileSystemViewWithOutOfOrderFiles;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.stream.Stream;

/**
 * Test utility that extends {@link HoodieBackedTableMetadata} to return file slices
 * in reverse (out-of-order) sequence.
 * <p>
 * This is used to reproduce and test the RLI file slice ordering bug where
 * unsorted file slices cause incorrect record lookups due to hash-based
 * distribution assuming consistent ordering.
 * <p>
 * The class overrides {@link #getMetadataFileSystemView()} to use
 * {@link TestFileSystemViewWithOutOfOrderFiles} which returns file groups in reverse order.
 */
class TestHoodieBackedMetadataWithOutOfOrderFiles extends HoodieBackedTableMetadata {
  private HoodieTableMetaClient metadataMetaClient;
  private HoodieTableFileSystemView metadataFileSystemView;

  public TestHoodieBackedMetadataWithOutOfOrderFiles(HoodieEngineContext engineContext,
                                                     HoodieMetadataConfig metadataConfig,
                                                     String datasetBasePath) {
    super(engineContext, metadataConfig, datasetBasePath);
    this.metadataMetaClient = HoodieTableMetaClient.builder().setConf(getHadoopConf())
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(dataBasePath.toString())).build();
  }

  @Override
  public HoodieTableFileSystemView getMetadataFileSystemView() {
    if (metadataFileSystemView == null) {
      metadataFileSystemView = getFileSystemView(engineContext, metadataMetaClient);
    }
    return metadataFileSystemView;
  }

  @Override
  public void close() {
    super.close();
    if (metadataFileSystemView != null) {
      metadataFileSystemView.close();
      metadataFileSystemView = null;
    }
  }

  @Override
  public void reset() {
    super.reset();
    if (metadataFileSystemView != null) {
      metadataFileSystemView.close();
      metadataFileSystemView = null;
    }
  }

  private HoodieTableFileSystemView getFileSystemView(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient) {
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    if (timeline.empty()) {
      final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieActiveTimeline.createNewInstantTime());
      timeline = new HoodieDefaultTimeline(Stream.of(instant), metaClient.getActiveTimeline());
    }
    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(engineContext, metaClient.getTableConfig(),
        metaClient.getSerializableHadoopConf(), metaClient.getBasePathV2().toString(), false);
    return new TestFileSystemViewWithOutOfOrderFiles(tableMetadata, metaClient, timeline);
  }
}

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

package org.apache.hudi.common;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * The common hoodie test harness to provide the basic infrastructure.
 */
public class HoodieCommonTestHarness {

  protected String basePath = null;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected transient HoodieTableMetaClient metaClient;

  /**
   * Initializes basePath.
   */
  protected void initPath() {
    this.basePath = folder.getRoot().getAbsolutePath();
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initMetaClient() throws IOException {
    metaClient = HoodieTestUtils.init(folder.getRoot().getAbsolutePath(), getTableType());
    basePath = metaClient.getBasePath();
  }

  protected void refreshFsView() throws IOException {
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) throws IOException {
    return getFileSystemView(timeline, false);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline, boolean enableIncrementalTimelineSync) {
    return new HoodieTableFileSystemView(metaClient, timeline, enableIncrementalTimelineSync);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) throws IOException {
    return getFileSystemView(metaClient, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants());
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline timeline)
      throws IOException {
    return getFileSystemView(timeline, true);
  }

  /**
   * Gets a default {@link HoodieTableType#COPY_ON_WRITE} table type. Sub-classes can override this method to specify a
   * new table type.
   *
   * @return an instance of Hoodie table type.
   */
  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }
}

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

package org.apache.hudi.common.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Provides an API for reading metadata
 * 1) for latest commit.
 * 2) as of given commit.
 * 
 * This only includes base files (parquet) today. But can be extended to return other metadata information.
 */
public class HoodieSnapshotMetadataClient {

  private static final Logger LOG = LogManager.getLogger(HoodieSnapshotMetadataClient.class);

  private HoodieTableMetaClient metaClient;

  /**
   * Constructor to create metadata client.
   * 
   * @param conf hadoop configuration bag.
   * @param basePath Table location absolute path.
   */
  public HoodieSnapshotMetadataClient(Configuration conf, String basePath) throws IOException {
    this(HoodieTableMetaClient.builder().setBasePath(basePath).setConf(conf).build());
  }

  /**
   * Create HoodieIncrementalMetadataClient from HoodieTableMetaClient.
   */
  public HoodieSnapshotMetadataClient(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  /**
   * Returns the latest set of data files that represent rows in table. Note that this only returns 'base' files and 
   * not log files.
   */
  public Stream<Path> getLatestSnapshotFiles(String partitionPath) {
    HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    return fileSystemView.getLatestBaseFiles(partitionPath).map(bf -> new Path(bf.getPath()));
  }

  /**
   * Returns set of data files that represent rows in table as of specified instant. 
   * Note that this only returns 'base' files and not log files. 
   * Also, note that this only works if corresponding base files are not removed by Cleaner.
   */
  public Stream<Path> getSnapshotFilesAt(String instant, String partitionPath) {
    HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    return fileSystemView.getLatestBaseFilesBeforeOrOn(partitionPath, instant).map(bf -> new Path(bf.getPath()));
  }

  /**
   * Returns the latest commit instant time on hoodie table.
   */
  public Option<String> getLatestInstant() {
    return this.metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::getTimestamp);
  }

  /**
   * reload active timeline to read new commits (if any).
   */
  public void reload() {
    this.metaClient.reloadActiveTimeline();
  }

  /**
   * Getter for metaClient.
   */
  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }
}

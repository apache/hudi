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

package org.apache.hudi.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieIncrementalMetadataClient;
import org.apache.hudi.common.table.HoodieSnapshotMetadataClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;

/**
 * Provides API for reading Hoodie metadata. This is useful for clients that need access to snapshot 
 * as well as incremental APIs. Both snapshot and incremental use same meta client, keeping timeline in sync.
 */
public class HoodieMetadataReadClient {

  private HoodieSnapshotMetadataClient snapshotMetadataClient;
  private HoodieIncrementalMetadataClient incrementalMetadataClient;
  
  /**
   * @param basePath path to Hoodie table.
   */
  public HoodieMetadataReadClient(Configuration conf, String basePath) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf, basePath);
    this.snapshotMetadataClient = new HoodieSnapshotMetadataClient(metaClient);
    this.incrementalMetadataClient = new HoodieIncrementalMetadataClient(metaClient);
  }

  /**
   * Refresh timeline to see if there are new commits/instants in timeline.
   */
  public void reload() {
    // same metaclient is shared by snapshot/incremental read clients. so refreshing one of them is sufficient.
    this.snapshotMetadataClient.reload();
  }

  /**
   * Getters for snapshot metadata client.
   */
  public HoodieSnapshotMetadataClient getSnapshotMetadataClient() {
    return snapshotMetadataClient;
  }

  /**
   * Getters for incremental metadata client.
   */
  public HoodieIncrementalMetadataClient getIncrementalMetadataClient() {
    return incrementalMetadataClient;
  }

}

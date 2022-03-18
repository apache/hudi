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

package org.apache.hudi.common.table.timeline;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetaServerConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.client.HoodieMetaServerClient;
import org.apache.hudi.metaserver.client.RetryingHoodieMetaServerClient;

/**
 * Active timeline for hoodie table whose metadata is stored in the hoodie meta server instead of file system.
 */
public class HoodieMetaServerBasedTimeline extends HoodieActiveTimeline {
  private String databaseName;
  private String tableName;

  private HoodieMetaServerClient metaServerClient;
  
  public HoodieMetaServerBasedTimeline(HoodieTableMetaClient metaClient, HoodieMetaServerConfig config) {
    this.metaClient = metaClient;
    this.metaServerClient = RetryingHoodieMetaServerClient.getProxy(config);
    this.databaseName = config.getString(HoodieTableConfig.DATABASE_NAME.key());
    this.tableName = config.getString(HoodieTableConfig.NAME.key());
    this.setInstants(metaServerClient.listInstants(databaseName, tableName, 24));
  }

  protected void deleteInstantFile(HoodieInstant instant) {
    metaServerClient.deleteInstant(databaseName, tableName, instant);
  }

  public void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data, boolean allowRedundantTransitions) {
    ValidationUtils.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    metaServerClient.transitionInstantState(databaseName, tableName, fromInstant, toInstant, data);
  }

  public void createFileInMetaPath(String filename, Option<byte[]> content, boolean allowOverwrite) {
    FileStatus status = new FileStatus();
    status.setPath(new Path(filename));
    HoodieInstant instant = new HoodieInstant(status);
    ValidationUtils.checkArgument(instant.getState().equals(HoodieInstant.State.REQUESTED));
    metaServerClient.createNewInstant(databaseName, tableName, instant, Option.empty());
  }

  protected void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    throw new HoodieException("Unsupported now");
  }

  public Option<byte[]> readDataFromPath(Path detailPath) {
    FileStatus status = new FileStatus();
    status.setPath(detailPath);
    HoodieInstant instant = new HoodieInstant(status);
    return metaServerClient.getInstantMeta(databaseName, tableName, instant);
  }

  public HoodieMetaServerBasedTimeline reload() {
    return new HoodieMetaServerBasedTimeline(metaClient, metaClient.getMetaServerConfig());
  }
}

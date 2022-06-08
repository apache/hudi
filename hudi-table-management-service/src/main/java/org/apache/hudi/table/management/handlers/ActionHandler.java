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

package org.apache.hudi.table.management.handlers;

import org.apache.hudi.table.management.common.ServiceConfig;
import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.store.MetadataStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ActionHandler implements AutoCloseable {
  private static Logger LOG = LoggerFactory.getLogger(ActionHandler.class);

  protected final Configuration conf;
  protected final FileSystem fileSystem;
  protected final MetadataStore metadataStore;

  private final CompactionHandler compactionHandler;

  public ActionHandler(Configuration conf,
                       MetadataStore metadataStore) throws IOException {
    this.conf = conf;
    this.fileSystem = FileSystem.get(conf);
    this.metadataStore = metadataStore;
    boolean cacheEnable = ServiceConfig.getInstance().getBool(ServiceConfig.ServiceConfVars.CompactionCacheEnable);
    this.compactionHandler = new CompactionHandler(cacheEnable);
  }

  public void scheduleCompaction(Instance instance) {
    compactionHandler.scheduleCompaction(metadataStore, instance);
  }

  public void removeCompaction(Instance instance) throws IOException {
    compactionHandler.removeCompaction(metadataStore, instance);
  }

  // TODO: support clustering
  public void scheduleClustering(Instance instance) {

  }

  public void removeClustering(Instance instance) {

  }

  @Override
  public void close() throws Exception {
    this.fileSystem.close();
  }
}

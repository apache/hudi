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

package org.apache.hudi.tableformat;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.metadata.TableMetadataFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Test implementation of HoodieTableFormat that records all Hoodie instants in memory.
 * Used for functional testing of HoodieTableFormat.
 */
public class TestTableFormat implements HoodieTableFormat {

  private static final Map<String, List<HoodieInstant>> RECORDED_INSTANTS = new ConcurrentHashMap<>();
  
  public TestTableFormat() {
  }

  public static List<HoodieInstant> getRecordedInstants(String basePath) {
    return RECORDED_INSTANTS.getOrDefault(basePath, Collections.emptyList());
  }

  public static void tearDown() {
    RECORDED_INSTANTS.clear();
  }

  @Override
  public String getName() {
    return "test-format";
  }

  @Override
  public void commit(HoodieCommitMetadata commitMetadata, HoodieInstant completedInstant, 
                    HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, 
                    FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.putIfAbsent(metaClient.getBasePath().toString(), new CopyOnWriteArrayList<>());
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(completedInstant);
  }

  @Override
  public void clean(HoodieCleanMetadata cleanMetadata, HoodieInstant completedInstant, 
                   HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, 
                   FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(completedInstant);
  }

  @Override
  public void archive(Supplier<List<HoodieInstant>> archivedInstants, HoodieEngineContext engineContext,
                      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).removeAll(archivedInstants.get());
  }

  @Override
  public void rollback(HoodieInstant completedInstant, HoodieEngineContext engineContext, 
                      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    // No-op.
  }

  @Override
  public void completedRollback(HoodieInstant rollbackInstant, HoodieEngineContext engineContext, 
                               HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.putIfAbsent(metaClient.getBasePath().toString(), new CopyOnWriteArrayList<>());
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(rollbackInstant);
  }

  @Override
  public void savepoint(HoodieInstant savepointInstant, HoodieEngineContext engineContext, 
                       HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(savepointInstant);
  }

  @Override
  public void restore(HoodieInstant restoreCompletedInstant, HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(restoreCompletedInstant);
  }

  @Override
  public TimelineFactory getTimelineFactory() {
    return new TestTimelineFactory(null);
  }

  @Override
  public TableMetadataFactory getMetadataFactory() {
    return TestTableMetadataFactory.getInstance();
  }
}

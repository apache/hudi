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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;

import java.io.IOException;
import java.util.List;

/**
 * Interface to building records for initializing and updating a type of metadata or index
 * in the metadata table.
 * <p>
 * When a new type of index is added to MetadataPartitionType, an
 * implementation of the {@link Indexer} interface is required, and it
 * must be added to {@link IndexerFactory}.
 */
public interface Indexer {
  /**
   * Generates records for initializing the index.
   *
   * @param context shared metadata indexing context
   * @return zero or more {@link IndexInitializationPlan} entries to be initialized.
   * Returning an empty list means no metadata partition needs initialization in this invocation.
   * @throws IOException upon IO error
   */
  List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException;

  /**
   * Hook invoked after the bootstrap bulk commit for an index partition succeeds.
   * Implementations can use this to perform index-specific follow-up work.
   *
   * @param metadataMetaClient     metadata table meta client used during initialization
   * @param records                records committed during index partition initialization
   * @param fileGroupCount         number of file groups created for the index partition
   * @param relativePartitionPath  metadata table relative partition path being initialized
   */
  void postInitialization(
      HoodieTableMetaClient metadataMetaClient,
      HoodieData<HoodieRecord> records,
      int fileGroupCount,
      String relativePartitionPath);
}

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

import org.apache.hudi.client.clustering.run.strategy.SingleSparkJobExecutionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.execution.SparkLazyInsertIterable;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.SingleFileHandleCreateFactory;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusteringIdentityTestExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends SingleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringIdentityTestExecutionStrategy.class);

  public ClusteringIdentityTestExecutionStrategy(HoodieTable table,
                                                 HoodieEngineContext engineContext,
                                                 HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected List<WriteStatus> performClusteringForGroup(ReaderContextFactory<T> readerContextFactory,
                                                        ClusteringGroupInfo clusteringGroup,
                                                        Map<String, String> strategyParams,
                                                        boolean preserveHoodieMetadata,
                                                        HoodieSchema schema,
                                                        TaskContextSupplier taskContextSupplier,
                                                        String instantTime) {
    if (clusteringGroup.getOperations().size() != 1) {
      throw new HoodieClusteringException("Expect only one partition and one fileId in clustering group for identity strategy: " + getClass().getName());
    }

    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, getWriteConfig());
    String fileId = clusteringGroup.getOperations().get(0).getFileId();
    try (ClosableIterator recordItr =
             getRecordIterator(readerContextFactory, clusteringGroup.getOperations().get(0), instantTime, maxMemoryPerCompaction)) {
      SparkLazyInsertIterable<T> insertIterable =
          new SparkLazyInsertIterable<>(recordItr, true, getWriteConfig(), instantTime, getHoodieTable(),
              fileId, taskContextSupplier, new SingleFileHandleCreateFactory(fileId, preserveHoodieMetadata));
      return insertIterable.hasNext() ? insertIterable.next() : Collections.emptyList();
    }
  }
}

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

import org.apache.avro.Schema;
import org.apache.hudi.client.clustering.run.strategy.SingleSparkJobExecutionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.execution.SparkLazyInsertIterable;
import org.apache.hudi.io.SingleFileHandleCreateFactory;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
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
  public Iterator<List<WriteStatus>> performClusteringWithRecordsIterator(final Iterator<HoodieRecord<T>> recordItr, final int numOutputGroups,
                                                                          final String instantTime,
                                                                          final Map<String, String> strategyParams, final Schema schema,
                                                                          final List<HoodieFileGroupId> inputFileIds, final boolean preserveHoodieMetadata,
                                                                          final TaskContextSupplier taskContextSupplier) {
    if (inputFileIds.size() != 1) {
      throw new HoodieClusteringException("Expect only one partition and one fileId in clustering group for identity strategy: " + getClass().getName());
    }

    String fileId = inputFileIds.get(0).getFileId();
    return new SparkLazyInsertIterable(recordItr, true, getWriteConfig(), instantTime, getHoodieTable(),
        fileId, taskContextSupplier, new SingleFileHandleCreateFactory(fileId, preserveHoodieMetadata));
  }
}

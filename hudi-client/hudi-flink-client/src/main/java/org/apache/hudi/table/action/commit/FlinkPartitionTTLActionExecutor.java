/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieDeletePartitionPendingTableServiceException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.ttl.strategy.HoodiePartitionTTLStrategyFactory;
import org.apache.hudi.table.action.ttl.strategy.PartitionTTLStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class FlinkPartitionTTLActionExecutor<T> extends BaseFlinkCommitActionExecutor<T> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkPartitionTTLActionExecutor.class);

  public FlinkPartitionTTLActionExecutor(HoodieEngineContext context,
                                         HoodieWriteConfig config,
                                         HoodieTable table,
                                         String instantTime) {
    super(context, null, config, table, instantTime, WriteOperationType.DELETE_PARTITION);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    HoodieWriteMetadata<List<WriteStatus>> emptyResult = new HoodieWriteMetadata<>();
    emptyResult.setPartitionToReplaceFileIds(Collections.emptyMap());
    emptyResult.setWriteStatuses(Collections.emptyList());
    try {
      PartitionTTLStrategy strategy = HoodiePartitionTTLStrategyFactory.createStrategy(table, config.getProps(), instantTime);
      List<String> expiredPartitions = strategy.getExpiredPartitionPaths();
      if (expiredPartitions.isEmpty()) {
        return emptyResult;
      }
      LOG.info("Partition ttl find the following expired partitions to delete:  " + String.join(",", expiredPartitions));
      // Auto commit is disabled in config, copy config and enable auto commit for FlinkDeletePartitionCommitActionExecutor.
      HoodieWriteConfig autoCommitConfig = HoodieWriteConfig.newBuilder().withProperties(config.getProps()).withAutoCommit(true).build();
      return new FlinkDeletePartitionCommitActionExecutor<>(context, autoCommitConfig, table, instantTime, expiredPartitions).execute();
    } catch (HoodieDeletePartitionPendingTableServiceException deletePartitionPendingTableServiceException) {
      LOG.info("Partition is under table service, do nothing, call delete partition next time.");
      return emptyResult;
    } catch (IOException e) {
      throw new HoodieIOException("Error executing hoodie partition ttl: ", e);
    }
  }
}

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

package org.apache.hudi.table.action.compact.strategy;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * CompactionStrategy which looks at total IO to be done for the compaction (read + write) and limits the list of
 * compactions to be under a configured limit on the IO.
 *
 * @see CompactionStrategy
 */
public class BoundedIOCompactionStrategy extends CompactionStrategy {

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    // Iterate through the operations in order and accept operations as long as we are within the
    // IO limit
    // Preserves the original ordering of compactions
    List<HoodieCompactionOperation> finalOperations = new ArrayList<>();
    long targetIORemaining = writeConfig.getLong(HoodieCompactionConfig.TARGET_IO_PER_COMPACTION_IN_MB);
    for (HoodieCompactionOperation op : operations) {
      long opIo = op.getMetrics().get(TOTAL_IO_MB).longValue();
      targetIORemaining -= opIo;
      finalOperations.add(op);
      if (targetIORemaining <= 0) {
        return finalOperations;
      }
    }
    return finalOperations;
  }
}

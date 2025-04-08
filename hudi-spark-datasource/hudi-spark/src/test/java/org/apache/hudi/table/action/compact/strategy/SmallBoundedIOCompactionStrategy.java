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

package org.apache.hudi.table.action.compact.strategy;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SmallBoundedIOCompactionStrategy extends BoundedIOCompactionStrategy {

  // Compare to BoundedIOCompactionStrategy, SmallBoundedIOCompactionStrategy is limit targetIO to 0.1M
  private final double targetIO = 0.1;
  private final long opIo = 1;
  public SmallBoundedIOCompactionStrategy() {
    super();
  }

  @Override
  public Pair<List<HoodieCompactionOperation>, List<String>> orderAndFilter(HoodieWriteConfig writeConfig,
                                                                            List<HoodieCompactionOperation> operations,
                                                                            List<HoodieCompactionPlan> pendingCompactionPlans) {
    ArrayList<String> missingPartitions = new ArrayList<>();
    List<HoodieCompactionOperation> finalOperations = new ArrayList<>();
    double targetIORemaining = targetIO;
    for (HoodieCompactionOperation op : operations) {
      if (targetIORemaining > 0) {
        targetIORemaining -= opIo;
        finalOperations.add(op);
      } else if (writeConfig.isIncrementalTableServiceEnabled()) {
        missingPartitions.add(op.getPartitionPath());
      } else  {
        return Pair.of(finalOperations, Collections.emptyList());
      }
    }
    return Pair.of(finalOperations, missingPartitions);
  }
}

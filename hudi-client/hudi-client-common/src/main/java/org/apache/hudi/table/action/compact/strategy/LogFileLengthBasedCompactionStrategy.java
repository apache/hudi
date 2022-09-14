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
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LogFileLengthBasedCompactionStrategy orders the compactions based on the total log files length,
 * filters the file group which log files length is greater than the threshold and limits the compactions within a configured IO bound.
 */
public class LogFileLengthBasedCompactionStrategy extends BoundedIOCompactionStrategy
    implements Comparator<HoodieCompactionOperation> {

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig, List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    Long lengthThreshold = writeConfig.getCompactionLogFileLengthThreshold();
    List<HoodieCompactionOperation> filterOperator = operations.stream()
        .filter(e -> e.getDeltaFilePaths().size() >= lengthThreshold)
        .sorted(this).collect(Collectors.toList());
    return super.orderAndFilter(writeConfig, filterOperator, pendingCompactionPlans);
  }

  @Override
  public int compare(HoodieCompactionOperation hco1, HoodieCompactionOperation hco2) {
    return hco2.getDeltaFilePaths().size() - hco1.getDeltaFilePaths().size();
  }
}

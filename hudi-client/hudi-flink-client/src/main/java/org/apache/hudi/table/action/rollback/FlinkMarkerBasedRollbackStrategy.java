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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.MarkerFiles;

import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

@SuppressWarnings("checkstyle:LineLength")
public class FlinkMarkerBasedRollbackStrategy<T extends HoodieRecordPayload> extends AbstractMarkerBasedRollbackStrategy<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  public FlinkMarkerBasedRollbackStrategy(HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table, HoodieEngineContext context, HoodieWriteConfig config, String instantTime) {
    super(table, context, config, instantTime);
  }

  @Override
  public List<HoodieRollbackStat> execute(HoodieInstant instantToRollback) {
    try {
      MarkerFiles markerFiles = new MarkerFiles(table, instantToRollback.getTimestamp());
      List<HoodieRollbackStat> rollbackStats = context.map(markerFiles.allMarkerFilePaths(), markerFilePath -> {
        String typeStr = markerFilePath.substring(markerFilePath.lastIndexOf(".") + 1);
        IOType type = IOType.valueOf(typeStr);
        switch (type) {
          case MERGE:
            return undoMerge(MarkerFiles.stripMarkerSuffix(markerFilePath));
          case APPEND:
            return undoAppend(MarkerFiles.stripMarkerSuffix(markerFilePath), instantToRollback);
          case CREATE:
            return undoCreate(MarkerFiles.stripMarkerSuffix(markerFilePath));
          default:
            throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
        }
      }, 0);

      return rollbackStats.stream().map(rollbackStat -> new Tuple2<>(rollbackStat.getPartitionPath(), rollbackStat))
          .collect(Collectors.groupingBy(Tuple2::_1))
          .values()
          .stream()
          .map(x -> x.stream().map(y -> y._2).reduce(RollbackUtils::mergeRollbackStat).get())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }
}

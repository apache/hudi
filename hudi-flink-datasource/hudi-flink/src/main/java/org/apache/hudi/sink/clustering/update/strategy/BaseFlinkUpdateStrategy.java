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

package org.apache.hudi.sink.clustering.update.strategy;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.table.action.cluster.update.strategy.UpdateStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseFlinkUpdateStrategy<T extends HoodieRecordPayload> extends UpdateStrategy<T, List<BaseFlinkUpdateStrategy.RecordsInstantPair>> {

  public static class RecordsInstantPair {
    public List<HoodieRecord> records;
    public String instantTime;

    public static RecordsInstantPair of(List<HoodieRecord> records, String instant) {
      RecordsInstantPair ret = new RecordsInstantPair();
      ret.records = records;
      ret.instantTime = instant;
      return ret;
    }
  }

  private boolean initialized = false;

  public BaseFlinkUpdateStrategy(HoodieEngineContext engineContext) {
    super(engineContext, null, Collections.emptySet());
  }

  public void initialize(HoodieFlinkWriteClient writeClient) {
    if (initialized) {
      return;
    }

    this.table = writeClient.getHoodieTable();
    this.fileGroupsInPendingClustering = writeClient.getHoodieTable().getFileSystemView().getFileGroupsInPendingClustering()
        .map(Pair::getKey).collect(Collectors.toSet());
    this.initialized = true;
  }

  public void reset() {
    initialized = false;
  }

  @Override
  public Pair<List<RecordsInstantPair>, List<HoodieFileGroupId>> handleUpdate(HoodieFileGroupId fileId, List<RecordsInstantPair> recordList) {
    ValidationUtils.checkArgument(initialized, "Strategy has not been initialized");
    ValidationUtils.checkArgument(recordList.size() == 1);

    RecordsInstantPair recordsInstantPair = recordList.get(0);
    // Return the input records directly if the corresponding file group is not under clustering.
    if (fileGroupsInPendingClustering.isEmpty() || !fileGroupsInPendingClustering.contains(fileId)) {
      return Pair.of(Collections.singletonList(recordsInstantPair), Collections.singletonList(fileId));
    }

    return doHandleUpdate(fileId, recordsInstantPair);
  }

  @Override
  public Pair<List<RecordsInstantPair>, Set<HoodieFileGroupId>> handleUpdate(List<RecordsInstantPair> recordList) {
    throw new UnsupportedOperationException("handleUpdate(String, List<HoodieRecord>) should be used instead");
  }

  /**
   * Do the actual work of the update handling. The subclass can assume the current records batch are writing to a file group
   * that is under clustering.
   * @param fileId    file group writing to
   * @param recordsInstantPair records to write and the default instant
   * @return
   *   - pair.left:  the batch records and their corresponding instant time
   *   - pair.right: set of file group id that the records need to write to
   */
  protected abstract Pair<List<RecordsInstantPair>, List<HoodieFileGroupId>> doHandleUpdate(HoodieFileGroupId fileId, RecordsInstantPair recordsInstantPair);
}

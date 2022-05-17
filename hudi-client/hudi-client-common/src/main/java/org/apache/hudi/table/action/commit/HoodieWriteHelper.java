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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordCombiningEngine;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

public class HoodieWriteHelper<T, R> extends BaseWriteHelper<T, HoodieData<HoodieRecord<T>>,
    HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {
  private HoodieWriteHelper() {
  }

  private static class WriteHelperHolder {
    private static final HoodieWriteHelper HOODIE_WRITE_HELPER = new HoodieWriteHelper<>();
  }

  public static HoodieWriteHelper newInstance() {
    return WriteHelperHolder.HOODIE_WRITE_HELPER;
  }

  @Override
  protected HoodieData<HoodieRecord<T>> tag(HoodieData<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context,
                                            HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table) {
    return table.getIndex().tagLocation(dedupedRecords, context, table);
  }

  @Override
  public HoodieData<HoodieRecord<T>> deduplicateRecords(
      HoodieData<HoodieRecord<T>> records, HoodieIndex<?, ?> index, int parallelism, HoodieRecordCombiningEngine combiningEngine) {
    boolean isIndexingGlobal = index.isGlobal();
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global,x then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return Pair.of(key, record);
    }).reduceByKey((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      HoodieRecord reducedRecord =  combiningEngine.preCombine(rec1, rec2);
      HoodieKey reducedKey = rec1.getData().equals(reducedRecord) ? rec1.getKey() : rec2.getKey();

      return (HoodieRecord<T>) reducedRecord.newInstance(reducedKey);
    }, parallelism).map(Pair::getRight);
  }

}

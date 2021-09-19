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

import org.apache.hudi.SparkHoodieRDDData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import static org.apache.hudi.table.action.commit.SparkCommitHelper.getRdd;

/**
 * A spark implementation of {@link BaseWriteHelper}.
 *
 * @param <T>
 */
public class SparkWriteHelper<T extends HoodieRecordPayload<T>> extends BaseWriteHelper<T> {
  private SparkWriteHelper() {
  }

  private static class WriteHelperHolder {
    private static final SparkWriteHelper SPARK_WRITE_HELPER = new SparkWriteHelper();
  }

  public static SparkWriteHelper newInstance() {
    return WriteHelperHolder.SPARK_WRITE_HELPER;
  }

  @Override
  protected HoodieData<HoodieRecord<T>> tag(
      HoodieData<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context, HoodieTable table) {
    // perform index loop up to get existing location of records
    return SparkHoodieRDDData.of(
        (JavaRDD<HoodieRecord<T>>) table.getIndex().tagLocation(getRdd(dedupedRecords), context, table));
  }

  @Override
  public HoodieData<HoodieRecord<T>> deduplicateRecords(HoodieData<HoodieRecord<T>> records,
                                                        HoodieIndex index,
                                                        int parallelism) {
    boolean isIndexingGlobal = index.isGlobal();
    JavaRDD<HoodieRecord<T>> recordsRdd = getRdd(records);
    return SparkHoodieRDDData.of(recordsRdd.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return new Tuple2<>(key, record);
    }).reduceByKey((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      T reducedData = (T) rec2.getData().preCombine(rec1.getData());
      HoodieKey reducedKey = rec1.getData().equals(reducedData) ? rec1.getKey() : rec2.getKey();

      return new HoodieRecord<T>(reducedKey, reducedData);
    }, parallelism).map(Tuple2::_2));
  }

}

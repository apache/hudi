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
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.data.HoodieJavaDataFrame;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.sql.Dataset;

import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;

public class HoodieDataFrameWriteHelper<T, R> extends BaseWriteHelper<T, HoodieData<HoodieRecord<T>>,
    HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {

  private HoodieDataFrameWriteHelper() {
    super(HoodieData::getNumPartitions);
  }

  @Override
  protected HoodieData<HoodieRecord<T>> tag(HoodieData<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context,
                                            HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>,
                                                    HoodieData<WriteStatus>> table) {
    return table.getIndex().tagLocation(dedupedRecords, context, table);
  }

  @Override
  public HoodieData<HoodieRecord<T>> deduplicateRecords(HoodieData<HoodieRecord<T>> records, HoodieIndex<?, ?> index,
                                                     int parallelism, String schemaStr, TypedProperties props,
                                                     HoodieRecordMerger merger) {
    boolean isIndexingGlobal = index.isGlobal();
    final SerializableSchema schema = new SerializableSchema(schemaStr);
    return deduplicateRecords((HoodieJavaDataFrame<HoodieRecord<T>>) records, isIndexingGlobal);
  }

  public HoodieData<HoodieRecord<T>> deduplicateRecords(HoodieJavaDataFrame<HoodieRecord<T>> records,
                                                        boolean isGlobalIndex) {
    Dataset<HoodieRecord<T>> df = HoodieJavaDataFrame.getDataFrame(records);

    if (isGlobalIndex) {
      return HoodieJavaDataFrame.of(df.dropDuplicates(RECORD_KEY_METADATA_FIELD));
    } else {
      return HoodieJavaDataFrame.of(df.dropDuplicates(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD));
    }
  }

  private static class WriteHelperHolder {
    private static final HoodieDataFrameWriteHelper HOODIE_WRITE_HELPER = new HoodieDataFrameWriteHelper<>();
  }

  public static HoodieDataFrameWriteHelper newInstance() {
    return WriteHelperHolder.HOODIE_WRITE_HELPER;
  }

}

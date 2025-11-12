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
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

public class HoodieWriteHelper<T, R> extends BaseWriteHelper<T, HoodieData<HoodieRecord<T>>,
    HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {

  private HoodieWriteHelper() {
    super(HoodieData::deduceNumPartitions);
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
  public HoodieData<HoodieRecord<T>> deduplicateRecords(HoodieData<HoodieRecord<T>> records,
                                                        HoodieIndex<?, ?> index,
                                                        int parallelism,
                                                        String schemaStr,
                                                        TypedProperties props,
                                                        BufferedRecordMerger<T> recordMerger,
                                                        HoodieReaderContext<T> readerContext,
                                                        String[] orderingFieldNames) {
    boolean isIndexingGlobal = index.isGlobal();
    final SerializableSchema schema = new SerializableSchema(schemaStr);
    RecordContext<T> recordContext = readerContext.getRecordContext();
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, schema.get());
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
      //       Here we have to make a copy of the incoming record, since it might be holding
      //       an instance of [[InternalRow]] pointing into shared, mutable buffer
      return Pair.of(key, record.copy());
    }).reduceByKey(
        (previous, next) -> reduceRecords(props, recordMerger, orderingFieldNames, previous, next, schema.get(), recordContext, deleteContext),
        parallelism).map(Pair::getRight);
  }
}

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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class JavaWriteHelper<T,R> extends BaseWriteHelper<T, List<HoodieRecord<T>>,
    List<HoodieKey>, List<WriteStatus>, R> {

  private JavaWriteHelper() {
    super(ignored -> -1);
  }

  private static class WriteHelperHolder {
    private static final JavaWriteHelper JAVA_WRITE_HELPER = new JavaWriteHelper();
  }

  public static JavaWriteHelper newInstance() {
    return WriteHelperHolder.JAVA_WRITE_HELPER;
  }

  @Override
  protected List<HoodieRecord<T>> tag(List<HoodieRecord<T>> dedupedRecords, HoodieEngineContext context, HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table) {
    return table.getIndex().tagLocation(HoodieListData.eager(dedupedRecords), context, table).collectAsList();
  }

  @Override
  public List<HoodieRecord<T>> deduplicateRecords(List<HoodieRecord<T>> records,
                                                  HoodieIndex<?, ?> index,
                                                  int parallelism,
                                                  String schemaStr,
                                                  TypedProperties props,
                                                  BufferedRecordMerger<T> recordMerger,
                                                  HoodieReaderContext<T> readerContext,
                                                  String[] orderingFieldNames) {
    boolean isIndexingGlobal = index.isGlobal();
    Map<Object, List<Pair<Object, HoodieRecord<T>>>> keyedRecords = records.stream().map(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return Pair.of(key, record);
    }).collect(Collectors.groupingBy(Pair::getLeft));

    final Schema schema = new Schema.Parser().parse(schemaStr);
    return keyedRecords.values().stream().map(x -> x.stream().map(Pair::getRight).reduce((rec1, rec2) -> {
      HoodieRecord<T> reducedRecord;
      try {
        Option<BufferedRecord<T>> merged = merge(
            rec2, rec1, schema, schema, readerContext.getRecordContext(), orderingFieldNames, recordMerger, props);
        reducedRecord = merged.map(bufferedRecord -> readerContext.getRecordContext().constructHoodieRecord(bufferedRecord)).orElse(rec1);
      } catch (IOException e) {
        throw new HoodieException(String.format("Error to merge two records, %s, %s", rec1, rec2), e);
      }
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      return reducedRecord.newInstance(rec1.getKey(), rec1.getOperation());
    }).orElse(null)).filter(Objects::nonNull).collect(Collectors.toList());
  }
}

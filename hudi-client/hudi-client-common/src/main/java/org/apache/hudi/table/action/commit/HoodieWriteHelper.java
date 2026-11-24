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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
  protected HoodieData<HoodieRecord<T>> mergeLocatedSqlMergeIntoRecords(HoodieData<HoodieRecord<T>> taggedRecords,
                                                                        String instantTime,
                                                                        HoodieEngineContext context,
                                                                        HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
                                                                        int configuredShuffleParallelism) {
    // The records are read more than once, and are tagged and may be computed from the source of the statement each time
    taggedRecords.persist(table.getConfig().getTaggedRecordStorageLevel(), context,
        HoodieDataCacheKey.of(table.getConfig().getBasePath(), instantTime));
    // A key can still be located through a deleted copy in a base file. The record then keeps its location, as an
    // insert into another file group would leave the key in two of them
    return HoodieIndexUtils.mergeLocatedExpressionPayloadRecords(
        taggedRecords, table.getConfig(), table, deduceShuffleParallelism(taggedRecords, configuredShuffleParallelism));
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
    final HoodieSchema schema = HoodieSchema.parse(schemaStr);
    RecordContext<T> recordContext = readerContext.getRecordContext();
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, schema);
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
      //       Here we have to make a copy of the incoming record, since it might be holding
      //       an instance of [[InternalRow]] pointing into shared, mutable buffer
      return Pair.of(key, record.copy());
    }).reduceByKey(
        (previous, next) -> reduceRecords(props, recordMerger, orderingFieldNames, previous, next, schema, recordContext, deleteContext),
        parallelism).map(Pair::getRight);
  }

  @Override
  protected HoodieData<HoodieRecord<T>> failOnMultipleMatches(HoodieData<HoodieRecord<T>> taggedRecords, boolean combineNewKeys,
                                                              HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table) {
    boolean isIndexingGlobal = table.getIndex().isGlobal();
    Option<SerializableBiFunction<HoodieRecord<T>, HoodieRecord<T>, HoodieRecord<T>>> newKeyCombiner =
        combineNewKeys ? Option.of(recordCombiner(table)) : Option.empty();
    HoodieSchema writeSchema = HoodieSchema.parse(table.getConfig().getWriteSchema());
    TypedProperties props = table.getConfig().getProps();
    return taggedRecords
        // NOTE: the incoming record may hold an instance of [[InternalRow]] pointing into a
        //       shared, mutable buffer, so it has to be copied before the shuffle
        .mapToPair(record -> Pair.of(isIndexingGlobal ? record.getRecordKey() : record.getKey(), record.copy()))
        .groupByKey()
        .values()
        .flatMap(records -> failOnMultipleMatches(records, newKeyCombiner, writeSchema, props).iterator());
  }

  /**
   * Fails when two of the records with one key are located at the same row, and combines the ones that are not
   * located with the given combiner, if any. One record can be located at more than one row, as a table can hold
   * rows with the same key in one file group or across file groups. Before combining, drops the records that the
   * write of an insert ignores, as a Spark SQL MERGE INTO record that no WHEN NOT MATCHED clause accepts, so that
   * such a record does not take the place of one that a clause accepts.
   */
  private static <T> List<HoodieRecord<T>> failOnMultipleMatches(
      Iterable<HoodieRecord<T>> records, Option<SerializableBiFunction<HoodieRecord<T>, HoodieRecord<T>, HoodieRecord<T>>> newKeyCombiner,
      HoodieSchema writeSchema, TypedProperties props) {
    List<HoodieRecord<T>> result = new ArrayList<>();
    Set<Pair<String, Long>> matchedRows = new HashSet<>();
    List<HoodieRecord<T>> newKeyRecords = new ArrayList<>();
    for (HoodieRecord<T> record : records) {
      if (record.isCurrentLocationKnown()) {
        HoodieRecordLocation location = record.getCurrentLocation();
        if (!matchedRows.add(Pair.of(location.getFileId(), location.getPosition()))) {
          throw multipleMatchesException(record.getRecordKey(), record.getPartitionPath(), location.getFileId());
        }
        result.add(record);
      } else if (newKeyCombiner.isPresent()) {
        newKeyRecords.add(record);
      } else {
        result.add(record);
      }
    }
    if (newKeyRecords.size() > 1) {
      newKeyRecords.removeIf(record -> shouldIgnore(record, writeSchema, props));
    }
    if (!newKeyRecords.isEmpty()) {
      result.add(newKeyRecords.stream().reduce(newKeyCombiner.get()::apply).get());
    }
    return result;
  }

  private static <T> boolean shouldIgnore(HoodieRecord<T> record, HoodieSchema writeSchema, TypedProperties props) {
    try {
      return record.shouldIgnore(writeSchema, props);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to evaluate the record with key " + record.getRecordKey(), e);
    }
  }
}

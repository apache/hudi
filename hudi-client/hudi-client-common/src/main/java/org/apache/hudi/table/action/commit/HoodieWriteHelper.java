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
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

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
  protected HoodieData<HoodieRecord<T>> updatesAsDeletesAndInserts(HoodieData<HoodieRecord<T>> taggedRecords,
                                                                   HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
                                                                   int configuredShuffleParallelism) {
    HoodieWriteConfig config = table.getConfig();
    TypedProperties props = config.getProps();
    final HoodieSchema schema = HoodieSchema.parse(config.getSchema());
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, schema);
    int parallelism = deduceShuffleParallelism(taggedRecords, configuredShuffleParallelism);
    // A located record is resolved the way the append handle resolves an update, where a Spark SQL MERGE INTO
    // record applies its matched clauses, so its new version is inserted as evaluated rather than re-evaluated
    // against the not-matched clauses on the insert path
    TypedProperties updateProps = TypedProperties.copy(props);
    updateProps.setProperty(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, "true");
    return taggedRecords
        // NOTE: the incoming record may hold an instance of [[InternalRow]] pointing into a
        //       shared, mutable buffer, so it has to be copied before the shuffle
        .mapToPair(record -> Pair.of(record.getKey(), record.copy()))
        .reduceByKey(HoodieWriteHelper::latestLocation, parallelism)
        .map(Pair::getRight)
        .flatMap(record -> {
          if (!record.isCurrentLocationKnown() || record.isDelete(deleteContext, updateProps)
              || (isExpressionPayload(record) && record.shouldIgnore(schema, updateProps))) {
            return Collections.singletonList(record).iterator();
          }
          HoodieRecord<T> insertRecord = isExpressionPayload(record)
              ? evaluatedNewVersion(record, schema, updateProps) : record.newInstance(record.getKey());
          HoodieRecord<T> deleteRecord = HoodieDeleteHelper.createDeleteRecord(config, record.getKey());
          deleteRecord.setIgnoreIndexUpdate(insertRecord != null);
          HoodieIndexUtils.tagRecord(deleteRecord, record.getCurrentLocation());
          return insertRecord == null
              ? Collections.singletonList(deleteRecord).iterator()
              : Arrays.asList(deleteRecord, insertRecord).iterator();
        });
  }

  private static boolean isExpressionPayload(HoodieRecord<?> record) {
    return record.getData() instanceof HoodieRecordPayload
        && "org.apache.spark.sql.hudi.command.payload.ExpressionPayload".equals(record.getData().getClass().getName());
  }

  /**
   * Returns the new version of a located Spark SQL MERGE INTO record as its matched clauses evaluate it, or null
   * when they delete the record. The delete status a record caches may come from an evaluation outside an update,
   * such as the merged lookup of a global index, so it can miss such a delete.
   */
  private static <T> HoodieRecord<T> evaluatedNewVersion(HoodieRecord<T> record, HoodieSchema schema, TypedProperties updateProps)
      throws IOException {
    Option<IndexedRecord> newVersion = ((HoodieRecordPayload<?>) record.getData()).getInsertValue(schema.toAvroSchema(), updateProps);
    return newVersion.isPresent() ? (HoodieRecord<T>) new HoodieAvroIndexedRecord(record.getKey(), newVersion.get()) : null;
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
}

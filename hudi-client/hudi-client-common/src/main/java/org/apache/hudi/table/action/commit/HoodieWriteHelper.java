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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.List;

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
                                                        List<String> orderingFieldNames,
                                                        BaseKeyGenerator keyGenerator) {
    boolean isIndexingGlobal = index.isGlobal();
    final SerializableSchema schema = new SerializableSchema(schemaStr);
    RecordContext recordContext = readerContext.getRecordContext();
    Schema writerSchema = new Schema.Parser().parse(schemaStr);
    DeleteContext deleteContext = new DeleteContext(props, writerSchema).withReaderSchema(writerSchema);
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
      //       Here we have to make a copy of the incoming record, since it might be holding
      //       an instance of [[InternalRow]] pointing into shared, mutable buffer
      return Pair.of(key, record.copy());
    }).reduceByKey((rec1, rec2) -> {
      HoodieRecord<T> reducedRecord;
      try {
        // NOTE: The order of rec1 and rec2 is uncertain within "reduceByKey".
        Option<BufferedRecord<T>> merged = merge(
            rec1, rec2, schema.get(), schema.get(), recordContext, orderingFieldNames,
            recordMerger, deleteContext, deleteContext, props);
        // NOTE: For merge mode based merging, it returns non-null.
        //       For mergers / payloads based merging, it may return null.

//        reducedRecord = recordContext.constructHoodieRecord(merged.get(), rec1.getPartitionPath());
//        boolean allowOperationMetadataField = ConfigUtils.getBooleanWithAltKeys(props, HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD);
//        Schema writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writerSchema, allowOperationMetadataField);
//        String partitionPath = keyGenerator.getPartitionPath(recordContext.convertToAvroRecord(merged.get(), writerSchema));

//        HoodieRecord<R> withMeta = reducedRecord.prependMetaFields(writerSchema, writeSchemaWithMetaFields,
//            new MetadataValues().setRecordKey(reducedRecord.getRecordKey()).setPartitionPath(partitionPath), props);
//        return withMeta.wrapIntoHoodieRecordPayloadWithParams(writeSchemaWithMetaFields, props, Option.empty(),
//            allowOperationMetadataField, Option.empty(), false, Option.of(writerSchema));
//        TypedProperties recordCreationProps = TypedProperties.copy(props);
//        recordCreationProps.setProperty(POPULATE_META_FIELDS.key(), "false");
//        return reducedRecord.wrapIntoHoodieRecordPayloadWithParams(writerSchema, recordCreationProps, Option.empty(),
//            allowOperationMetadataField, Option.empty(), false, Option.of(writerSchema));
//        TypedProperties recordCreationProps = TypedProperties.copy(props);
//        recordCreationProps.setProperty(POPULATE_META_FIELDS.key(), "false");
//        return reducedRecord.wrapIntoHoodieRecordPayloadWithKeyGen(writerSchema, recordCreationProps, Option.of(keyGenerator));

        //return recordContext.constructHoodieAvroRecord(merged.get(), ConfigUtils.getPayloadClass(props), partitionPath);

//        boolean choosePrev = rec1.getData().equals(merged.get().getRecord());
//        HoodieKey reducedKey = choosePrev ? rec1.getKey() : rec2.getKey();
//        HoodieOperation operation = choosePrev ? rec1.getOperation() : rec2.getOperation();
//        return (HoodieRecord<T>) new HoodieAvroIndexedRecord((IndexedRecord) merged.get().getRecord()).newInstance(reducedKey, operation);

//        return recordContext.constructHoodieRecord(merged.get());
//        String partitionPath = keyGenerator.getPartitionPath((GenericRecord) recordContext.constructHoodieRecord(merged.get()).getData());
        boolean choosePrev = rec1.getData().equals(merged.get().getRecord());
        HoodieKey reducedKey = choosePrev ? rec1.getKey() : rec2.getKey();
        HoodieOperation operation = choosePrev ? rec1.getOperation() : rec2.getOperation();
        HoodieRecordPayload reducedPayload = (HoodieRecordPayload) (choosePrev ? rec1.getData() : rec2.getData());
        return new HoodieAvroRecord(reducedKey, reducedPayload, operation);

//        return recordContext.constructHoodieRecord(merged.get(), partitionPath);
      } catch (IOException e) {
        throw new HoodieException(String.format("Error to merge two records, %s, %s", rec1, rec2), e);
      }
      //return reducedRecord.newInstance(rec1.getKey(), reducedRecord.getOperation());
    }, parallelism).map(Pair::getRight);
  }
}

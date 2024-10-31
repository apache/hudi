/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A stream write function with simple bucket hash index.
 *
 * <p>The task holds a fresh new local index: {(partition + bucket number) &rarr fileId} mapping, this index
 * is used for deciding whether the incoming records in an UPDATE or INSERT.
 * The index is local because different partition paths have separate items in the index.
 *
 * @param <I> the input type
 */
public class BucketStreamWriteFunctionRowData<I> extends StreamWriteFunction<I> {

  private static final Logger LOG = LoggerFactory.getLogger(BucketStreamWriteFunction.class);

  private int parallelism;

  private int bucketNum;

  private String indexKeyFields;

  private boolean isNonBlockingConcurrencyControl;

  /**
   * BucketID to file group mapping in each partition.
   * Map(partition -> Map(bucketId, fileID)).
   */
  private Map<String, Map<Integer, String>> bucketIndex;

  /**
   * Incremental bucket index of the current checkpoint interval,
   * it is needed because the bucket type('I' or 'U') should be decided based on the committed files view,
   * all the records in one bucket should have the same bucket type.
   */
  private Set<String> incBucketIndex;

  /**
   * Functions for calculating the task partition to dispatch.
   */
  private Functions.Function2<String, Integer, Integer> partitionIndexFunc;


  /**
   * To prevent strings compare for each record, define this only during open()
   */
  private boolean isInsertOverwrite;

  private RowType rowType;
  private transient Schema avroSchema;
  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;
  private transient PayloadCreation payloadCreation;

  /**
   * Constructs a BucketStreamWriteFunction.
   *
   * @param config The config options
   */
  public BucketStreamWriteFunctionRowData(Configuration config, RowType rowType) {
    super(config);
    this.rowType = rowType;
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    this.bucketNum = config.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.indexKeyFields = OptionsResolver.getIndexKeyField(config);
    this.isNonBlockingConcurrencyControl = OptionsResolver.isNonBlockingConcurrencyControl(config);
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    this.bucketIndex = new HashMap<>();
    this.incBucketIndex = new HashSet<>();
    this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(bucketNum, parallelism);
    this.isInsertOverwrite = OptionsResolver.isInsertOverwrite(config);

    this.avroSchema = StreamerUtil.getSourceSchema(this.config);
    this.converter = RowDataToAvroConverters.createConverter(this.rowType, this.config.getBoolean(FlinkOptions.WRITE_UTC_TIMEZONE));
    try {
      this.payloadCreation = PayloadCreation.instance(config);
    } catch (Exception ex) {
      throw new HoodieException("Failed payload creation in BucketStreamWriteFunctionRowData", ex);
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
  }

  @Override
  public void snapshotState() {
    super.snapshotState();
    this.incBucketIndex.clear();
  }

  @Override
  public void processElement(I income, ProcessFunction<I, Object>.Context context, Collector<Object> collector) throws Exception {

    Tuple2<StringData, StringData> metadata = ((Tuple2<Tuple, RowData>) income).getField(0);
    String recordKey = metadata.getField(0).toString();
    String partition = metadata.getField(1).toString();

    RowData row = ((Tuple2<Tuple, RowData>) income).getField(1);
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, row);
    final HoodieKey hoodieKey = new HoodieKey(recordKey, partition);
    HoodieRecordPayload payload = payloadCreation.createPayload(gr);
    HoodieOperation operation = HoodieOperation.fromValue(row.getRowKind().toByteValue());
    HoodieRecord record = new HoodieAvroRecord<>(hoodieKey, payload, operation);
    final HoodieRecordLocation location;

    // for insert overwrite operation skip the index loading
    if (!isInsertOverwrite) {
      bootstrapIndexIfNeed(partition);
    }

    Map<Integer, String> bucketToFileId = bucketIndex.computeIfAbsent(partition, p -> new HashMap<>());
    final int bucketNum = BucketIdentifier.getBucketId(hoodieKey, indexKeyFields, this.bucketNum);
    final String bucketId = partition + "/" + bucketNum;

    if (incBucketIndex.contains(bucketId)) {
      location = new HoodieRecordLocation("I", bucketToFileId.get(bucketNum));
    } else if (bucketToFileId.containsKey(bucketNum)) {
      location = new HoodieRecordLocation("U", bucketToFileId.get(bucketNum));
    } else {
      String newFileId = BucketIdentifier.newBucketFileIdPrefix(bucketNum, isNonBlockingConcurrencyControl);
      location = new HoodieRecordLocation("I", newFileId);
      bucketToFileId.put(bucketNum, newFileId);
      incBucketIndex.add(bucketId);
    }
    record.unseal();
    record.setCurrentLocation(location);
    record.seal();
    bufferRecord(record);
  }

  /**
   * Determine whether the current fileID belongs to the current task.
   * partitionIndex == this taskID belongs to this task.
   */
  public boolean isBucketToLoad(int bucketNumber, String partition) {
    return this.partitionIndexFunc.apply(partition, bucketNumber) == taskID;
  }

  /**
   * Get partition_bucket -> fileID mapping from the existing hudi table.
   * This is a required operation for each restart to avoid having duplicate file ids for one bucket.
   */
  private void bootstrapIndexIfNeed(String partition) {
    if (bucketIndex.containsKey(partition)) {
      return;
    }
    LOG.info("Loading Hoodie Table {}, with path {}/{}", this.metaClient.getTableConfig().getTableName(),
        this.metaClient.getBasePath(), partition);

    // Load existing fileID belongs to this task
    Map<Integer, String> bucketToFileIDMap = new HashMap<>();
    this.writeClient.getHoodieTable().getHoodieView().getLatestFileSlices(partition).forEach(fileSlice -> {
      String fileId = fileSlice.getFileId();
      int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileId);
      if (isBucketToLoad(bucketNumber, partition)) {
        LOG.info(String.format("Should load this partition bucket %s with fileId %s", bucketNumber, fileId));
        // Validate that one bucketId has only ONE fileId
        if (bucketToFileIDMap.containsKey(bucketNumber)) {
          throw new RuntimeException(String.format("Duplicate fileId %s from bucket %s of partition %s found "
              + "during the BucketStreamWriteFunction index bootstrap.", fileId, bucketNumber, partition));
        } else {
          LOG.info(String.format("Adding fileId %s to the bucket %s of partition %s.", fileId, bucketNumber, partition));
          bucketToFileIDMap.put(bucketNumber, fileId);
        }
      }
    });
    bucketIndex.put(partition, bucketToFileIDMap);
  }
}

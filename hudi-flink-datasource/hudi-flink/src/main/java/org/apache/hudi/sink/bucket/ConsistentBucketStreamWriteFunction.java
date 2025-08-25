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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.buffer.RowDataBucket;
import org.apache.hudi.sink.clustering.update.strategy.ConsistentBucketUpdateStrategy;
import org.apache.hudi.sink.clustering.update.strategy.ConsistentBucketUpdateStrategy.BucketRecords;
import org.apache.hudi.util.MutableIteratorWrapperIterator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A stream write function with consistent bucket hash index.
 * The function tags each incoming record with a location of a file based on consistent bucket index.
 */
public class ConsistentBucketStreamWriteFunction extends StreamWriteFunction {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketStreamWriteFunction.class);

  private transient ConsistentBucketUpdateStrategy updateStrategy;

  /**
   * Constructs a ConsistentBucketStreamWriteFunction.
   *
   * @param config  The config options
   * @param rowType LogicalType of record
   */
  public ConsistentBucketStreamWriteFunction(Configuration config, RowType rowType) {
    super(config, rowType);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    List<String> indexKeyFields = Arrays.asList(config.get(FlinkOptions.INDEX_KEY_FIELD).split(","));
    this.updateStrategy = new ConsistentBucketUpdateStrategy(this.writeClient, indexKeyFields);
    LOG.info("Create update strategy with index key fields: {}", indexKeyFields);
  }

  @Override
  public void snapshotState() {
    super.snapshotState();
    updateStrategy.reset();
  }

  @Override
  protected List<WriteStatus> writeRecords(String instant, RowDataBucket rowDataBucket) {
    writeMetrics.startFileFlush();
    updateStrategy.initialize(this.writeClient);

    Iterator<BinaryRowData> rowItr =
        new MutableIteratorWrapperIterator<>(
            rowDataBucket.getDataIterator(), () -> new BinaryRowData(rowType.getFieldCount()));
    Iterator<HoodieRecord> recordItr = deduplicateRecordsIfNeeded(
        new MappingIterator<>(rowItr, rowData -> recordConverter.convert(rowData, rowDataBucket.getBucketInfo())),
        rowDataBucket.getBucketInfo().getBucketType());

    Pair<List<BucketRecords>, Set<HoodieFileGroupId>> recordListFgPair =
        updateStrategy.handleUpdate(Collections.singletonList(BucketRecords.of(recordItr, rowDataBucket.getBucketInfo(), instant)));

    return recordListFgPair.getKey().stream()
        .flatMap(bucketRecords -> writeFunction.write(bucketRecords.getRecordItr(), bucketRecords.getBucketInfo(), bucketRecords.getInstant()).stream())
        .collect(Collectors.toList());
  }
}

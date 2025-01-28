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

package org.apache.hudi.sink;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * Sink function to write incoming {@link HoodieFlinkInternalRow}s to the underneath filesystem.
 */
public class StreamWriteRowDataFunction<I extends HoodieFlinkInternalRow> extends StreamWriteFunction<I> {

  private final RowType rowType;
  private transient Schema avroSchema;
  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;
  private transient PayloadCreation payloadCreation;

  public StreamWriteRowDataFunction(Configuration config, RowType rowType) {
    super(config);
    this.rowType = rowType;
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    this.avroSchema = StreamerUtil.getSourceSchema(this.config);
    this.converter = RowDataToAvroConverters.createConverter(this.rowType, this.config.getBoolean(FlinkOptions.WRITE_UTC_TIMEZONE));
    try {
      this.payloadCreation = PayloadCreation.instance(config);
    } catch (Exception ex) {
      throw new HoodieException("Failed payload creation in StreamWriteRowDataFunction", ex);
    }
  }

  @Override
  public void processElement(I record,
                             ProcessFunction<I, Object>.Context ctx,
                             Collector<Object> out) throws Exception {
    RowData row = record.getRowData();
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, row);
    final HoodieKey hoodieKey = new HoodieKey(record.getRecordKey(), record.getPartitionPath());

    // Processing partially switched to `HoodieFlinkInternalRow#operationType`.
    // This is processing of delete records from `BucketAssignRowDataFunction::processHoodieFlinkRecord`
    HoodieRecordPayload payload;
    if (record.getOperationType().equals("D")) {
      payload = payloadCreation.createDeletePayload((BaseAvroPayload) payloadCreation.createPayload(gr));
    } else {
      payload = payloadCreation.createPayload(gr);
    }
    // [HUDI-8968] Use operationType uniformly instead of instantTime
    HoodieOperation operation = HoodieOperation.fromValue(row.getRowKind().toByteValue());
    HoodieRecord hoodieRecord = new HoodieAvroRecord<>(hoodieKey, payload, operation);

    hoodieRecord.unseal();
    hoodieRecord.setCurrentLocation(new HoodieRecordLocation(record.getInstantTime(), record.getFileId()));
    hoodieRecord.seal();

    bufferRecord(hoodieRecord);
  }
}

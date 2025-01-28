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

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * A stream write function with simple bucket hash index for processing of {@link HoodieFlinkRecord},
 * which places records into buffer of {@link StreamWriteFunction}.
 */
final class BucketStreamWriteRowDataFunction<T extends Tuple> extends BucketStreamWriteFunction<T> {

  private final RowType rowType;
  private transient Schema avroSchema;
  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;
  private transient PayloadCreation payloadCreation;

  BucketStreamWriteRowDataFunction(Configuration config, RowType rowType) {
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
      throw new HoodieException("Failed payload creation in BucketStreamWriteRowDataFunction", ex);
    }
  }

  @Override
  public void processElement(T income,
                             ProcessFunction<T, Object>.Context context,
                             Collector<Object> collector) throws Exception {
    HoodieFlinkRecord hoodieFlinkRecord = new HoodieFlinkRecord(income);
    String recordKey = hoodieFlinkRecord.getRecordKey();
    String partition = hoodieFlinkRecord.getPartitionPath();
    RowData row = hoodieFlinkRecord.getRowData();

    final HoodieKey hoodieKey = new HoodieKey(recordKey, partition);
    HoodieRecordPayload payload = payloadCreation.createPayload(
        (GenericRecord) this.converter.convert(this.avroSchema, row));
    HoodieOperation operation = HoodieOperation.fromValue(row.getRowKind().toByteValue());
    HoodieRecord record = new HoodieAvroRecord<>(hoodieKey, payload, operation);

    record.unseal();
    record.setCurrentLocation(defineRecordLocation(hoodieKey, partition));
    record.seal();
    bufferRecord(record);
  }
}

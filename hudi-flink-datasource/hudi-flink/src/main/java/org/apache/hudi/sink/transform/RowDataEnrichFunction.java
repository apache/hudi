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

package org.apache.hudi.sink.transform;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Function that converts Flink {@link RowData} into {@link HoodieFlinkRecord}, which extends Flink {@link Tuple}.
 */
class RowDataEnrichFunction<I extends RowData, O extends Tuple> extends RichMapFunction<I, O> {
  private final Configuration config;
  private final RowType rowType;
  private transient Schema avroSchema;
  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;
  private transient KeyGenerator keyGenerator;

  RowDataEnrichFunction(Configuration config, RowType rowType) {
    this.config = config;
    this.rowType = rowType;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.avroSchema = StreamerUtil.getSourceSchema(this.config);
    this.converter = RowDataToAvroConverters.createConverter(this.rowType, this.config.getBoolean(FlinkOptions.WRITE_UTC_TIMEZONE));
    this.keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(StreamerUtil.flinkConf2TypedProperties(this.config));
  }

  @Override
  public O map(I record) throws Exception {
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
    final HoodieKey hoodieKey = keyGenerator.getKey(gr);
    return (O) new HoodieFlinkRecord(hoodieKey.getRecordKey(), hoodieKey.getPartitionPath(), record);
  }
}

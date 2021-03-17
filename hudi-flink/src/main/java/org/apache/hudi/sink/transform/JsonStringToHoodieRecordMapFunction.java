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

package org.apache.hudi.sink.transform;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieFlinkStreamerException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.schema.FilebasedSchemaProvider;
import org.apache.hudi.util.AvroConvertor;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.IOException;

/**
 * Function helps to transfer json string to {@link HoodieRecord}.
 */
public class JsonStringToHoodieRecordMapFunction implements MapFunction<String, HoodieRecord> {

  private TypedProperties props;
  private KeyGenerator keyGenerator;
  private AvroConvertor avroConvertor;
  private Option<String> schemaStr;
  private String payloadClassName;
  private String orderingField;

  public JsonStringToHoodieRecordMapFunction(TypedProperties props) {
    this(props, Option.empty());
  }

  public JsonStringToHoodieRecordMapFunction(TypedProperties props, Option<String> schemaStr) {
    this.props = props;
    this.schemaStr = schemaStr;
    init();
  }

  @Override
  public HoodieRecord map(String value) throws Exception {
    GenericRecord gr = this.avroConvertor.fromJson(value);
    HoodieRecordPayload payload = StreamerUtil.createPayload(this.payloadClassName, gr,
        (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, this.orderingField, false));

    return new HoodieRecord<>(this.keyGenerator.getKey(gr), payload);
  }

  private void init() {
    if (schemaStr.isPresent()) {
      this.avroConvertor = new AvroConvertor(new Schema.Parser().parse(schemaStr.get()));
    } else {
      this.avroConvertor = new AvroConvertor(new FilebasedSchemaProvider(props).getSourceSchema());
    }
    this.payloadClassName = props.getString(HoodieWriteConfig.WRITE_PAYLOAD_CLASS,
        OverwriteWithLatestAvroPayload.class.getName());
    this.orderingField = props.getString(HoodieWriteConfig.PRECOMBINE_FIELD_PROP, "ts");
    try {
      this.keyGenerator = StreamerUtil.createKeyGenerator(props);
    } catch (IOException e) {
      throw new HoodieFlinkStreamerException(String.format("KeyGenerator %s initialization failed",
          props.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_PROP, SimpleAvroKeyGenerator.class.getName())), e);
    }
  }
}

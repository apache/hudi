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

package org.apache.hudi.util;

import org.apache.hudi.adapter.AbstractRichFunctionAdapter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Wrapper function that manages the lifecycle of the JSON deserialization schema.
 */
public final class JsonDeserializationFunction
    extends AbstractRichFunctionAdapter
    implements MapFunction<String, RowData> {
  private final JsonRowDataDeserializationSchema deserializationSchema;

  public static JsonDeserializationFunction getInstance(Configuration conf) {
    // Read from file source
    RowType rowType =
        (RowType) HoodieSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();
    return getInstance(rowType);
  }

  public static JsonDeserializationFunction getInstance(RowType rowType) {
    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    return new JsonDeserializationFunction(deserializationSchema);
  }

  public JsonDeserializationFunction(JsonRowDataDeserializationSchema deserializationSchema) {
    this.deserializationSchema = deserializationSchema;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.deserializationSchema.open(null);
  }

  @Override
  public RowData map(String record) throws Exception {
    return deserializationSchema.deserialize(getUTF8Bytes(record));
  }
}

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

import org.apache.hudi.util.RowDataToAvroConverters.RowDataToAvroConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utils for get/set operations on {@link RowData}.
 */
public class HoodieRowDataUtil {

  private static final Map<Schema, RowDataToAvroConverter> ROWDATA_CONVERTER_CACHE = new ConcurrentHashMap<>();

  public static RowDataToAvroConverters.RowDataToAvroConverter getRowDataToAvroConverter(Schema schema, boolean utcTimezone) {
    RowDataToAvroConverter converter = ROWDATA_CONVERTER_CACHE.get(schema);
    if (converter == null) {
      LogicalType rowType = AvroSchemaConverter.convertToDataType(schema).getLogicalType();
      converter = RowDataToAvroConverters.createConverter(rowType, utcTimezone);
      ROWDATA_CONVERTER_CACHE.put(schema, converter);
    }
    return converter;
  }
}

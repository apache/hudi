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

package org.apache.hudi.util;

import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utils for converting Avro records into RowData.
 */
public class AvroConverterUtils {
  // Avro converter cache: <record_schema, use_utc_timezone> -> AvroToRowDataConverter
  private static final Map<Pair<Schema, Boolean>, AvroToRowDataConverters.AvroToRowDataConverter> AVRO_CONVERTER_CACHE = new ConcurrentHashMap<>();

  /**
   * Get a {@link AvroToRowDataConverters.AvroToRowDataConverter} from cache.
   *
   * @param recordSchema Avro schema for a record
   * @param utcTimezone whether to use utc timezone to convert timestamp field
   * @return a {@link AvroToRowDataConverters.AvroToRowDataConverter}
   */
  public static AvroToRowDataConverters.AvroToRowDataConverter internAvroConverter(Schema recordSchema, boolean utcTimezone) {
    Pair<Schema, Boolean> cacheKey = Pair.of(recordSchema, utcTimezone);
    return AVRO_CONVERTER_CACHE.computeIfAbsent(cacheKey, key -> {
      RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(recordSchema).getLogicalType();
      return AvroToRowDataConverters.createRowConverter(rowType, utcTimezone);
    });
  }
}

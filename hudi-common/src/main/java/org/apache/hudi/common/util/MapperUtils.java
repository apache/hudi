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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PRECOMBINE_FIELD;
import static org.apache.hudi.TypeUtils.unsafeCast;

public class MapperUtils {

  private static final Logger LOG = LogManager.getLogger(MapperUtils.class);

  public static final String SIMPLE_KEY_GEN_FIELDS_OPT = "SIMPLE_KEY_GEN_FIELDS_OPT";
  public static final String WITH_OPERATION_FIELD = "WITH_OPERATION_FIELD";
  public static final String PARTITION_NAME = "PARTITION_NAME";
  public static final String POPULATE_META_FIELDS = "POPULATE_META_FIELDS";
  public static final String RECORD_TYPE = "RECORD_TYPE";

  public static Map<String, Object> buildMapperConfig(String payloadClass, String preCombineField, Option<Pair<String, String>> simpleKeyGenFieldsOpt, boolean withOperation,
      Option<String> partitionName) {
    HashMap<String, Object> map = new HashMap<>();
    putIfNotNull(map, PAYLOAD_CLASS_NAME.key(), payloadClass);
    putIfNotNull(map, PRECOMBINE_FIELD.key(), preCombineField);
    putIfNotNull(map, SIMPLE_KEY_GEN_FIELDS_OPT, simpleKeyGenFieldsOpt);
    putIfNotNull(map, WITH_OPERATION_FIELD, withOperation);
    putIfNotNull(map, PARTITION_NAME, partitionName);
    return map;
  }

  public static Map<String, Object> buildMapperConfig(String payloadClass, String preCombineField, Option<Pair<String, String>> simpleKeyGenFieldsOpt, boolean withOperation,
      Option<String> partitionName, boolean populateMetaFields) {
    Map<String, Object> map = buildMapperConfig(payloadClass, preCombineField, simpleKeyGenFieldsOpt, withOperation, partitionName);
    map.put(POPULATE_META_FIELDS, populateMetaFields);
    return map;
  }

  public static <T1, T2> Function<T1, HoodieRecord<T2>> createMapper(HoodieRecordType recordType, Function<T1, T2> converter) {
    if (recordType == HoodieRecordType.AVRO) {
      return converter.andThen((rec) -> unsafeCast(new HoodieAvroIndexedRecord((IndexedRecord) rec)));
    } else if (recordType == HoodieRecordType.SPARK) {
      Class<?> recodeClazz = ReflectionUtils.getClass("org.apache.hudi.HoodieSparkRecord");
      Class<?> internalRowClazz = ReflectionUtils.getClass("org.apache.spark.sql.catalyst.InternalRow");
      try {
        Constructor<?> recordConstructor = recodeClazz.getConstructor(internalRowClazz);
        return converter.andThen((rec) -> {
          try {
            return unsafeCast(recordConstructor.newInstance(rec));
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            // This can't happen
            LOG.error("Error new record instance with " + recordType);
            throw new HoodieException(e);
          }
        });
      } catch (NoSuchMethodException e) {
        throw new HoodieException(e);
      }
    } else {
      throw new UnsupportedOperationException(recordType.name());
    }
  }

  /**
   * To support other type of record. If we read avro block, we should deserialize byte[] to IndexRecord then convert to other type.
   */
  public static <T1, T2> Function<T1, T2> createConverter(String dataType, HoodieRecordType recordType, Schema schema) {
    if (dataType.equals(IndexedRecord.class.getName()) && recordType == HoodieRecordType.AVRO) {
      return unsafeCast(Function.identity());
    } else if (dataType.equals(IndexedRecord.class.getName()) && recordType == HoodieRecordType.SPARK) {
      Class<?> utilsClazz = ReflectionUtils.getClass("org.apache.spark.sql.hudi.HoodieInternalRowUtils");
      try {
        Method convertAvro = utilsClazz.getMethod("avro2Row", Schema.class, IndexedRecord.class);
        return (data) -> {
          try {
            return unsafeCast(convertAvro.invoke(null, schema, data));
          } catch (IllegalAccessException | InvocationTargetException e) {
            LOG.error("Error convert data with " + recordType);
            throw new HoodieException(e);
          }
        };
      } catch (NoSuchMethodException e) {
        throw new HoodieException(e);
      }
    } else {
      throw new UnsupportedOperationException(dataType + " and " + recordType);
    }
  }

  private static void putIfNotNull(Map<String, Object> map, String name, Object value) {
    if (value != null) {
      map.put(name, value);
    }
  }
}

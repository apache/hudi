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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.OperationModeAwareness;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility class for HoodieRecord.
 */
public class HoodieRecordUtils {
  private static final Map<String, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();
  private static final Map<String, Constructor<?>> CONSTRUCTOR_CACHE_WITH_ORDERING_VALUE = new ConcurrentHashMap<>();
  private static final Set<String> DEPRECATED_PAYLOADS = Stream.of(
      AWSDmsAvroPayload.class.getName(),
      DefaultHoodieRecordPayload.class.getName(),
      EventTimeAvroPayload.class.getName(),
      HoodieAvroPayload.class.getName(),
      OverwriteNonDefaultsWithLatestAvroPayload.class.getName(),
      OverwriteWithLatestAvroPayload.class.getName(),
      RewriteAvroPayload.class.getName(),
      PostgresDebeziumAvroPayload.class.getName(),
      MySqlDebeziumAvroPayload.class.getName()).collect(Collectors.toSet());

  /**
   * Instantiate a given class with a record merge.
   */
  public static HoodieRecordMerger loadRecordMerger(String mergerClass) {
    return (HoodieRecordMerger) ReflectionUtils.loadClass(mergerClass, new Object[] {});
  }

  /**
   * Instantiate a given class with a record merge.
   */
  public static HoodieRecordMerger createRecordMerger(String basePath, EngineType engineType,
                                                      List<String> mergerClassList, String recordMergerStrategy) {
    if (mergerClassList.isEmpty() || HoodieTableMetadata.isMetadataTable(basePath)) {
      return new HoodieAvroRecordMerger();
    } else {
      return createValidRecordMerger(engineType, mergerClassList, recordMergerStrategy)
          .orElseGet(HoodieAvroRecordMerger::new);
    }
  }

  public static Option<HoodieRecordMerger> createValidRecordMerger(EngineType engineType,
                                                                   String mergerImpls, String recordMergerStrategy) {
    if (recordMergerStrategy.equals(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID)) {
      return Option.of(new HoodieAvroRecordMerger());
    }
    return createValidRecordMerger(engineType,ConfigUtils.split2List(mergerImpls), recordMergerStrategy);
  }

  public static Option<HoodieRecordMerger> createValidRecordMerger(EngineType engineType,
                                                                   List<String> mergeImplClassList,
                                                                   String recordMergeStrategyId) {
    return Option.fromJavaOptional(mergeImplClassList.stream()
        .map(clazz -> loadRecordMerger(clazz))
        .filter(Objects::nonNull)
        .filter(merger -> merger.getMergingStrategy().equals(recordMergeStrategyId))
        .filter(merger -> recordTypeCompatibleEngine(merger.getRecordType(), engineType))
        .findFirst());
  }

  public static boolean isPayloadClassDeprecated(String recordPayloadClass) {
    return DEPRECATED_PAYLOADS.contains(recordPayloadClass);
  }

  /**
   * Instantiate a given class with an avro record payload.
   */
  public static <T extends HoodieRecordPayload> T loadPayload(String recordPayloadClass, GenericRecord record, Comparable orderingValue) {
    if (orderingValue == null) {
      return loadPayload(recordPayloadClass, record);
    }
    try {
      return (T) CONSTRUCTOR_CACHE_WITH_ORDERING_VALUE.computeIfAbsent(recordPayloadClass, key -> {
        try {
          return ReflectionUtils.getClass(recordPayloadClass).getConstructor(GenericRecord.class, Comparable.class);
        } catch (NoSuchMethodException ex) {
          throw new HoodieException("Unable to find constructor for payload class: " + recordPayloadClass, ex);
        }
      }).newInstance(record, orderingValue);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new HoodieException("Unable to instantiate payload class ", e);
    }
  }

  /**
   * Instantiate a given class with an avro record payload.
   */
  public static <T extends HoodieRecordPayload> T loadPayload(String recordPayloadClass, GenericRecord record) {
    try {
      return (T) CONSTRUCTOR_CACHE.computeIfAbsent(recordPayloadClass, key -> {
        try {
          return ReflectionUtils.getClass(recordPayloadClass).getConstructor(Option.class);
        } catch (NoSuchMethodException ex) {
          throw new HoodieException("Unable to find constructor for payload class: " + recordPayloadClass, ex);
        }
      }).newInstance(Option.ofNullable(record));
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new HoodieException("Unable to instantiate payload class ", e);
    }
  }

  public static HoodieRecord createHoodieRecord(GenericRecord data, Comparable orderingVal, HoodieKey hKey, String payloadClass, boolean requiresPayload) {
    return createHoodieRecord(data, orderingVal, hKey, payloadClass, null, Option.empty(), requiresPayload);
  }

  public static HoodieRecord createHoodieRecord(GenericRecord data, Comparable orderingVal, HoodieKey hKey,
                                                String payloadClass, HoodieOperation hoodieOperation, Option<HoodieRecordLocation> recordLocation, boolean requiresPayload) {
    HoodieRecord record;
    if (!requiresPayload && isPayloadClassDeprecated(payloadClass)) {
      record = new HoodieAvroIndexedRecord(hKey, data, orderingVal, hoodieOperation);
    } else {
      HoodieRecordPayload payload = HoodieRecordUtils.loadPayload(payloadClass, data, orderingVal);
      record = new HoodieAvroRecord<>(hKey, payload, hoodieOperation);
    }
    recordLocation.ifPresent(record::setCurrentLocation);
    return record;
  }

  public static HoodieRecord createHoodieRecord(GenericRecord data, HoodieKey hKey,
                                                String payloadClass, boolean requiresPayload) {
    return createHoodieRecord(data, hKey, payloadClass, Option.empty(), requiresPayload);
  }

  public static HoodieRecord createHoodieRecord(GenericRecord data, HoodieKey hKey,
                                                String payloadClass, Option<HoodieRecordLocation> recordLocation, boolean requiresPayload) {
    HoodieRecord record;
    if (!requiresPayload && isPayloadClassDeprecated(payloadClass)) {
      record = new HoodieAvroIndexedRecord(hKey, data);
    } else {
      HoodieRecordPayload payload = HoodieRecordUtils.loadPayload(payloadClass, data);
      record = new HoodieAvroRecord<>(hKey, payload);
    }
    recordLocation.ifPresent(record::setCurrentLocation);
    return record;
  }

  public static <R> R generateEmptyPayload(String recKey, String partitionPath, Comparable orderingVal, String payloadClazz) {
    HoodieKey key = new HoodieKey(recKey, partitionPath);
    return (R) new HoodieAvroRecord<>(key, HoodieRecordUtils.loadPayload(payloadClazz, null, orderingVal));
  }

  public static HoodieRecord generateEmptyAvroRecord(HoodieKey key, Comparable orderingVal, String payloadClazz, HoodieOperation hoodieOperation) {
    if (isPayloadClassDeprecated(payloadClazz)) {
      return new HoodieEmptyRecord<>(key, hoodieOperation, orderingVal, HoodieRecordType.AVRO);
    }
    return new HoodieAvroRecord<>(key, HoodieRecordUtils.loadPayload(payloadClazz, null, orderingVal), hoodieOperation, true);
  }

  public static boolean recordTypeCompatibleEngine(HoodieRecordType recordType, EngineType engineType) {
    return (engineType == EngineType.SPARK && recordType == HoodieRecordType.SPARK)
        || engineType == EngineType.FLINK && recordType == HoodieRecordType.FLINK
        || (engineType == EngineType.JAVA && recordType == HoodieRecordType.AVRO);
  }

  public static HoodieRecordMerger mergerToPreCombineMode(HoodieRecordMerger merger) {
    return merger instanceof OperationModeAwareness ? ((OperationModeAwareness) merger).asPreCombiningMode() : merger;
  }

  public static String getCurrentLocationInstant(HoodieRecord<?> record) {
    if (record.getCurrentLocation() != null) {
      return record.getCurrentLocation().getInstantTime();
    }
    return null;
  }

  public static List<String> getOrderingFieldNames(RecordMergeMode mergeMode,
                                                   TypedProperties props,
                                                   HoodieTableMetaClient metaClient) {
    return mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING
        ? Collections.emptyList()
        : Option.ofNullable(ConfigUtils.getOrderingFields(props)).map(Arrays::asList).orElseGet(() -> metaClient.getTableConfig().getOrderingFields());
  }
}
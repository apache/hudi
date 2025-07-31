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

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.OperationModeAwareness;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A utility class for HoodieRecord.
 */
public class HoodieRecordUtils {
  private static final Map<String, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();

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
      return HoodieAvroRecordMerger.INSTANCE;
    } else {
      return createValidRecordMerger(engineType, mergerClassList, recordMergerStrategy)
          .orElse(HoodieAvroRecordMerger.INSTANCE);
    }
  }

  public static Option<HoodieRecordMerger> createValidRecordMerger(EngineType engineType,
                                                                   String mergerImpls, String recordMergerStrategy) {
    if (recordMergerStrategy.equals(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID)) {
      return Option.of(HoodieAvroRecordMerger.INSTANCE);
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

  /**
   * Instantiate a given class with an avro record payload.
   */
  public static <T extends HoodieRecordPayload> T loadPayload(String recordPayloadClass, GenericRecord record, Comparable orderingValue) {
    try {
      return (T) CONSTRUCTOR_CACHE.computeIfAbsent(recordPayloadClass, key -> {
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
}
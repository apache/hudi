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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A utility class for HoodieRecord.
 */
public class HoodieRecordUtils {

  private static final Map<String, Object> INSTANCE_CACHE = new HashMap<>();
  private static final Logger LOG = LogManager.getLogger(HoodieRecordUtils.class);

  /**
   * Instantiate a given class with a record merge.
   */
  public static HoodieRecordMerger loadRecordMerger(String mergerClass) {
    try {
      HoodieRecordMerger recordMerger = (HoodieRecordMerger) INSTANCE_CACHE.get(mergerClass);
      if (null == recordMerger) {
        synchronized (HoodieRecordMerger.class) {
          recordMerger = (HoodieRecordMerger) INSTANCE_CACHE.get(mergerClass);
          if (null == recordMerger) {
            recordMerger = (HoodieRecordMerger) ReflectionUtils.loadClass(mergerClass,
                new Object[]{});
            INSTANCE_CACHE.put(mergerClass, recordMerger);
          }
        }
      }
      return recordMerger;
    } catch (HoodieException e) {
      throw new HoodieException("Unable to instantiate hoodie merge class ", e);
    }
  }

  /**
   * Instantiate a given class with a record merge.
   */
  public static HoodieRecordMerger generateRecordMerger(String basePath, EngineType engineType,
      List<String> mergerClassList) {
    if (mergerClassList.isEmpty() || HoodieTableMetadata.isMetadataTable(basePath)) {
      return HoodieRecordUtils.loadRecordMerger(HoodieAvroRecordMerger.class.getName());
    } else {
      return mergerClassList.stream()
          .map(clazz -> {
            try {
              return loadRecordMerger(clazz);
            } catch (HoodieException e) {
              LOG.warn(String.format("Unable to init %s", clazz), e);
              return null;
            }
          })
          .filter(Objects::nonNull)
          .filter(merger -> recordTypeCompatibleEngine(merger.getRecordType(), engineType))
          .findFirst()
          .orElse(HoodieRecordUtils.loadRecordMerger(HoodieAvroRecordMerger.class.getName()));
    }
  }

  /**
   * Instantiate a given class with an avro record payload.
   */
  public static <T extends HoodieRecordPayload> T loadPayload(String recordPayloadClass,
      Object[] payloadArgs,
      Class<?>... constructorArgTypes) {
    try {
      return (T) ReflectionUtils.getClass(recordPayloadClass).getConstructor(constructorArgTypes)
          .newInstance(payloadArgs);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException("Unable to instantiate payload class ", e);
    }
  }

  public static boolean recordTypeCompatibleEngine(HoodieRecordType recordType, EngineType engineType) {
    if (engineType == EngineType.SPARK && recordType == HoodieRecordType.SPARK) {
      return true;
    } else {
      return false;
    }
  }
}
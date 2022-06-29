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

import org.apache.hudi.common.model.HoodieMerge;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility class for HoodieRecord.
 */
public class HoodieRecordUtils {

  private static final Map<String, Object> INSTANCE_CACHE = new HashMap<>();

  /**
   * Instantiate a given class with a record merge.
   */
  public static HoodieMerge loadMerge(String mergeClass) {
    try {
      HoodieMerge merge = (HoodieMerge) INSTANCE_CACHE.get(mergeClass);
      if (null == merge) {
        synchronized (HoodieMerge.class) {
          merge = (HoodieMerge) INSTANCE_CACHE.get(mergeClass);
          if (null == merge) {
            merge = (HoodieMerge)ReflectionUtils.loadClass(mergeClass, new Object[]{});
            INSTANCE_CACHE.put(mergeClass, merge);
          }
        }
      }
      return merge;
    } catch (HoodieException e) {
      throw new HoodieException("Unable to instantiate hoodie merge class ", e);
    }
  }

  /**
   * Instantiate a given class with an avro record payload.
   */
  public static <T extends HoodieRecordPayload> T loadPayload(String recordPayloadClass, Object[] payloadArgs,
                                                              Class<?>... constructorArgTypes) {
    try {
      return (T) ReflectionUtils.getClass(recordPayloadClass).getConstructor(constructorArgTypes).newInstance(payloadArgs);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException("Unable to instantiate payload class ", e);
    }
  }
}
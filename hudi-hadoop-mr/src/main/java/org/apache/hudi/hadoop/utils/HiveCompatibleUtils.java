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

package org.apache.hudi.hadoop.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

/**
 * This class is used to assist compatibility with versions smaller than hive 2.3 (hive 2.x).
 * There have been some API changes on hive (2.0-2.2) and hive 2.3, mainly the data structure
 * returned in mapwork.getPathToAliases. In 2.3, the key returned was path, while in previous
 * versions, String was returned. What was done here is to convert it uniformly to Path so
 * that existing code can accommodate these changes.
 */
public class HiveCompatibleUtils {

  /**
   * Convert the key of the internal map(IOPrepareCache.get().allocatePartitionDescMap)
   * of PartitionDescMap from String to Path
   */
  public static <K> Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> convertPartitionDesc(
      Map<Map<K, PartitionDesc>, Map<K, PartitionDesc>> oldMap) {
    Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> resMap = new LinkedHashMap<>();
    Set<Entry<Map<K, PartitionDesc>, Map<K, PartitionDesc>>> entries = oldMap.entrySet();
    for (Entry<Map<K, PartitionDesc>, Map<K, PartitionDesc>> entry : entries) {
      Map<Path, PartitionDesc> newKeyMap = convertMapKeyToPath(entry.getKey());
      Map<Path, PartitionDesc> newValueMap = convertMapKeyToPath(entry.getValue());
      resMap.put(newKeyMap, newValueMap);
    }
    return resMap;
  }

  /**
   * Convert the key of PathToAliases map(MapWork.getPathToAliases) from String to Path
   */
  public static <K, V> Map<Path, V> convertMapKeyToPath(Map<K, V> pathToAliases) {
    return convertMap(pathToAliases, key -> {
      if (key instanceof String) {
        return new Path(String.valueOf(key));
      } else {
        return (Path) key;
      }
    });
  }

  private static <K, V, T> Map<T, V> convertMap(Map<K, V> oldMap, Function<K, T> convertFunction) {
    LinkedHashMap<T, V> resMap = new LinkedHashMap<>();
    for (Entry<K, V> kvEntry : oldMap.entrySet()) {
      resMap.put(convertFunction.apply(kvEntry.getKey()), kvEntry.getValue());
    }
    return resMap;
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * Common transformations in test cases.
 */
public final class Transformations {

  public static <T> List<T> flatten(Iterator<List<T>> iteratorOfLists) {
    List<T> flattened = new ArrayList<>();
    iteratorOfLists.forEachRemaining(flattened::addAll);
    return flattened;
  }

  public static <T> Iterator<T> flattenAsIterator(Iterator<List<T>> iteratorOfLists) {
    return flatten(iteratorOfLists).iterator();
  }

  public static Set<String> recordsToRecordKeySet(List<HoodieRecord> records) {
    return records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
  }

  public static List<HoodieKey> recordsToHoodieKeys(List<HoodieRecord> records) {
    return records.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
  }

  public static Map<String, List<HoodieRecord>> recordsToPartitionRecordsMap(List<HoodieRecord> records) {
    return records.stream().collect(groupingBy(HoodieRecord::getPartitionPath));
  }

  /**
   * Pseudorandom: select even indices first, then select odd ones.
   */
  public static <T> List<T> randomSelect(List<T> items, int n) {
    int s = items.size();
    if (n < 0 || n > s) {
      throw new IllegalArgumentException(String.format("Invalid number of items to select! Valid range for n: [0, %s]", s));
    }
    List<T> selected = new ArrayList<>();
    for (int i = 0, numSelected = 0; i < s && numSelected < n; i += 2, numSelected++) {
      selected.add(items.get(i));
    }
    for (int i = 1, numSelected = selected.size(); i < s && numSelected < n; i += 2, numSelected++) {
      selected.add(items.get(i));
    }
    return selected;
  }

  public static List<HoodieKey> randomSelectAsHoodieKeys(List<HoodieRecord> records, int n) {
    return randomSelect(recordsToHoodieKeys(records), n);
  }
}

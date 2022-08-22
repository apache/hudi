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

package org.apache.hudi.testutils;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.bloom.BloomIndexFileInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BloomIndexTestUtils {
  public static Map<String, BloomIndexFileInfo> convertBloomIndexFileInfoListToMap(
      List<BloomIndexFileInfo> infoList) {
    return infoList.stream().collect(Collectors.toMap(BloomIndexFileInfo::getFileId, Function.identity()));
  }

  public static Map<String, BloomIndexFileInfo> convertBloomIndexFileInfoPairListToMap(
      List<Pair<String, BloomIndexFileInfo>> filesList) {
    Map<String, BloomIndexFileInfo> filesMap = new HashMap<>();
    for (Pair<String, BloomIndexFileInfo> t : filesList) {
      filesMap.put(t.getKey() + "/" + t.getValue().getFileId(), t.getValue());
    }
    return filesMap;
  }

  public static void assertBloomInfoEquals(
      List<Pair<String, BloomIndexFileInfo>> expected, List<Pair<String, BloomIndexFileInfo>> actual) {
    assertEquals(expected.size(), actual.size());
    Map<String, BloomIndexFileInfo> expectedMap = convertBloomIndexFileInfoPairListToMap(expected);
    Map<String, BloomIndexFileInfo> actualMap = convertBloomIndexFileInfoPairListToMap(actual);

    for (String key : expectedMap.keySet()) {
      assertTrue(actualMap.containsKey(key));
      BloomIndexFileInfo expectedInfo = expectedMap.get(key);
      BloomIndexFileInfo actualInfo = actualMap.get(key);
      assertEquals(expectedInfo.getFileId(), actualInfo.getFileId());
      assertEquals(expectedInfo.getMinRecordKey(), actualInfo.getMinRecordKey());
      assertEquals(expectedInfo.getMaxRecordKey(), actualInfo.getMaxRecordKey());
    }
  }
}

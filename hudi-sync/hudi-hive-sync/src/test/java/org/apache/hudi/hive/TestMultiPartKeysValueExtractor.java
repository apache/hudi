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

package org.apache.hudi.hive;

import org.apache.hudi.sync.common.model.partextractor.MultiPartKeysValueExtractor;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMultiPartKeysValueExtractor {

  @Test
  public void testMultiPartExtractor() {
    MultiPartKeysValueExtractor valueExtractor = new MultiPartKeysValueExtractor();
    // Test extract empty partitionPath
    assertEquals(new ArrayList<>(), valueExtractor.extractPartitionValuesInPath(""));
    List<String> expected = new ArrayList<>();
    expected.add("2021-04-25");
    expected.add("04");
    // Test extract multi-partition path
    assertEquals(expected, valueExtractor.extractPartitionValuesInPath("2021-04-25/04"));
    // Test extract hive style partition path
    assertEquals(expected, valueExtractor.extractPartitionValuesInPath("ds=2021-04-25/hh=04"));
  }
}

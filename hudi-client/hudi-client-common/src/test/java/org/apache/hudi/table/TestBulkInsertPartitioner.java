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

package org.apache.hudi.table;

import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.stream.Stream;

public class TestBulkInsertPartitioner {

  private static Stream<Arguments> argsForTryPrependPartitionColumns() {
    return Stream.of(
        Arguments.of(Arrays.asList("_hoodie_partition_path", "col1", "col2").toArray(), Arrays.asList("col1", "col2").toArray(), true, "pt"),
        Arguments.of(Arrays.asList("_hoodie_partition_path", "col1", "col2").toArray(), Arrays.asList("col1", "_hoodie_partition_path", "col2").toArray(), true, "pt"),
        Arguments.of(Arrays.asList("col1", "col2").toArray(), Arrays.asList("col1", "col2").toArray(), false, ""),
        Arguments.of(Arrays.asList("pt1", "col1", "col2").toArray(), Arrays.asList("col1", "col2").toArray(), false, "pt1"),
        Arguments.of(Arrays.asList("pt1", "pt2", "col1", "col2").toArray(), Arrays.asList("col1", "col2").toArray(), false, "pt1,pt2"),
        Arguments.of(Arrays.asList("pt1", "pt2", "col1", "col2").toArray(), Arrays.asList("col1", "pt1", "col2").toArray(), false, "pt1,pt2")
    );
  }
}

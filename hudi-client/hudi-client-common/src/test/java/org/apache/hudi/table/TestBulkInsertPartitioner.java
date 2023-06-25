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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

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

  @ParameterizedTest
  @MethodSource("argsForTryPrependPartitionColumns")
  public void testTryPrependPartitionColumns(String[] expectedSortColumns, String[] sortColumns, boolean populateMetaField, String partitionColumnName) {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionColumnName);
    props.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaField));
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/").withProperties(props).build();
    assertArrayEquals(expectedSortColumns, BulkInsertPartitioner.tryPrependPartitionPathColumns(sortColumns, writeConfig));
  }

}

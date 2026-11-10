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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

public class TestRowCustomColumnsSortPartitioner {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void tryPrependPartitionPathAndSuffixRecordKeyColumns(boolean suffixRecordKey) {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS.key(), String.valueOf(suffixRecordKey));
    HoodieWriteConfig writeConfig = HoodieWriteConfig
        .newBuilder()
        .withPath("/")
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withProperties(properties)
        .build();

    String[] columnValues = new String[] {"col1", "col2", "col3"};
    String[] sortColumns = RowCustomColumnsSortPartitioner.tryPrependPartitionPathAndSuffixRecordKeyColumns(columnValues, writeConfig);
    if (suffixRecordKey) {
      Assertions.assertArrayEquals(new Object[] {"_hoodie_partition_path", "col1", "col2", "col3", "_hoodie_record_key"}, sortColumns);
    } else {
      Assertions.assertArrayEquals(new Object[] {"_hoodie_partition_path", "col1", "col2", "col3"}, sortColumns);
    }
  }

  @Test
  void constructorRejectsColumnsWithoutOrdering() {
    // MultipleSparkJobExecutionStrategy.getPartitioner validates before it constructs a
    // partitioner, so this constructor check is only reached through a user-defined partitioner
    // (hoodie.bulkinsert.user.defined.partitioner.class).
    String schema = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("v", HoodieSchema.createVariant()))).toString();
    HoodieWriteConfig writeConfig = HoodieWriteConfig
        .newBuilder()
        .withPath("/")
        .withSchema(schema)
        .build();

    HoodieException failure = Assertions.assertThrows(HoodieException.class,
        () -> new RowCustomColumnsSortPartitioner(new String[] {"v"}, writeConfig));
    Assertions.assertTrue(failure.getMessage().contains("Sorting by column 'v'"),
        "The error must name the column, got: " + failure.getMessage());
    Assertions.assertDoesNotThrow(() -> new RowCustomColumnsSortPartitioner(new String[] {"id"}, writeConfig));
  }
}

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

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.FlatLists;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSortUtils {

  private static final String SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  @Test
  void testPrependPartitionPath() {
    Object[] columnValues = new Object[] {"col1", "col2", "col3"};
    Object[] sortColumns = SortUtils.prependPartitionPath("partition_path", columnValues);
    Assertions.assertArrayEquals(new Object[] {"partition_path", "col1", "col2", "col3"}, sortColumns);
  }

  @Test
  void testPrependPartitionPathAndSuffixRecordKey() {
    Object[] columnValues = new Object[] {"col1", "col2", "col3"};
    Object[] sortColumns = SortUtils.prependPartitionPathAndSuffixRecordKey("partition_path", "record_key", columnValues);
    Assertions.assertArrayEquals(new Object[] {"partition_path", "col1", "col2", "col3", "record_key"}, sortColumns);
  }

  @ParameterizedTest
  @MethodSource("getArguments")
  void testGetComparableSortColumnsAvroRecord(HoodieRecordType recordType, boolean suffixRecordKey) {
    Schema schema = new Schema.Parser().parse(SCHEMA);
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("non_pii_col", "val1");
    genericRecord.put("pii_col", "val2");
    genericRecord.put("timestamp", 3.5);
    HoodieRecordPayload payload = new RewriteAvroPayload(genericRecord);

    HoodieRecord record;
    if (recordType == HoodieRecordType.AVRO) {
      record = new HoodieAvroRecord(new HoodieKey("record1", "partition1"), payload);
    } else {
      record = new TestSparkRecord(new HoodieKey("record1", "partition1"), payload);
    }
    String[] userSortColumns = new String[] {"non_pii_col", "timestamp"};
    FlatLists.ComparableList<Comparable<HoodieRecord>> comparableList = SortUtils.getComparableSortColumns(record, userSortColumns, HoodieSchema.parse(SCHEMA), suffixRecordKey, true);
    Object[] expectedSortColumnValues;
    if (suffixRecordKey) {
      expectedSortColumnValues = new Object[] {"partition1", "val1", 3.5, "record1"};
    } else {
      expectedSortColumnValues = new Object[] {"partition1", "val1", 3.5};
    }
    assertEquals(FlatLists.ofComparableArray(expectedSortColumnValues), comparableList);
  }

  private static Stream<Arguments> getArguments() {
    return Stream.of(
        Arguments.of(HoodieRecordType.SPARK, true),
        Arguments.of(HoodieRecordType.SPARK, false),
        Arguments.of(HoodieRecordType.AVRO, true),
        Arguments.of(HoodieRecordType.AVRO, false)
    );
  }

  public static class TestSparkRecord<T extends HoodieRecordPayload> extends HoodieAvroRecord {

    public TestSparkRecord(HoodieKey key, T data) {
      super(key, data);
    }

    @Override
    public HoodieRecordType getRecordType() {
      return HoodieRecordType.SPARK;
    }
  }
}

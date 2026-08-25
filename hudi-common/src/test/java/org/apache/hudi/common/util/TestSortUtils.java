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
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.collection.FlatLists;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  void testValidateSortableColumnsRejectsTypesWithoutOrderingAtAnyDepth() {
    HoodieSchema stringMap = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema structWithMap = HoodieSchema.createRecord("struct_with_map", null, null,
        Collections.singletonList(HoodieSchemaField.of("m", stringMap)));
    HoodieSchema schema = HoodieSchema.createRecord("sortable_rec", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("v", HoodieSchema.createVariant()),
        HoodieSchemaField.of("m", HoodieSchema.createNullable(stringMap)),
        HoodieSchemaField.of("s", structWithMap),
        HoodieSchemaField.of("arr", HoodieSchema.createArray(stringMap)),
        HoodieSchemaField.of("blob", HoodieSchema.createBlob()),
        HoodieSchemaField.of("vec", HoodieSchema.createVector(8))));

    // VARIANT and MAP have no ordering, at the top level or nested inside a struct or an array.
    HoodieException variantFailure = assertThrows(HoodieException.class,
        () -> SortUtils.validateSortableColumns(new String[] {"v"}, schema));
    assertTrue(variantFailure.getMessage().contains("Sorting by column 'v'"),
        "The error must name the column, got: " + variantFailure.getMessage());
    // A top-level offender is fully described by the column and its type: no nested path is added.
    assertFalse(variantFailure.getMessage().contains("it holds a"),
        "A top-level VARIANT must not be reported as holding one, got: " + variantFailure.getMessage());
    assertThrows(HoodieException.class, () -> SortUtils.validateSortableColumns(new String[] {"m"}, schema));

    // A nested offender is named by its path, so the user can see which member to drop rather than
    // only that some part of the struct or array is unorderable.
    HoodieException structFailure = assertThrows(HoodieException.class,
        () -> SortUtils.validateSortableColumns(new String[] {"s"}, schema));
    assertTrue(structFailure.getMessage().contains("it holds a MAP at 's.m'"),
        "The error must name the nested member, got: " + structFailure.getMessage());
    HoodieException arrayFailure = assertThrows(HoodieException.class,
        () -> SortUtils.validateSortableColumns(new String[] {"arr"}, schema));
    assertTrue(arrayFailure.getMessage().contains("it holds a MAP at 'arr[]'"),
        "The error must name the array element, got: " + arrayFailure.getMessage());

    // Matching is case-insensitive, mirroring Spark's column resolution.
    HoodieException upperCaseFailure = assertThrows(HoodieException.class,
        () -> SortUtils.validateSortableColumns(new String[] {"V"}, schema));
    assertTrue(upperCaseFailure.getMessage().contains("Sorting by column 'V'"),
        "The error must name the column as configured, got: " + upperCaseFailure.getMessage());

    // BLOB is a struct of atomics in Spark and VECTOR an array of floats: both are orderable, and
    // rejecting them would be a new restriction on a path this check does not otherwise touch.
    assertDoesNotThrow(() -> SortUtils.validateSortableColumns(new String[] {"blob"}, schema));
    assertDoesNotThrow(() -> SortUtils.validateSortableColumns(new String[] {"vec"}, schema));
    assertDoesNotThrow(() -> SortUtils.validateSortableColumns(new String[] {"id", "name"}, schema));

    // Names absent from the schema are left for the caller to handle, and missing inputs are no-ops.
    assertDoesNotThrow(() -> SortUtils.validateSortableColumns(new String[] {"not_a_column"}, schema));
    assertDoesNotThrow(() -> SortUtils.validateSortableColumns((String[]) null, schema));
    assertDoesNotThrow(() -> SortUtils.validateSortableColumns(new String[0], schema));
    assertDoesNotThrow(() -> SortUtils.validateSortableColumns(new String[] {"v"}, (HoodieSchema) null));
  }

  @ParameterizedTest
  @MethodSource("getArguments")
  void testGetComparableSortColumnsAvroRecord(HoodieRecordType recordType, boolean suffixRecordKey) {
    HoodieSchema schema = HoodieSchema.parse(SCHEMA);
    GenericRecord genericRecord = new GenericData.Record(schema.toAvroSchema());
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

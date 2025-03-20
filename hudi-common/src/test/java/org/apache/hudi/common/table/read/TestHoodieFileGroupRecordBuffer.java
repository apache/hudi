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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.table.read.HoodieBaseFileGroupRecordBuffer.getOrderingValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HoodieBaseFileGroupRecordBuffer}
 */
public class TestHoodieFileGroupRecordBuffer {
  @Test
  void testGetOrderingValueFromDeleteRecord() {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    DeleteRecord deleteRecord = mock(DeleteRecord.class);
    mockDeleteRecord(deleteRecord, null);
    assertEquals(DEFAULT_ORDERING_VALUE, getOrderingValue(readerContext, deleteRecord));
    mockDeleteRecord(deleteRecord, DEFAULT_ORDERING_VALUE);
    assertEquals(DEFAULT_ORDERING_VALUE, getOrderingValue(readerContext, deleteRecord));
    String orderingValue = "xyz";
    String convertedValue = "_xyz";
    mockDeleteRecord(deleteRecord, orderingValue);
    when(readerContext.convertValueToEngineType(orderingValue)).thenReturn(convertedValue);
    assertEquals(convertedValue, getOrderingValue(readerContext, deleteRecord));
  }

  @ParameterizedTest
  @CsvSource({
      "true, true, EVENT_TIME_ORDERING",
      "true, false, EVENT_TIME_ORDERING",
      "false, true, EVENT_TIME_ORDERING",
      "false, false, EVENT_TIME_ORDERING",
      "true, true, COMMIT_TIME_ORDERING",
      "true, false, COMMIT_TIME_ORDERING",
      "false, true, COMMIT_TIME_ORDERING",
      "false, false, COMMIT_TIME_ORDERING",
      "true, true, CUSTOM",
      "true, false, CUSTOM",
      "false, true, CUSTOM",
      "false, false, CUSTOM",
      "true, true,",
      "true, false,",
      "false, true,",
      "false, false,"
  })
  public void testSchemaForMandatoryFields(boolean setPrecombine, boolean addHoodieIsDeleted, RecordMergeMode mergeMode) {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getHasBootstrapBaseFile()).thenReturn(false);
    when(readerContext.getHasLogFiles()).thenReturn(true);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    when(readerContext.getRecordMerger()).thenReturn(Option.of(recordMerger));
    when(recordMerger.isProjectionCompatible()).thenReturn(false);

    String preCombineField = "ts";
    List<String> dataSchemaFields = new ArrayList<>();
    dataSchemaFields.addAll(Arrays.asList(
        HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, preCombineField,
        "colA", "colB", "colC", "colD"));
    if (addHoodieIsDeleted) {
      dataSchemaFields.add(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    }

    Schema dataSchema = getSchema(dataSchemaFields);
    Schema requestedSchema = getSchema(Arrays.asList(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD));

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordMergeMode()).thenReturn(mergeMode);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    when(tableConfig.getPreCombineField()).thenReturn(setPrecombine ? preCombineField : StringUtils.EMPTY_STRING);

    TypedProperties props = new TypedProperties();
    HoodieFileGroupReaderSchemaHandler fileGroupReaderSchemaHandler = new HoodieFileGroupReaderSchemaHandler(readerContext,
        dataSchema, requestedSchema, Option.empty(), tableConfig, props);
    List<String> expectedFields = new ArrayList();
    expectedFields.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    expectedFields.add(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    if (setPrecombine && mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING) { // commit time ordering does not project ordering field.
      expectedFields.add(preCombineField);
    }
    if (addHoodieIsDeleted) {
      expectedFields.add(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    }
    Schema expectedSchema = mergeMode == RecordMergeMode.CUSTOM ? dataSchema : getSchema(expectedFields);
    Schema actualSchema = fileGroupReaderSchemaHandler.generateRequiredSchema();
    assertEquals(expectedSchema, actualSchema);
  }

  private Schema getSchema(List<String> fields) {
    SchemaBuilder.FieldAssembler<Schema> schemaFieldAssembler = SchemaBuilder.builder().record("test_schema")
        .namespace("test_namespace").fields();
    for (String field : fields) {
      schemaFieldAssembler = schemaFieldAssembler.name(field).type().stringType().noDefault();
    }
    return schemaFieldAssembler.endRecord();
  }

  private void mockDeleteRecord(DeleteRecord deleteRecord,
                                Comparable orderingValue) {
    when(deleteRecord.getOrderingValue()).thenReturn(orderingValue);
  }
}

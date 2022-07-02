/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link org.apache.hudi.util.AvroSchemaConverter}.
 */
public class TestAvroSchemaConverter {
  @Test
  void testUnionSchemaWithMultipleRecordTypes() {
    Schema schema = HoodieMetadataRecord.SCHEMA$;
    DataType dataType = AvroSchemaConverter.convertToDataType(schema);
    int pos = HoodieMetadataRecord.SCHEMA$.getField(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).pos();
    final String expected = "ROW<"
        + "`fileName` STRING, "
        + "`columnName` STRING, "
        + "`minValue` ROW<`wrapper` RAW('java.lang.Object', ?) NOT NULL>, "
        + "`maxValue` ROW<`wrapper` RAW('java.lang.Object', ?) NOT NULL>, "
        + "`valueCount` BIGINT, "
        + "`nullCount` BIGINT, "
        + "`totalSize` BIGINT, "
        + "`totalUncompressedSize` BIGINT, "
        + "`isDeleted` BOOLEAN NOT NULL>";
    assertThat(dataType.getChildren().get(pos).toString(), is(expected));
  }
}

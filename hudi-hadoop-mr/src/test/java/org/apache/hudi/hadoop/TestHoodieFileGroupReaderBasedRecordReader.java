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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class TestHoodieFileGroupReaderBasedRecordReader {

  @Test
  void testGetProgressAndPosForLogFilesOnlyInMergeOnReadTable() throws Exception {
    HoodieFileGroupReaderBasedRecordReader recordReader = new HoodieFileGroupReaderBasedRecordReader(
        mock(HiveHoodieReaderContext.class),
        mock(ClosableIterator.class),
        mock(ArrayWritable.class),
        mock(InputSplit.class),
        mock(JobConf.class),
        mock(UnaryOperator.class),
        false
    );

    assertEquals(0, recordReader.getProgress());
    assertEquals(0, recordReader.getPos());
  }

  /**
   * Test to verify that the functionality handles duplicate column names
   * that could occur when columns are referenced in both SELECT and WHERE clauses.
   * This test ensures the fix for duplicate field exception works correctly.
   * The fix was applied in createRequestedSchema method where .distinct() was added
   * to prevent duplicate column names from causing schema generation issues.
   */
  @Test
  void testDuplicateFieldHandlingInHiveQueryWithWhereClause() {
    JobConf jobConf = new JobConf();
    // Simulate a query where same column appears in both SELECT and WHERE clauses
    // This would result in duplicates in READ_COLUMN_NAMES_CONF_STR like "field1,field2,field1"
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "field1,field2,part1,field1");
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "part1");

    String schemaStr = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"testRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"field1\", \"type\": \"string\"},\n"
        + "    {\"name\": \"field2\", \"type\": \"int\"},\n"
        + "    {\"name\": \"field3\", \"type\": \"int\"}\n"
        + "  ]\n"
        + "}";
    HoodieSchema tableSchema = HoodieSchema.parse(schemaStr);
    HoodieSchema requestedSchema = HoodieFileGroupReaderBasedRecordReader.createRequestedSchema(tableSchema, jobConf);
    assertEquals(2, requestedSchema.getFields().size());
    assertEquals("field1", requestedSchema.getFields().get(0).name());
    assertEquals("field2", requestedSchema.getFields().get(1).name());
  }
}

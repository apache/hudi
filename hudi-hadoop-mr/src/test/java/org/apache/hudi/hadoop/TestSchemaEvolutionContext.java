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

import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers how {@link SchemaEvolutionContext#setColumnTypeList} reads {@code hive.io.file.readcolumn.ids}.
 * Every id there is parsed with {@code Integer#parseInt}, so the blank entries HIVE-22438 leaves behind and
 * the unset key both used to surface as a bare {@code NumberFormatException} or an NPE rather than as the
 * size mismatch the method already reports.
 */
public class TestSchemaEvolutionContext {

  // col2 is a record whose nested field was renamed, so setColumnTypeList has something to write back:
  // primitive types are returned unchanged, and an unchanged rewrite cannot tell a right pairing from a wrong one.
  private static final List<Types.Field> TWO_FIELDS = Arrays.asList(
      Types.Field.get(0, "col1", Types.StringType.get()),
      Types.Field.get(1, "col2", Types.RecordType.get(
          Types.Field.get(2, "renamed", Types.StringType.get()))));

  private JobConf job;
  private SchemaEvolutionContext context;

  @BeforeEach
  public void setUp() throws IOException {
    job = new JobConf();
    // Keeps the constructor off the table: it is the projection-id parsing below that is under test.
    job.setBoolean("hudi.hive.schema.evolution", false);
    job.set(serdeConstants.LIST_COLUMN_TYPES, "string,struct<original:string>");
    context = new SchemaEvolutionContext(new FileSplit(new Path("file:///tmp/unused"), 0, 0, (String[]) null), job);
  }

  @Test
  public void testSetColumnTypeListWithUnsetReadColumnIds() {
    job.unset(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
    HoodieException thrown = assertThrows(HoodieException.class, () -> context.setColumnTypeList(job, TWO_FIELDS));
    assertTrue(thrown.getMessage().contains("is not equal to projection columns"),
        () -> "Expected the size mismatch rather than an NPE, got: " + thrown.getMessage());
  }

  @Test
  public void testSetColumnTypeListWithBlankReadColumnId() {
    job.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, ",0");
    HoodieException thrown = assertThrows(HoodieException.class, () -> context.setColumnTypeList(job, TWO_FIELDS));
    assertTrue(thrown.getMessage().contains("is not equal to projection columns"),
        () -> "Expected the size mismatch rather than a NumberFormatException, got: " + thrown.getMessage());
    assertTrue(thrown.getMessage().contains("#nonBlankIds: 1, #projectionColumns: 2"),
        () -> "the message should carry the counts that were compared, got: " + thrown.getMessage());
  }

  @Test
  public void testSetColumnTypeListIgnoresBlankAndPaddedReadColumnIds() {
    job.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, ", 0 ,1");
    assertDoesNotThrow(() -> context.setColumnTypeList(job, TWO_FIELDS));
    assertEquals("string,struct<renamed:string>", job.get(serdeConstants.LIST_COLUMN_TYPES),
        "id 1 should still pair with col2, so the renamed nested field lands in the second slot");
  }
}

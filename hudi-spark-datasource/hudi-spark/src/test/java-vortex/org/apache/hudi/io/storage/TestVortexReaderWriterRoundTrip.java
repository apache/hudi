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

package org.apache.hudi.io.storage;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end round-trip test for the Vortex file writer and reader against the real vortex-jni
 * API. Writes InternalRows through {@link HoodieSparkVortexWriter} and reads them back through
 * {@link HoodieSparkVortexReader}, spanning multiple flushed batches.
 */
@DisabledIfSystemProperty(named = "vortex.skip.tests", matches = "true")
public class TestVortexReaderWriterRoundTrip {

  @TempDir
  File tempDir;

  @Test
  public void testWriteReadRoundTrip() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();

    StructType schema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("name", DataTypes.StringType, false)
        .add("score", DataTypes.DoubleType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/roundtrip.vortex");

    int numRows = 2500; // > default batch size (1000) to force multiple flushed batches
    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      try (HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path)
          .sparkSchema(schema)
          .instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier)
          .storage(storage)
          .populateMetaFields(false)
          .build()) {
        for (int i = 0; i < numRows; i++) {
          InternalRow row = new GenericInternalRow(new Object[] {
              (long) (100 + i), UTF8String.fromString("name" + i), (double) i + 0.5});
          writer.writeRow(row);
        }
      }

      assertTrue(storage.exists(path), "Vortex file should exist");

      try (HoodieSparkVortexReader reader = new HoodieSparkVortexReader(path)) {
        assertEquals(numRows, reader.getTotalRecords(), "row count");

        HoodieSchema readSchema = reader.getSchema();
        assertEquals(3, readSchema.getFields().size(), "schema field count");

        int count = 0;
        try (ClosableIterator<HoodieRecord<InternalRow>> it = reader.getRecordIterator(readSchema)) {
          while (it.hasNext()) {
            InternalRow row = it.next().getData();
            assertEquals(100L + count, row.getLong(0), "id at row " + count);
            assertEquals("name" + count, row.getUTF8String(1).toString(), "name at row " + count);
            assertEquals((double) count + 0.5, row.getDouble(2), 1e-9, "score at row " + count);
            count++;
          }
        }
        assertEquals(numRows, count, "rows read back");
      }
    }
  }

  @Test
  public void testProjectedRead() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();

    StructType schema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("name", DataTypes.StringType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/projected.vortex");
    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      try (HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path).sparkSchema(schema).instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier).storage(storage).populateMetaFields(false).build()) {
        for (int i = 0; i < 3; i++) {
          writer.writeRow(new GenericInternalRow(new Object[] {(long) i, UTF8String.fromString("n" + i)}));
        }
      }

      // Project only the non-leading "name" column so the assertion fails if the reader returns the
      // full row instead of narrowing to the requested schema (projecting the leading "id" column
      // would read back at index 0 either way and could not detect that regression).
      HoodieSchema projected = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
          new StructType().add("name", DataTypes.StringType, false), "record", "", false);
      try (HoodieSparkVortexReader reader = new HoodieSparkVortexReader(path);
           ClosableIterator<HoodieRecord<InternalRow>> it = reader.getRecordIterator(projected)) {
        int count = 0;
        while (it.hasNext()) {
          InternalRow row = it.next().getData();
          assertEquals("n" + count, row.getUTF8String(0).toString(), "projected name at row " + count);
          count++;
        }
        assertEquals(3, count, "projected rows");
      }
    }
  }
}

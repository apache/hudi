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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.file.LanceFileReader;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Tests for {@link HoodieRowDataLanceWriter}. */
public class TestHoodieRowDataLanceWriter {

  @TempDir
  Path tempDir;

  @Test
  public void testWritesVectorDataAndFooterMetadata() throws Exception {
    RowType rowType = RowType.of(
        new LogicalType[] {new IntType(false), new ArrayType(new FloatType(false))},
        new String[] {"id", "embedding"});
    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(
        rowType, "vector_record", "embedding:2");
    StoragePath path = new StoragePath(tempDir.resolve("vector.lance").toUri());

    try (HoodieRowDataLanceWriter writer = new HoodieRowDataLanceWriter(
        path,
        hoodieSchema,
        "001",
        mock(TaskContextSupplier.class),
        Option.empty(),
        128 * 1024 * 1024L,
        64 * 1024 * 1024L,
        16 * 1024 * 1024L,
        true,
        false,
        false)) {
      writer.writeRow("key1", GenericRowData.of(
          1, new GenericArrayData(new Object[] {1.25F, 2.5F})));
    }

    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator);
         ArrowReader arrowReader = reader.readAll(null, null, Integer.MAX_VALUE)) {
      assertEquals(1, reader.numRows());
      Map<String, String> metadata = reader.schema().getCustomMetadata();
      assertEquals("embedding:VECTOR(2)",
          metadata.get(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY));
      ArrowType.FixedSizeList fixedSizeList = assertInstanceOf(
          ArrowType.FixedSizeList.class, reader.schema().findField("embedding").getType());
      assertEquals(2, fixedSizeList.getListSize());

      assertTrue(arrowReader.loadNextBatch());
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      FixedSizeListVector vector = (FixedSizeListVector) root.getVector("embedding");
      Float4Vector elements = (Float4Vector) vector.getDataVector();
      assertEquals(1.25F, elements.get(0));
      assertEquals(2.5F, elements.get(1));
    }
  }
}

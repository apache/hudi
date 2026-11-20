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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBlobTableSchema {

  @Test
  public void columnsMatchTheVersionedBlobContract() {
    StructType schema = BlobTableSchema.schema();
    assertEquals(Arrays.asList("source_path_sha", "source_modification_time", "source_file_path", "source_file_name",
        "source_file_extension", "source_file_size", "content_type", "blob"), Arrays.asList(schema.fieldNames()));
    for (String required : Arrays.asList("source_path_sha", "source_modification_time", "source_file_path", "source_file_name",
        "source_file_extension", "source_file_size")) {
      assertFalse(schema.apply(required).nullable(), required);
    }
    assertEquals(DataTypes.LongType, schema.apply("source_modification_time").dataType());
    assertEquals(DataTypes.LongType, schema.apply("source_file_size").dataType());
    assertTrue(schema.apply("content_type").nullable());
    assertTrue(schema.apply("blob").nullable());
  }

  @Test
  public void blobRetainsItsLogicalTypeAndInlineByteField() {
    StructField blob = BlobTableSchema.schema().apply("blob");
    assertEquals("BLOB", blob.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    StructType value = (StructType) blob.dataType();
    assertEquals(Arrays.asList("type", "data", "reference"), Arrays.asList(value.fieldNames()));
    assertEquals(DataTypes.StringType, value.apply("type").dataType());
    assertEquals(DataTypes.BinaryType, value.apply("data").dataType());
    StructType reference = (StructType) value.apply("reference").dataType();
    assertEquals(Arrays.asList("external_path", "offset", "length", "managed"), Arrays.asList(reference.fieldNames()));
  }
}

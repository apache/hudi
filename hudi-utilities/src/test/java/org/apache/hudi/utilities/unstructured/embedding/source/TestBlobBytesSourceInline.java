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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.util.FileIOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link BlobBytesSource} over the INLINE blobs the blob ingest writes, and how {@link FileMeta} names a file. */
public class TestBlobBytesSourceInline {

  @Test
  void opensTheInlineBytesOfABlobStruct() throws Exception {
    byte[] bytes = "%PDF-1.7 inline bytes".getBytes(StandardCharsets.UTF_8);
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
    Row blob = inlineBlob(bytes);
    try (InputStream in = source.open(blob)) {
      assertArrayEquals(bytes, FileIOUtils.readAsByteArray(in));
    }
    try (InputStream again = source.open(blob)) {
      assertArrayEquals(bytes, FileIOUtils.readAsByteArray(again), "every open is a fresh stream from the start");
    }
  }

  @Test
  void fileMetaNamesTheFileByItsSourcePathSha() {
    assertEquals("source_path_sha abc (pdf, 42 bytes)", new FileMeta("abc", "pdf", 42L).toString());
  }

  private static Row inlineBlob(byte[] bytes) {
    StructType type = (StructType) BlobType.dataType();
    Object[] values = new Object[type.fields().length];
    values[type.fieldIndex(HoodieSchema.Blob.TYPE)] = HoodieSchema.Blob.INLINE;
    values[type.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)] = bytes;
    return new GenericRowWithSchema(values, type);
  }
}

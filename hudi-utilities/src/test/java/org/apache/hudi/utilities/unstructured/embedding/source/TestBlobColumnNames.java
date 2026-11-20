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

import org.apache.hudi.utilities.sources.blob.BlobTableSchema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The blob table column names: {@link BlobTableSchema} owns them, and {@link BlobColumns} reads the same strings.
 */
class TestBlobColumnNames {

  @Test
  void blobColumnNamesKeepTheirLiterals() {
    List<String> literals = Arrays.asList("source_path_sha", "source_modification_time", "source_file_path", "source_file_name",
        "source_file_extension", "source_file_size", "content_type", "blob");
    List<String> schemaColumns = Arrays.asList(BlobTableSchema.SOURCE_PATH_SHA, BlobTableSchema.SOURCE_MODIFICATION_TIME,
        BlobTableSchema.SOURCE_FILE_PATH, BlobTableSchema.SOURCE_FILE_NAME, BlobTableSchema.SOURCE_FILE_EXTENSION,
        BlobTableSchema.SOURCE_FILE_SIZE, BlobTableSchema.CONTENT_TYPE, BlobTableSchema.BLOB);
    assertEquals(literals, schemaColumns);
    List<String> sourceRowColumns = Arrays.asList(BlobColumns.SOURCE_PATH_SHA, BlobColumns.SOURCE_MODIFICATION_TIME, BlobColumns.SOURCE_FILE_PATH,
        BlobColumns.SOURCE_FILE_NAME, BlobColumns.SOURCE_FILE_EXTENSION, BlobColumns.SOURCE_FILE_SIZE, BlobColumns.CONTENT_TYPE,
        BlobColumns.BLOB);
    assertEquals(literals, sourceRowColumns, "the source row columns the embedding side reads");
  }
}

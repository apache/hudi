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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.table.HoodieTableConfig;

/** Storage and schema versions owned by the unstructured streams, independent of Hudi's table version. */
public final class UnstructuredTableVersion {
  /**
   * Version 1 preserves the current managed-blob names and resolves scheme-less paths against the table. The key is Hudi's own
   * ({@link HoodieTableConfig#BLOB_LAYOUT_VERSION}), which the blob reader resolves paths by.
   */
  public static final ConfigProperty<String> BLOB_LAYOUT_VERSION = HoodieTableConfig.BLOB_LAYOUT_VERSION;
  /** The stream that owns the table: blob or embedding. */
  public static final ConfigProperty<String> KIND = ConfigProperty.key("hoodie.table.unstructured.kind").noDefaultValue();
  /** The column contract of the table kind, currently version 1. */
  public static final ConfigProperty<String> SCHEMA_VERSION = ConfigProperty
      .key("hoodie.table.unstructured.schema.version").noDefaultValue();

  /** Version 1 hashes the UTF-8 bucket/key verbatim, independent of S3 filesystem scheme aliases. */
  public static final ConfigProperty<String> KEY_ENCODING_VERSION = ConfigProperty
      .key("hoodie.table.unstructured.key.encoding.version").noDefaultValue();

  /** Common persisted format pins, shared by source creation and standalone embedding validation. */
  public static UnstructuredTableProperties.Builder tablePropertiesBuilder(String sourceName, String kind) {
    return UnstructuredTableProperties.builder(sourceName)
        .requireTableProperty(BLOB_LAYOUT_VERSION, "1")
        .requireTableProperty(KIND, kind)
        .requireTableProperty(SCHEMA_VERSION, "1")
        .requireTableProperty(KEY_ENCODING_VERSION, "1");
  }

  private UnstructuredTableVersion() {
  }
}

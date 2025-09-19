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

package org.apache.parquet.avro;

import org.apache.hudi.AvroSchemaConverter;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.parquet.schema.PrimitiveType;

/**
 * Parquet-Java AvroSchemaConverter doesn't support local timestamp types until version 1.14
 * for this reason we use a modified version of the AvroSchemaConverter that adds support for local timestamp types
 * Parquet-Java still supports local timestamp types from version 1.11.0, just that the AvroSchemaConverter
 * doesn't work.
 * <p>
 * However, for versions < 1.11.0, local timestamp is not supported at all. Therefore, we just use the
 * library AvroSchemaConverter in this case.
 *
 */
public interface AvroAdapter {
  static AvroAdapter getAdapter() {
    String version = PrimitiveType.class.getPackage().getImplementationVersion();
    if (version != null) {
      String[] parts = version.split("\\.");
      if (parts.length < 3) {
        throw new RuntimeException("Invalid version: " + version);
      }
      int major = Integer.parseInt(parts[0]);
      int minor = Integer.parseInt(parts[1]);

      // Use native adapter for anything < 1.11.0 or >= 1.14.0
      if ((major == 1 && minor < 11) || (major == 1 && minor >= 14)) {
        return ReflectionUtils.loadClass("org.apache.parquet.avro.NativeAvroAdapter");
      }
    }
    try {
      Class.forName("org.apache.parquet.avro.HoodieAvroSchemaConverter");
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return ReflectionUtils.loadClass("org.apache.parquet.avro.NativeAvroAdapter");
    }
    return ReflectionUtils.loadClass("org.apache.parquet.avro.ModifiedAvroAdapter");
  }

  AvroSchemaConverter getAvroSchemaConverter(StorageConfiguration<?> storageConfiguration);
}

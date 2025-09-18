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

package org.apache.hudi;

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.stats.ValueType;

import org.apache.parquet.schema.PrimitiveType;

/**
 * LogicalTypeAnnotations are added in parquet 1.11.0
 * For versions < 1.11.0, we use OriginalType, which LogicalTypeAnnotations replaced
 */
public interface ParquetAdapter {

  static ParquetAdapter getAdapter() {
    String version = PrimitiveType.class.getPackage().getImplementationVersion();
    if (version != null) {
      String[] parts = version.split("\\.");
      if (parts.length < 3) {
        throw new RuntimeException("Invalid version: " + version);
      }
      int major = Integer.parseInt(parts[0]);
      int minor = Integer.parseInt(parts[1]);

      // Use old adapter for anything < 1.11.0
      if (major < 1 || (major == 1 && minor < 11)) {
        return ReflectionUtils.loadClass("org.apache.parquet.schema.OriginalTypeParquetAdapter");
      }
    }
    try {
      return ReflectionUtils.loadClass("org.apache.parquet.schema.LogicalTypeParquetAdapter");
    } catch (IllegalAccessError e) {
      return ReflectionUtils.loadClass("org.apache.parquet.schema.OriginalTypeParquetAdapter");
    }
  }

  boolean hasAnnotation(PrimitiveType primitiveType);

  ValueType getValueTypeFromAnnotation(PrimitiveType primitiveType);

  int getPrecision(PrimitiveType primitiveType);

  int getScale(PrimitiveType primitiveType);
}

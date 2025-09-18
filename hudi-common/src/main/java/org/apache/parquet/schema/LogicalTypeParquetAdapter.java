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

package org.apache.parquet.schema;

import org.apache.hudi.ParquetAdapter;
import org.apache.hudi.stats.ValueType;

/**
 * Uses LogicalTypeAnnotation to extract value type, precision, and scale
 */
public class LogicalTypeParquetAdapter implements ParquetAdapter {
  @Override
  public boolean hasAnnotation(PrimitiveType primitiveType) {
    return primitiveType.getLogicalTypeAnnotation() != null;
  }

  @Override
  public ValueType getValueTypeFromAnnotation(PrimitiveType primitiveType) {
    return LogicalTypeTokenParser.fromLogicalTypeAnnotation(primitiveType);
  }

  @Override
  public int getPrecision(PrimitiveType primitiveType) {
    return LogicalTypeTokenParser.getPrecision(primitiveType);
  }

  @Override
  public int getScale(PrimitiveType primitiveType) {
    return LogicalTypeTokenParser.getScale(primitiveType);
  }
}

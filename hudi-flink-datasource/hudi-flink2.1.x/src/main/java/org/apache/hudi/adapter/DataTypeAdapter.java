/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.adapter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;
import org.apache.parquet.schema.LogicalTypeAnnotation;

/**
 * Adapter utils to provide {@code DataType} utilities.
 */
public class DataTypeAdapter {

  /**
   * The Parquet Variant binary format specification version. Version 1 is the initial spec
   * defined by the Parquet Variant proposal (parquet-format 2.11.0 / parquet-java 1.15.2).
   */
  private static final byte VARIANT_SPEC_VERSION = 1;

  private static final LogicalTypeAnnotation VARIANT_ANNOTATION =
      LogicalTypeAnnotation.variantType(VARIANT_SPEC_VERSION);

  /**
   * Returns the Parquet VARIANT {@link LogicalTypeAnnotation}.
   */
  public static LogicalTypeAnnotation variantParquetAnnotation() {
    return VARIANT_ANNOTATION;
  }

  public static Variant getVariant(RowData rowData, int pos) {
    return rowData.getVariant(pos);
  }

  public static Object createVariant(byte[] value, byte[] metadata) {
    return new BinaryVariant(value, metadata);
  }

  public static boolean isVariantType(LogicalType logicalType) {
    return logicalType.getTypeRoot() == LogicalTypeRoot.VARIANT;
  }

  public static DataType createVariantType() {
    return DataTypes.VARIANT();
  }

  public static byte[] getVariantMetadata(Object obj) {
    return ((BinaryVariant) obj).getMetadata();
  }

  public static byte[] getVariantValue(Object obj) {
    return ((BinaryVariant) obj).getValue();
  }
}

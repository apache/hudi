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

package org.apache.hudi.common.avro;

import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.GroupType;

/**
 * Interface for shredding variant values at write time and reconstructing them at read time.
 * <p>
 * Implementations parse variant binary data (value + metadata bytes) and produce
 * a shredded {@link GenericRecord} with typed_value columns populated according
 * to the shredding schema.
 * <p>
 * This interface allows the variant binary parsing logic (which may depend on
 * engine-specific libraries like Spark's variant module) to be loaded via reflection,
 * keeping the core write support free of engine-specific dependencies.
 */
public interface VariantShreddingProvider {

  /**
   * Transform an unshredded variant GenericRecord into a shredded one.
   * <p>
   * The input record is expected to have:
   * <ul>
   *   <li>{@code value}: ByteBuffer containing the variant value binary</li>
   *   <li>{@code metadata}: ByteBuffer containing the variant metadata binary</li>
   * </ul>
   * <p>
   * The output record should conform to {@code shreddedSchema} and have:
   * <ul>
   *   <li>{@code value}: ByteBuffer or null (null when typed_value captures the full value)</li>
   *   <li>{@code metadata}: ByteBuffer (always present)</li>
   *   <li>{@code typed_value}: the typed representation extracted from the variant binary,
   *       or null if the variant type does not match the typed_value schema</li>
   * </ul>
   *
   * @param unshreddedVariant GenericRecord with {value: ByteBuffer, metadata: ByteBuffer}
   * @param shreddedSchema    target Avro schema with {value: nullable ByteBuffer, metadata: ByteBuffer, typed_value: type}
   * @param variantSchema     HoodieSchema.Variant containing the shredding schema information
   * @return a GenericRecord conforming to shreddedSchema with typed_value populated where possible
   */
  GenericRecord shredVariantRecord(
      GenericRecord unshreddedVariant,
      Schema shreddedSchema,
      HoodieSchema.Variant variantSchema);

  /**
   * Prepares the reconstruction of one shredded variant column of a parquet file (the inverse of
   * {@link #shredVariantRecord}), validating its shredding layout once, when the file is opened.
   * <p>
   * Used on the read path: records read from an already-shredded base file (compaction/clustering)
   * arrive with {@code typed_value} populated. The returned rebuilder turns each such record back
   * into the standard unshredded {@code {metadata, value}} shape before it reaches the merger/writer.
   * <p>
   * The layout comes from the file's parquet type rather than from the Avro schema the record is read
   * at: converting the parquet footer to Avro loses encodings (unsigned and decimal INT32/INT64, JSON,
   * nanosecond timestamps) that decide how a {@code typed_value} must be read.
   *
   * @param columnPath         dotted path of the variant column, for error messages
   * @param parquetVariantType the variant column's parquet group, as it sits in the file
   * @param unshreddedSchema   target Avro schema with {value: ByteBuffer, metadata: ByteBuffer}
   * @return a rebuilder for records of this column
   * @throws org.apache.hudi.exception.HoodieException if the column's shredding layout cannot be read
   */
  VariantRebuilder prepareRebuild(String columnPath, GroupType parquetVariantType, Schema unshreddedSchema);

  /**
   * Rebuilds the records of one shredded variant column, as prepared by {@link #prepareRebuild}.
   */
  interface VariantRebuilder {

    /**
     * Rebuilds the full variant binary of one record of the column.
     *
     * @param shreddedVariant GenericRecord with {value, metadata, typed_value} read from a shredded base file
     * @return a GenericRecord conforming to the unshredded schema with the full reconstructed variant
     *         binary in {@code value}, or {@code null} when {@code shreddedVariant} is null
     * @throws org.apache.hudi.exception.HoodieException if {@code shreddedVariant} is missing its
     *         required {@code metadata} field (a malformed shredded base file)
     */
    GenericRecord rebuild(GenericRecord shreddedVariant);
  }
}

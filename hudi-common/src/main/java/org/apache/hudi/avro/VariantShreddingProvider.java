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

package org.apache.hudi.avro;

import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Interface for shredding variant values at write time.
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
}
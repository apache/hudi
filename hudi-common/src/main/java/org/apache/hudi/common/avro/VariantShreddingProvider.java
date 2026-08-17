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
   * Provider implementations to auto-detect on the classpath, in priority order, when the variant
   * shredding provider class is not set explicitly via config. Sole provider today: variant
   * shredding currently requires Spark 4.0+.
   */
  String[] CLASSPATH_CANDIDATES = {"org.apache.hudi.variant.Spark4VariantShreddingProvider"};

  /**
   * Returns the fully-qualified class name of the first {@link VariantShreddingProvider}
   * implementation available on the classpath, or {@code null} if none is present.
   */
  static String detectProviderClassOnClasspath() {
    for (String candidate : CLASSPATH_CANDIDATES) {
      try {
        Class.forName(candidate);
        return candidate;
      } catch (ClassNotFoundException | NoClassDefFoundError e) {
        // Provider not on classpath; try the next candidate.
      }
    }
    return null;
  }

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
   * Reconstruct an unshredded variant GenericRecord from a shredded one (the inverse of
   * {@link #shredVariantRecord}).
   * <p>
   * Used on the read path: records read from an already-shredded base file (compaction/clustering)
   * arrive with {@code typed_value} populated. This rebuilds the full variant binary so the record
   * presents the standard unshredded {@code {metadata, value}} shape before it reaches the
   * merger/writer.
   *
   * @param shreddedVariant  GenericRecord with {value, metadata, typed_value} read from a shredded base file
   * @param shreddedSchema   the Avro schema of {@code shreddedVariant} (carries typed_value)
   * @param unshreddedSchema target Avro schema with {value: ByteBuffer, metadata: ByteBuffer}
   * @return a GenericRecord conforming to {@code unshreddedSchema} with the full reconstructed
   *         variant binary in {@code value}, or {@code null} when {@code shreddedVariant} is null
   * @throws org.apache.hudi.exception.HoodieException if {@code shreddedVariant} is missing its
   *         required {@code metadata} field (a malformed shredded base file)
   */
  GenericRecord rebuildVariantRecord(
      GenericRecord shreddedVariant,
      Schema shreddedSchema,
      Schema unshreddedSchema);
}
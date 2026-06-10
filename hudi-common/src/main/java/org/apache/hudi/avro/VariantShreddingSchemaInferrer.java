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

import java.util.List;
import java.util.Map;

/**
 * Infers per-file variant shredding schemas (typed_value) from sampled variant binaries.
 *
 * <p>Implementations wrap an engine's inference machinery (e.g. Spark 4.1's
 * {@code InferVariantShreddingSchema}) and are loaded via classpath detection, keeping
 * hudi-common free of engine-specific dependencies. Implementations must be stateless,
 * thread-safe and expose a public no-arg constructor.</p>
 *
 * <p>See https://github.com/apache/hudi/issues/18937.</p>
 */
public interface VariantShreddingSchemaInferrer {

  /**
   * Infers a typed_value schema for each variant column from row-aligned binary samples.
   *
   * <p>All variant columns of a file must be passed in ONE call: inference heuristics may
   * apply a global budget (e.g. Spark caps the total shredded field count per file) and
   * per-column calls would skew it.</p>
   *
   * @param columnNames top-level variant column names, in a fixed order
   * @param rowSamples  one entry per sampled record; {@code rowSamples.get(i)[j]} holds the
   *                    binaries of column {@code columnNames.get(j)} in record {@code i}, or
   *                    null when that variant value is null
   * @return column name to typed_value HoodieSchema in the nested shredding-spec form
   *         (object fields wrapped as {@code {value, typed_value}}); a column absent from the
   *         map declined inference and stays unshredded. May throw on malformed variant
   *         binaries; callers must treat any throw as "decline all".
   */
  Map<String, HoodieSchema> inferTypedValueSchemas(List<String> columnNames, List<VariantSample[]> rowSamples);

  /**
   * The raw binary pair of a single variant value. Both arrays are always non-null; a null
   * variant is represented by a null {@code VariantSample} slot instead.
   */
  final class VariantSample {
    private final byte[] value;
    private final byte[] metadata;

    public VariantSample(byte[] value, byte[] metadata) {
      this.value = value;
      this.metadata = metadata;
    }

    public byte[] getValue() {
      return value;
    }

    public byte[] getMetadata() {
      return metadata;
    }
  }
}

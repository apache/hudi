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

package org.apache.hudi.io.storage;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * View over a Lance-bound {@link InternalRow} that delegates every accessor to the
 * wrapped input row, except at configured variant ordinals where it returns a
 * pre-allocated {@code (metadata, value)} struct populated by extractors built
 * from {@link org.apache.spark.sql.hudi.SparkAdapter#createVariantValueWriter}.
 *
 * <p>The implementation lives in a Spark-4-specific module since it must override
 * {@code InternalRow.getVariant} (introduced in Spark 4.0). This interface keeps
 * the shared writer module free of Spark-4-only types.
 */
public interface VariantProjectedRow {

  /**
   * Set the underlying row and return an {@link InternalRow} view of this projection.
   * Implementations are typically stateful and return {@code this}; the returned row
   * is valid only until the next call to {@code wrap}.
   */
  InternalRow wrap(InternalRow input);
}

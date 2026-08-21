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

import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.spark.unsafe.types.UTF8String;

/**
 * Layout of the Hudi BLOB struct {@code {type, data, reference}} as decoded by the Spark-side
 * Lance code. Shared by {@link BlobDescriptorTransform} (read) and {@link HoodieSparkLanceWriter}
 * (write) so the two cannot drift apart. Ordinals mirror the field order defined by
 * {@link HoodieSchema.Blob}.
 */
final class BlobStructLayout {

  // Child field indices within the Hudi BLOB struct: {type(0), data(1), reference(2)}.
  static final int TYPE_IDX = 0;
  static final int DATA_IDX = 1;
  static final int REF_IDX = 2;
  static final int FIELD_COUNT = HoodieSchema.Blob.getFieldCount();
  static final int REF_FIELD_COUNT = HoodieSchema.Blob.getReferenceFieldCount();

  // Precomputed type tokens, compared against each row's type field without per-row allocation.
  static final UTF8String INLINE_UTF8 = UTF8String.fromString(HoodieSchema.Blob.INLINE);
  static final UTF8String OUT_OF_LINE_UTF8 = UTF8String.fromString(HoodieSchema.Blob.OUT_OF_LINE);

  private BlobStructLayout() {
  }
}

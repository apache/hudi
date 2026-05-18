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
import org.apache.hudi.exception.HoodieException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.UTF8String;
import org.lance.spark.vectorized.BlobStructAccessor;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import java.util.HashSet;
import java.util.Set;

/**
 * Per-row transform that rewrites BLOB columns from Lance's DESCRIPTOR shape into the Hudi BLOB
 * shape. Composed into {@link LanceRecordIterator} for DESCRIPTOR-mode reads.
 *
 * <p>In DESCRIPTOR mode Lance surfaces the blob-stream coordinates of INLINE payloads as a
 * {@code Struct<position, size>} in the BLOB {@code data} child instead of materializing the
 * bytes. For each row this transform:
 * <ul>
 *   <li>If {@code type == OUT_OF_LINE}, copies the existing {@code reference} through.</li>
 *   <li>If {@code type == INLINE}, reads {@code {position, size}} via {@link BlobStructAccessor}
 *       and synthesizes a reference whose {@code external_path} points at the current
 *       {@code .lance} file. The {@code type} is preserved as {@code INLINE} so downstream
 *       consumers see the original storage mode; {@code read_blob()} resolves both cases.</li>
 * </ul>
 */
public final class BlobDescriptorTransform {

  private static final UTF8String OUT_OF_LINE_UTF8 =
      UTF8String.fromString(HoodieSchema.Blob.OUT_OF_LINE);
  private static final UTF8String INLINE_UTF8 =
      UTF8String.fromString(HoodieSchema.Blob.INLINE);

  // Child field indices within the Hudi BLOB struct: {type(0), data(1), reference(2)}.
  private static final int TYPE_IDX = 0;
  private static final int DATA_IDX = 1;
  private static final int REF_IDX = 2;

  private final Set<String> blobFieldNames;
  private final UTF8String lanceFilePathUtf8;
  private final String lanceFilePath;

  private StructField[] outputFields;
  /** Column indices that are BLOB columns; populated once in {@link #init}. */
  private Set<Integer> blobColumnIndices;

  public BlobDescriptorTransform(Set<String> blobFieldNames, String lanceFilePath) {
    this.blobFieldNames = blobFieldNames;
    this.lanceFilePathUtf8 = UTF8String.fromString(lanceFilePath);
    this.lanceFilePath = lanceFilePath;
  }

  /**
   * Called once on the first batch to identify which column indices are BLOB columns.
   */
  void init(ColumnVector[] columnVectors, StructType sparkSchema) {
    this.outputFields = sparkSchema.fields();
    this.blobColumnIndices = new HashSet<>();
    for (int i = 0; i < outputFields.length; i++) {
      if (blobFieldNames.contains(outputFields[i].name())) {
        blobColumnIndices.add(i);
      }
    }
  }

  /**
   * Transform a single row, rewriting BLOB columns from Lance DESCRIPTOR shape into Hudi shape.
   *
   * @param row           the InternalRow from the current batch
   * @param rowId         row index within the batch; used only for {@link BlobStructAccessor}
   *                      access on INLINE rows
   * @param columnVectors current batch's column vectors; the {@link BlobStructAccessor} is
   *                      obtained fresh per row to avoid stale references across batches
   * @param projection    projection to convert to UnsafeRow
   */
  UnsafeRow transformRow(InternalRow row, int rowId, ColumnVector[] columnVectors,
                         UnsafeProjection projection) {
    Object[] rowBuffer = new Object[outputFields.length];
    for (int i = 0; i < outputFields.length; i++) {
      if (blobColumnIndices.contains(i)) {
        rowBuffer[i] = row.isNullAt(i) ? null
            : buildBlobOutputRow(row.getStruct(i, 3), columnVectors[i], rowId);
      } else {
        rowBuffer[i] = row.isNullAt(i) ? null : row.get(i, outputFields[i].dataType());
      }
    }
    return projection.apply(new GenericInternalRow(rowBuffer)).copy();
  }

  /**
   * Build one BLOB row from the blob struct obtained via the {@link InternalRow}.
   * OUT_OF_LINE source rows pass the existing reference through; INLINE source rows get a
   * synthesized reference from the Lance blob descriptor.
   *
   * @param blobStruct    the non-null BLOB struct ({type, data, reference}) from the current row
   * @param blobColumnVec the parent BLOB column vector for obtaining the {@link BlobStructAccessor}
   * @param rowId         row index within the batch, for {@link BlobStructAccessor} access
   */
  private InternalRow buildBlobOutputRow(InternalRow blobStruct, ColumnVector blobColumnVec,
                                         int rowId) {
    if (blobStruct.isNullAt(TYPE_IDX)) {
      throw new HoodieException("Malformed Lance BLOB row at rowId=" + rowId
          + " (file: " + lanceFilePath + "): payload struct is non-null but type is null");
    }
    UTF8String type = blobStruct.getUTF8String(TYPE_IDX);

    if (type.equals(OUT_OF_LINE_UTF8)) {
      InternalRow refRow = blobStruct.isNullAt(REF_IDX) ? null : blobStruct.getStruct(REF_IDX, 4);
      return new GenericInternalRow(new Object[] { OUT_OF_LINE_UTF8, null, refRow });
    }

    if (!type.equals(INLINE_UTF8)) {
      throw new HoodieException("Unexpected BLOB type '" + type + "' at rowId=" + rowId
          + " (file: " + lanceFilePath + "); expected INLINE or OUT_OF_LINE");
    }

    // INLINE source row — rewrite into a reference pointing at the .lance blob stream.
    // Defensively handle null data (e.g. a row with type=INLINE but null payload).
    if (blobStruct.isNullAt(DATA_IDX)) {
      return new GenericInternalRow(new Object[] { INLINE_UTF8, null, null });
    }

    // Obtain the accessor fresh from the current batch's column vector to avoid stale
    // references when Arrow reallocates buffers between batches.
    BlobStructAccessor accessor = ((LanceArrowColumnVector) blobColumnVec.getChild(DATA_IDX))
        .getBlobStructAccessor();
    long position = accessor.getPosition(rowId);
    long size = accessor.getSize(rowId);
    InternalRow refRow = new GenericInternalRow(
        new Object[] { lanceFilePathUtf8, position, size, Boolean.TRUE });
    return new GenericInternalRow(new Object[] { INLINE_UTF8, null, refRow });
  }
}

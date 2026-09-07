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

package org.apache.hudi.source;

import org.apache.hudi.common.blob.BlobRangeReader;
import org.apache.hudi.common.blob.BlobReadRequest;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A {@link ClosableIterator}{@code <RowData>} decorator that materializes out-of-line (OOL)
 * BLOB fields in batches.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Buffer up to {@code lookaheadSize} rows from the inner iterator (shallow-copied to
 *       prevent row-buffer aliasing in upstream iterators).
 *   <li>For every BLOB field in each row, check the {@code type} discriminator:
 *       {@code INLINE} passes through; {@code OUT_OF_LINE} is collected as a
 *       {@link BlobReadRequest} tagged with a {@code (rowIndex, blobFieldPosition)} key.
 *   <li>Delegate all I/O to {@link BlobRangeReader#readBatched}: it groups requests by file
 *       path, merges nearby ranges within {@code maxGapBytes}, and issues one
 *       seek+readFully per merged range.
 *   <li>Replace each materialized blob struct with {@code (INLINE, <bytes>, null)} and emit
 *       rows in their original order.
 * </ol>
 *
 * <p>Rows whose BLOB field is {@code null} or already {@code INLINE} are passed through unchanged.
 */
public class BlobMaterializingIterator implements ClosableIterator<RowData> {

  private final ClosableIterator<RowData> inner;
  private final HoodieStorage storage;
  private final int maxGapBytes;
  private final int lookaheadSize;

  // Primitive int[] follows the same convention as CopyOnWriteInputFormat.selectedFields and
  // RowDataProjection.positions — avoids Integer boxing in a per-row hot path.
  private final int[] blobFieldPositions;
  private final RowData.FieldGetter[] fieldGetters;
  private final int numFields;

  private final Deque<RowData> outputQueue = new ArrayDeque<>();

  /**
   * @param inner              raw iterator from the file group reader
   * @param requiredRowType    Flink RowType matching the rows emitted by {@code inner}
   * @param blobFieldPositions field indices in {@code requiredRowType} that are BLOB structs
   * @param storage            HoodieStorage instance for DFS I/O
   * @param maxGapBytes        max byte gap between two OOL ranges in the same file to merge
   * @param lookaheadSize      rows to buffer per batch
   */
  public BlobMaterializingIterator(
      ClosableIterator<RowData> inner,
      RowType requiredRowType,
      int[] blobFieldPositions,
      HoodieStorage storage,
      int maxGapBytes,
      int lookaheadSize) {
    this.inner = inner;
    this.storage = storage;
    this.maxGapBytes = maxGapBytes;
    this.lookaheadSize = lookaheadSize;
    this.blobFieldPositions = blobFieldPositions;
    this.numFields = requiredRowType.getFieldCount();
    this.fieldGetters = buildFieldGetters(requiredRowType);
  }

  @Override
  public boolean hasNext() {
    if (outputQueue.isEmpty()) {
      fillBatch();
    }
    return !outputQueue.isEmpty();
  }

  @Override
  public RowData next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return outputQueue.poll();
  }

  @Override
  public void close() {
    inner.close();
  }

  // -------------------------------------------------------------------------
  //  Batch fill
  // -------------------------------------------------------------------------

  private void fillBatch() {
    // Collect up to lookaheadSize rows; shallow-copy to guard against row-buffer reuse.
    List<RowData> lookaheadBuffer = new ArrayList<>(lookaheadSize);
    while (lookaheadBuffer.size() < lookaheadSize && inner.hasNext()) {
      lookaheadBuffer.add(shallowCopyRow(inner.next()));
    }
    if (lookaheadBuffer.isEmpty()) {
      return;
    }

    // Build BlobReadRequests for every OOL blob field in the lookahead buffer.
    List<BlobReadRequest<RowFieldKey>> readRequests = collectReadRequests(lookaheadBuffer);

    // Delegate all I/O to the common utility.
    Map<RowFieldKey, byte[]> dataMap = BlobRangeReader.readBatched(readRequests, storage, maxGapBytes);

    // Reconstruct rows, replacing OOL structs with INLINE ones; pass others through unchanged.
    for (int rowIndex = 0; rowIndex < lookaheadBuffer.size(); rowIndex++) {
      RowData row = lookaheadBuffer.get(rowIndex);
      if (!hasMaterializedBlob(rowIndex, dataMap)) {
        outputQueue.add(row);
        continue;
      }
      GenericRowData newRow = new GenericRowData(row.getRowKind(), numFields);
      for (int i = 0; i < numFields; i++) {
        newRow.setField(i, fieldGetters[i].getFieldOrNull(row));
      }
      for (int blobFieldPosition : blobFieldPositions) {
        byte[] data = dataMap.get(new RowFieldKey(rowIndex, blobFieldPosition));
        if (data != null) {
          newRow.setField(blobFieldPosition, buildInlineBlob(data));
        }
      }
      outputQueue.add(newRow);
    }
  }

  private boolean hasMaterializedBlob(int rowIndex, Map<RowFieldKey, byte[]> dataMap) {
    for (int blobFieldPosition : blobFieldPositions) {
      if (dataMap.containsKey(new RowFieldKey(rowIndex, blobFieldPosition))) {
        return true;
      }
    }
    return false;
  }

  private List<BlobReadRequest<RowFieldKey>> collectReadRequests(List<RowData> lookaheadBuffer) {
    List<BlobReadRequest<RowFieldKey>> requests = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < lookaheadBuffer.size(); rowIndex++) {
      RowData row = lookaheadBuffer.get(rowIndex);
      for (int blobFieldPosition : blobFieldPositions) {
        if (row.isNullAt(blobFieldPosition)) {
          continue;
        }
        RowData blobRow = row.getRow(blobFieldPosition, HoodieSchema.Blob.getFieldCount());
        if (blobRow == null || blobRow.isNullAt(0)) {
          continue;
        }
        String storageType = blobRow.getString(0).toString();
        if (!HoodieSchema.Blob.OUT_OF_LINE.equals(storageType)) {
          continue;
        }
        if (blobRow.isNullAt(2)) {
          throw new IllegalStateException(
              "OUT_OF_LINE blob is missing its reference struct at row " + rowIndex);
        }
        RowData refRow = blobRow.getRow(2, HoodieSchema.Blob.getReferenceFieldCount());
        String externalPath = refRow.getString(0).toString();
        boolean noOffset = refRow.isNullAt(1);
        boolean noLength = refRow.isNullAt(2);
        RowFieldKey key = new RowFieldKey(rowIndex, blobFieldPosition);
        if (noOffset && noLength) {
          requests.add(BlobReadRequest.wholeFile(externalPath, key));
        } else if (noOffset || noLength) {
          throw new IllegalArgumentException(
              "Blob reference for '" + externalPath
                  + "' must set both offset and length, or neither");
        } else {
          requests.add(BlobReadRequest.range(externalPath, refRow.getLong(1), refRow.getLong(2), key));
        }
      }
    }
    return requests;
  }

  // -------------------------------------------------------------------------
  //  Row helpers
  // -------------------------------------------------------------------------

  /**
   * Shallow-copies {@code row} into a {@link GenericRowData} so that upstream iterators that
   * reuse a single row buffer cannot alias entries in our lookahead batch.
   */
  private RowData shallowCopyRow(RowData row) {
    GenericRowData copy = new GenericRowData(row.getRowKind(), numFields);
    for (int i = 0; i < numFields; i++) {
      copy.setField(i, fieldGetters[i].getFieldOrNull(row));
    }
    return copy;
  }

  private static GenericRowData buildInlineBlob(byte[] data) {
    GenericRowData blob = new GenericRowData(HoodieSchema.Blob.getFieldCount());
    blob.setField(0, StringData.fromString(HoodieSchema.Blob.INLINE));
    blob.setField(1, data);
    blob.setField(2, null);
    return blob;
  }

  /**
   * Pre-computes one {@link RowData.FieldGetter} per field for cheap, type-safe extraction
   * without a runtime switch over every {@link LogicalType} in {@link #shallowCopyRow}.
   *
   * <p>This follows the same pattern as {@link org.apache.hudi.util.RowDataProjection}, which
   * centralises field-getter construction for projection in the Flink connector; we replicate
   * it here so {@code BlobMaterializingIterator} has no dependency on a Flink-specific utility
   * class from a different package and remains self-contained.
   */
  private static RowData.FieldGetter[] buildFieldGetters(RowType rowType) {
    List<LogicalType> children = rowType.getChildren();
    RowData.FieldGetter[] getters = new RowData.FieldGetter[children.size()];
    for (int i = 0; i < children.size(); i++) {
      getters[i] = RowData.createFieldGetter(children.get(i), i);
    }
    return getters;
  }

  /**
   * Identifies a single blob field within the lookahead batch by its row index and field position.
   * Used as the tag type for {@link BlobReadRequest} so results can be correlated back to the
   * correct cell after I/O completes.
   */
  private static final class RowFieldKey {
    final int rowIndex;
    final int blobFieldPosition;

    RowFieldKey(int rowIndex, int blobFieldPosition) {
      this.rowIndex = rowIndex;
      this.blobFieldPosition = blobFieldPosition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowFieldKey)) {
        return false;
      }
      RowFieldKey other = (RowFieldKey) o;
      return rowIndex == other.rowIndex && blobFieldPosition == other.blobFieldPosition;
    }

    @Override
    public int hashCode() {
      return Objects.hash(rowIndex, blobFieldPosition);
    }
  }
}

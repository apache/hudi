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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BlobMaterializingIterator}.
 *
 * <p>Each test builds a small RowData schema with one BLOB field (field index 2) and wires up a
 * mock {@link HoodieStorage} to assert that:
 * <ul>
 *   <li>INLINE blobs pass through unchanged.
 *   <li>OOL range-refs are batch-read and materialized as INLINE.
 *   <li>Nearby ranges within {@code maxGapBytes} are merged into a single read.
 *   <li>Whole-file OOL refs are read via {@link HoodieStorage#open(StoragePath)}.
 *   <li>Null blob fields pass through unchanged.
 *   <li>Original row order is preserved.
 * </ul>
 */
public class TestBlobMaterializingIterator {

  // -------------------------------------------------------------------------
  //  Schema helpers
  // -------------------------------------------------------------------------

  /**
   * Builds a RowType with fields: [id BIGINT, name VARCHAR, blob_col BLOB_ROW_TYPE, ts BIGINT].
   * The blob_col sub-type is the 3-field struct: (type VARCHAR, data VARBINARY, reference ROW(...)).
   */
  private static RowType buildRowType() {
    RowType referenceType = RowType.of(
        new LogicalType[] {
            new VarCharType(Integer.MAX_VALUE),
            new BigIntType(),
            new BigIntType(),
            new BooleanType()
        },
        new String[] {"external_path", "offset", "length", "managed"}
    );
    RowType blobType = RowType.of(
        new LogicalType[] {
            new VarCharType(Integer.MAX_VALUE),
            new VarBinaryType(Integer.MAX_VALUE),
            referenceType
        },
        new String[] {"type", "data", "reference"}
    );
    return RowType.of(
        new LogicalType[] {
            new BigIntType(),
            new VarCharType(Integer.MAX_VALUE),
            blobType,
            new BigIntType()
        },
        new String[] {"id", "name", "blob_col", "ts"}
    );
  }

  private static final int BLOB_FIELD_POS = 2;
  private static final int[] BLOB_POSITIONS = {BLOB_FIELD_POS};

  // -------------------------------------------------------------------------
  //  Row-building helpers
  // -------------------------------------------------------------------------

  private static RowData blobRowWithOolRange(long id, String name, String path, long offset, long length) {
    GenericRowData refRow = new GenericRowData(HoodieSchema.Blob.getReferenceFieldCount());
    refRow.setField(0, StringData.fromString(path));
    refRow.setField(1, offset);
    refRow.setField(2, length);
    refRow.setField(3, false);

    GenericRowData blobRow = new GenericRowData(HoodieSchema.Blob.getFieldCount());
    blobRow.setField(0, StringData.fromString(HoodieSchema.Blob.OUT_OF_LINE));
    blobRow.setField(1, null);
    blobRow.setField(2, refRow);

    GenericRowData row = new GenericRowData(4);
    row.setField(0, id);
    row.setField(1, StringData.fromString(name));
    row.setField(2, blobRow);
    row.setField(3, id * 1000L);
    return row;
  }

  private static RowData blobRowWithInline(long id, String name, byte[] data) {
    GenericRowData blobRow = new GenericRowData(HoodieSchema.Blob.getFieldCount());
    blobRow.setField(0, StringData.fromString(HoodieSchema.Blob.INLINE));
    blobRow.setField(1, data);
    blobRow.setField(2, null);

    GenericRowData row = new GenericRowData(4);
    row.setField(0, id);
    row.setField(1, StringData.fromString(name));
    row.setField(2, blobRow);
    row.setField(3, id * 1000L);
    return row;
  }

  private static RowData blobRowWithNullBlob(long id) {
    GenericRowData row = new GenericRowData(4);
    row.setField(0, id);
    row.setField(1, StringData.fromString("null-blob"));
    row.setField(2, null);
    row.setField(3, 999L);
    return row;
  }

  // -------------------------------------------------------------------------
  //  Tests
  // -------------------------------------------------------------------------

  @Test
  public void testInlineBlobPassesThrough() throws IOException {
    byte[] data = {1, 2, 3};
    RowData inlineRow = blobRowWithInline(1L, "doc", data);

    HoodieStorage storage = mock(HoodieStorage.class);
    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(inlineRow), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    List<RowData> results = drain(iter);
    assertEquals(1, results.size());
    RowData blobStruct = results.get(0).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount());
    assertEquals(HoodieSchema.Blob.INLINE, blobStruct.getString(0).toString());
    assertArrayEquals(data, blobStruct.getBinary(1));
  }

  @Test
  public void testNullBlobPassesThrough() throws IOException {
    RowData nullRow = blobRowWithNullBlob(5L);
    HoodieStorage storage = mock(HoodieStorage.class);
    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(nullRow), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    List<RowData> results = drain(iter);
    assertEquals(1, results.size());
    assertNull(results.get(0).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()));
  }

  @Test
  public void testSingleOolRangeIsMaterialized() throws IOException {
    byte[] fileContent = new byte[200];
    for (int i = 0; i < fileContent.length; i++) {
      fileContent[i] = (byte) i;
    }
    RowData oolRow = blobRowWithOolRange(1L, "doc", "file1.bin", 10, 50);

    HoodieStorage storage = mock(HoodieStorage.class);
    SeekableDataInputStream seekable = makeSeekableStream(fileContent);
    when(storage.openSeekable(any(StoragePath.class), eq(false))).thenReturn(seekable);

    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(oolRow), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    List<RowData> results = drain(iter);
    assertEquals(1, results.size());
    RowData blobStruct = results.get(0).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount());
    assertEquals(HoodieSchema.Blob.INLINE, blobStruct.getString(0).toString());
    assertArrayEquals(
        Arrays.copyOfRange(fileContent, 10, 60),
        blobStruct.getBinary(1));
  }

  @Test
  public void testTwoNearbyRangesAreMergedIntoOnePread() throws IOException {
    // Two refs in the same file at offsets 0 and 50, gap=50 within maxGapBytes=100.
    byte[] fileContent = new byte[200];
    for (int i = 0; i < fileContent.length; i++) {
      fileContent[i] = (byte) (i + 1);
    }
    RowData row1 = blobRowWithOolRange(1L, "a", "file1.bin", 0, 30);
    RowData row2 = blobRowWithOolRange(2L, "b", "file1.bin", 50, 40);

    HoodieStorage storage = mock(HoodieStorage.class);
    SeekableDataInputStream seekable = spy(makeSeekableStream(fileContent));
    when(storage.openSeekable(any(StoragePath.class), eq(false))).thenReturn(seekable);

    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(row1, row2), buildRowType(), BLOB_POSITIONS, storage, 100, 10);

    List<RowData> results = drain(iter);
    assertEquals(2, results.size());

    assertArrayEquals(Arrays.copyOfRange(fileContent, 0, 30),
        results.get(0).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()).getBinary(1));
    assertArrayEquals(Arrays.copyOfRange(fileContent, 50, 90),
        results.get(1).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()).getBinary(1));

    // Both refs were merged → only one openSeekable call.
    verify(storage, times(1)).openSeekable(any(StoragePath.class), eq(false));
  }

  @Test
  public void testTwoFarRangesAreNotMerged() throws IOException {
    byte[] fileContent = new byte[500];
    RowData row1 = blobRowWithOolRange(1L, "a", "file1.bin", 0, 10);
    RowData row2 = blobRowWithOolRange(2L, "b", "file1.bin", 400, 10);

    HoodieStorage storage = mock(HoodieStorage.class);
    // Use a fresh seekable stream each call since seek() moves the position.
    when(storage.openSeekable(any(StoragePath.class), eq(false)))
        .thenReturn(makeSeekableStream(fileContent));

    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(row1, row2), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    List<RowData> results = drain(iter);
    assertEquals(2, results.size());
  }

  @Test
  public void testOriginalRowOrderPreserved() throws IOException {
    // Three OOL refs from two different files; rows should come back in original (insertion) order.
    byte[] fileA = new byte[100];
    byte[] fileB = new byte[100];
    Arrays.fill(fileA, (byte) 'A');
    Arrays.fill(fileB, (byte) 'B');

    RowData row1 = blobRowWithOolRange(1L, "a", "fileA.bin", 0, 10);
    RowData row2 = blobRowWithOolRange(2L, "b", "fileB.bin", 0, 10);
    RowData row3 = blobRowWithOolRange(3L, "c", "fileA.bin", 20, 10);

    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.openSeekable(eq(new StoragePath("fileA.bin")), eq(false)))
        .thenReturn(makeSeekableStream(fileA));
    when(storage.openSeekable(eq(new StoragePath("fileB.bin")), eq(false)))
        .thenReturn(makeSeekableStream(fileB));

    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(row1, row2, row3), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    List<RowData> results = drain(iter);
    assertEquals(3, results.size());
    // Row order preserved.
    assertEquals(1L, results.get(0).getLong(0));
    assertEquals(2L, results.get(1).getLong(0));
    assertEquals(3L, results.get(2).getLong(0));
    // Bytes correct.
    assertArrayEquals(Arrays.copyOfRange(fileA, 0, 10),
        results.get(0).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()).getBinary(1));
    assertArrayEquals(Arrays.copyOfRange(fileB, 0, 10),
        results.get(1).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()).getBinary(1));
    assertArrayEquals(Arrays.copyOfRange(fileA, 20, 30),
        results.get(2).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()).getBinary(1));
  }

  @Test
  public void testMixedInlineAndOolInSameBatch() throws IOException {
    byte[] inlineData = {9, 8, 7};
    byte[] fileContent = new byte[100];
    Arrays.fill(fileContent, (byte) 42);

    RowData inlineRow = blobRowWithInline(1L, "inline", inlineData);
    RowData oolRow = blobRowWithOolRange(2L, "ool", "file.bin", 5, 10);
    RowData nullRow = blobRowWithNullBlob(3L);

    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.openSeekable(any(StoragePath.class), eq(false)))
        .thenReturn(makeSeekableStream(fileContent));

    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(inlineRow, oolRow, nullRow), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    List<RowData> results = drain(iter);
    assertEquals(3, results.size());

    // inline: unchanged bytes
    assertArrayEquals(inlineData,
        results.get(0).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()).getBinary(1));
    // ool: materialized
    assertArrayEquals(Arrays.copyOfRange(fileContent, 5, 15),
        results.get(1).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()).getBinary(1));
    // null: blob field is null
    assertNull(results.get(2).getRow(BLOB_FIELD_POS, HoodieSchema.Blob.getFieldCount()));
  }

  @Test
  public void testNonBlobFieldsArePreserved() throws IOException {
    byte[] fileContent = new byte[50];
    RowData oolRow = blobRowWithOolRange(42L, "preserved-name", "file.bin", 0, 10);

    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.openSeekable(any(StoragePath.class), eq(false)))
        .thenReturn(makeSeekableStream(fileContent));

    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(oolRow), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    List<RowData> results = drain(iter);
    assertEquals(1, results.size());
    assertEquals(42L, results.get(0).getLong(0));
    assertEquals("preserved-name", results.get(0).getString(1).toString());
    assertEquals(42_000L, results.get(0).getLong(3));
  }

  @Test
  public void testEmptyInputProducesNoRows() {
    HoodieStorage storage = mock(HoodieStorage.class);
    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    assertFalse(iter.hasNext());
  }

  @Test
  public void testMissingReferenceThrowsOnOolBlob() {
    // OOL blob with null reference struct should throw IllegalStateException.
    GenericRowData blobRow = new GenericRowData(HoodieSchema.Blob.getFieldCount());
    blobRow.setField(0, StringData.fromString(HoodieSchema.Blob.OUT_OF_LINE));
    blobRow.setField(1, null);
    blobRow.setField(2, null); // reference is null → error

    GenericRowData row = new GenericRowData(4);
    row.setField(0, 1L);
    row.setField(1, StringData.fromString("x"));
    row.setField(2, blobRow);
    row.setField(3, 1000L);

    HoodieStorage storage = mock(HoodieStorage.class);
    BlobMaterializingIterator iter = new BlobMaterializingIterator(
        listIterator(row), buildRowType(), BLOB_POSITIONS, storage, 0, 10);

    assertThrows(IllegalStateException.class, iter::hasNext);
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private static ClosableIterator<RowData> listIterator(RowData... rows) {
    List<RowData> list = Arrays.asList(rows);
    return new ClosableIterator<RowData>() {
      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < list.size();
      }

      @Override
      public RowData next() {
        return list.get(idx++);
      }

      @Override
      public void close() {
      }
    };
  }

  private static List<RowData> drain(BlobMaterializingIterator iter) {
    List<RowData> result = new ArrayList<>();
    while (iter.hasNext()) {
      result.add(iter.next());
    }
    iter.close();
    return result;
  }

  /**
   * Returns a {@link SeekableDataInputStream} backed by {@code content}.
   * Supports {@code seek()} by re-creating the ByteArrayInputStream at the given offset.
   */
  private static SeekableDataInputStream makeSeekableStream(byte[] content) {
    return new SeekableDataInputStream(new ByteArrayInputStream(content)) {
      private int pos = 0;

      @Override
      public long getPos() {
        return pos;
      }

      @Override
      public void seek(long newPos) {
        this.pos = (int) newPos;
        this.in = new ByteArrayInputStream(content, this.pos, content.length - this.pos);
      }
    };
  }
}

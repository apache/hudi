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

package org.apache.hudi.common.blob;

import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BlobRangeReader} and {@link BlobReadRequest}.
 *
 * <p>I/O is driven through a mock {@link HoodieStorage} backed by in-memory byte arrays.
 */
public class TestBlobRangeReader {

  // -------------------------------------------------------------------------
  //  groupSortMerge tests
  // -------------------------------------------------------------------------

  @Test
  public void testGroupSortMergeEmptyList() {
    List<BlobRangeReader.MergedRange<Integer>> result = BlobRangeReader.groupSortMerge(
        Collections.emptyList(), 0);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGroupSortMergeAdjacentRangesMergedWhenGapIsZero() {
    // [0, 10) and [10, 20) — zero gap → merged into [0, 20)
    List<BlobReadRequest<Integer>> reqs = Arrays.asList(
        BlobReadRequest.range("f.bin", 0, 10, 1),
        BlobReadRequest.range("f.bin", 10, 10, 2)
    );
    List<BlobRangeReader.MergedRange<Integer>> merged = BlobRangeReader.groupSortMerge(reqs, 0);
    assertEquals(1, merged.size());
    assertEquals(0L, merged.get(0).startOffset);
    assertEquals(20L, merged.get(0).endOffset);
    assertEquals(2, merged.get(0).requests.size());
  }

  @Test
  public void testGroupSortMergeRangesWithinMaxGapAreMerged() {
    // [0, 10) and [15, 25) — gap = 5 ≤ maxGapBytes = 10 → merged into [0, 25)
    List<BlobReadRequest<Integer>> reqs = Arrays.asList(
        BlobReadRequest.range("f.bin", 0, 10, 1),
        BlobReadRequest.range("f.bin", 15, 10, 2)
    );
    List<BlobRangeReader.MergedRange<Integer>> merged = BlobRangeReader.groupSortMerge(reqs, 10);
    assertEquals(1, merged.size());
    assertEquals(0L, merged.get(0).startOffset);
    assertEquals(25L, merged.get(0).endOffset);
  }

  @Test
  public void testGroupSortMergeRangesExceedingMaxGapAreNotMerged() {
    // [0, 10) and [50, 60) — gap = 40 > maxGapBytes = 10 → two separate ranges
    List<BlobReadRequest<Integer>> reqs = Arrays.asList(
        BlobReadRequest.range("f.bin", 0, 10, 1),
        BlobReadRequest.range("f.bin", 50, 10, 2)
    );
    List<BlobRangeReader.MergedRange<Integer>> merged = BlobRangeReader.groupSortMerge(reqs, 10);
    assertEquals(2, merged.size());
    assertEquals(0L, merged.get(0).startOffset);
    assertEquals(50L, merged.get(1).startOffset);
  }

  @Test
  public void testGroupSortMergeUnsortedInputIsSortedByOffset() {
    // Requests intentionally out of order; they should be sorted before merging.
    List<BlobReadRequest<Integer>> reqs = Arrays.asList(
        BlobReadRequest.range("f.bin", 20, 10, 2),
        BlobReadRequest.range("f.bin", 0, 10, 1)
    );
    List<BlobRangeReader.MergedRange<Integer>> merged = BlobRangeReader.groupSortMerge(reqs, 5);
    assertEquals(2, merged.size());
    assertEquals(0L, merged.get(0).startOffset);
    assertEquals(20L, merged.get(1).startOffset);
  }

  @Test
  public void testGroupSortMergeSeparateFilesProduceSeparateRanges() {
    List<BlobReadRequest<Integer>> reqs = Arrays.asList(
        BlobReadRequest.range("a.bin", 0, 10, 1),
        BlobReadRequest.range("b.bin", 0, 10, 2)
    );
    List<BlobRangeReader.MergedRange<Integer>> merged = BlobRangeReader.groupSortMerge(reqs, 1000);
    // Even with a huge maxGap, different files must not be merged.
    assertEquals(2, merged.size());
  }

  @Test
  public void testGroupSortMergeOverlappingRangesThrow() {
    // [0, 20) and [10, 30) overlap → should throw
    List<BlobReadRequest<Integer>> reqs = Arrays.asList(
        BlobReadRequest.range("f.bin", 0, 20, 1),
        BlobReadRequest.range("f.bin", 10, 20, 2)
    );
    assertThrows(IllegalArgumentException.class,
        () -> BlobRangeReader.groupSortMerge(reqs, 0));
  }

  // -------------------------------------------------------------------------
  //  readBatched tests
  // -------------------------------------------------------------------------

  @Test
  public void testReadBatchedEmptyReturnsEmptyMap() throws IOException {
    HoodieStorage storage = mock(HoodieStorage.class);
    Map<Integer, byte[]> result = BlobRangeReader.readBatched(
        Collections.emptyList(), storage, 0);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testReadBatchedSingleRangeReadsCorrectBytes() throws IOException {
    byte[] fileContent = new byte[100];
    for (int i = 0; i < fileContent.length; i++) {
      fileContent[i] = (byte) i;
    }
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.openSeekable(any(StoragePath.class), eq(false)))
        .thenReturn(makeSeekable(fileContent));

    Map<Integer, byte[]> result = BlobRangeReader.readBatched(
        Collections.singletonList(BlobReadRequest.range("f.bin", 10, 20, 42)),
        storage, 0);

    assertEquals(1, result.size());
    assertArrayEquals(Arrays.copyOfRange(fileContent, 10, 30), result.get(42));
  }

  @Test
  public void testReadBatchedTwoNearbyRangesOnlyOneSeek() throws IOException {
    byte[] fileContent = new byte[200];
    HoodieStorage storage = mock(HoodieStorage.class);
    SeekableDataInputStream stream = makeSeekable(fileContent);
    when(storage.openSeekable(any(StoragePath.class), eq(false))).thenReturn(stream);

    List<BlobReadRequest<Integer>> reqs = Arrays.asList(
        BlobReadRequest.range("f.bin", 0, 30, 1),
        BlobReadRequest.range("f.bin", 50, 40, 2)
    );
    Map<Integer, byte[]> result = BlobRangeReader.readBatched(reqs, storage, 100);

    assertEquals(2, result.size());
    assertArrayEquals(Arrays.copyOfRange(fileContent, 0, 30), result.get(1));
    assertArrayEquals(Arrays.copyOfRange(fileContent, 50, 90), result.get(2));
    // Both ranges merged → one openSeekable call
    verify(storage, times(1)).openSeekable(any(StoragePath.class), eq(false));
  }

  @Test
  public void testReadBatchedWholeFileUsesOpen() throws IOException {
    byte[] fileContent = {10, 20, 30};
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.open(any(StoragePath.class)))
        .thenReturn(new ByteArrayInputStream(fileContent));

    Map<Integer, byte[]> result = BlobRangeReader.readBatched(
        Collections.singletonList(BlobReadRequest.wholeFile("f.bin", 99)),
        storage, 0);

    assertEquals(1, result.size());
    assertArrayEquals(fileContent, result.get(99));
    verify(storage, times(1)).open(any(StoragePath.class));
  }

  @Test
  public void testReadBatchedDifferentFilesOneSeekEach() throws IOException {
    byte[] fa = new byte[50];
    byte[] fb = new byte[50];
    Arrays.fill(fa, (byte) 'A');
    Arrays.fill(fb, (byte) 'B');

    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.openSeekable(eq(new StoragePath("a.bin")), eq(false))).thenReturn(makeSeekable(fa));
    when(storage.openSeekable(eq(new StoragePath("b.bin")), eq(false))).thenReturn(makeSeekable(fb));

    List<BlobReadRequest<String>> reqs = Arrays.asList(
        BlobReadRequest.range("a.bin", 0, 10, "a"),
        BlobReadRequest.range("b.bin", 5, 10, "b")
    );
    Map<String, byte[]> result = BlobRangeReader.readBatched(reqs, storage, 0);

    assertArrayEquals(Arrays.copyOfRange(fa, 0, 10), result.get("a"));
    assertArrayEquals(Arrays.copyOfRange(fb, 5, 15), result.get("b"));
    verify(storage, times(1)).openSeekable(eq(new StoragePath("a.bin")), eq(false));
    verify(storage, times(1)).openSeekable(eq(new StoragePath("b.bin")), eq(false));
  }

  // -------------------------------------------------------------------------
  //  BlobReadRequest tests
  // -------------------------------------------------------------------------

  @Test
  public void testWholeFileRequestIsWholeFile() {
    BlobReadRequest<String> req = BlobReadRequest.wholeFile("f.bin", "tag");
    assertTrue(req.isWholeFile());
    assertEquals("f.bin", req.filePath);
    assertEquals("tag", req.tag);
  }

  @Test
  public void testRangeRequestNegativeOffsetThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> BlobReadRequest.range("f.bin", -1, 10, "x"));
  }

  @Test
  public void testRangeRequestNegativeLengthThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> BlobReadRequest.range("f.bin", 0, -5, "x"));
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private static SeekableDataInputStream makeSeekable(byte[] content) {
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

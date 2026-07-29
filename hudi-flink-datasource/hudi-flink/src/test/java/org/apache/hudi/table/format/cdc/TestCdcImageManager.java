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

package org.apache.hudi.table.format.cdc;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.format.FormatUtils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link CdcImageManager}.
 */
class TestCdcImageManager {

  @Test
  void testImageRecordLifecycle() {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    CdcImageManager imageManager = new CdcImageManager(
        rowType("value"),
        writeConfig,
        split -> {
          throw new AssertionError("No split should be loaded by this test");
        });
    ExternalSpillableMap<String, byte[]> imageCache = mockImageCache();

    GenericRowData original = GenericRowData.of(StringData.fromString("before"));
    imageManager.updateImageRecord("key-1", imageCache, original);

    RowData image = imageManager.getImageRecord("key-1", imageCache, RowKind.UPDATE_BEFORE);
    assertEquals(RowKind.UPDATE_BEFORE, image.getRowKind());
    assertEquals("before", image.getString(0).toString());

    RowData removed = imageManager.removeImageRecord("key-1", imageCache);
    assertEquals("before", removed.getString(0).toString());
    assertNull(imageManager.removeImageRecord("key-1", imageCache));
    assertThrows(
        IllegalStateException.class,
        () -> imageManager.getImageRecord("missing", imageCache, RowKind.DELETE));
    assertSame(writeConfig, imageManager.getWriteConfig());

    imageManager.close();
  }

  @Test
  void testDataViewAdapters() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    CdcImageManager.BytesArrayOutputView outputView =
        new CdcImageManager.BytesArrayOutputView(outputStream);
    outputView.writeByte(7);
    outputView.skipBytesToWrite(2);
    outputView.write(new CdcImageManager.BytesArrayInputView(new byte[] {8, 9}), 2);
    outputView.flush();

    assertArrayEquals(new byte[] {7, 0, 0, 8, 9}, outputStream.toByteArray());

    CdcImageManager.BytesArrayInputView inputView =
        new CdcImageManager.BytesArrayInputView(outputStream.toByteArray());
    inputView.skipBytesToRead(3);
    assertEquals(8, inputView.readByte());
    assertEquals(9, inputView.readUnsignedByte());
  }

  @Test
  void testImageCacheReuseEvictionAndClose() throws IOException {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getBasePath()).thenReturn("/table");
    ExternalSpillableMap<String, byte[]> first = mockImageCache();
    ExternalSpillableMap<String, byte[]> second = mockImageCache();
    ExternalSpillableMap<String, byte[]> third = mockImageCache();
    GenericRowData row = GenericRowData.of(
        StringData.fromString("commit"),
        StringData.fromString("seq"),
        StringData.fromString("key-1"));
    CdcImageManager imageManager = new CdcImageManager(
        RowType.of(
            new LogicalType[] {new VarCharType(), new VarCharType(), new VarCharType()},
            new String[] {"commit", "seq", "record_key"}),
        writeConfig,
        split -> ClosableIterator.wrap(List.<RowData>of(row).iterator()));

    try (MockedStatic<FormatUtils> mockedFormatUtils = mockStatic(FormatUtils.class)) {
      mockedFormatUtils.when(() -> FormatUtils.spillableMap(
          writeConfig, 1024L, CdcImageManager.class.getSimpleName()))
          .thenReturn(first, second, third);

      FileSlice slice1 = fileSlice("001");
      FileSlice slice2 = fileSlice("002");
      FileSlice slice3 = fileSlice("003");
      assertSame(first, imageManager.getOrLoadImages(1024L, slice1));
      assertSame(first, imageManager.getOrLoadImages(1024L, slice1));
      assertSame(second, imageManager.getOrLoadImages(1024L, slice2));
      assertSame(third, imageManager.getOrLoadImages(1024L, slice3));
      verify(first).close();
      verify(first).put(anyString(), any(byte[].class));

      imageManager.close();
      verify(second).close();
      verify(third).close();
      imageManager.close();
      verify(second, times(1)).close();
    }
  }

  @SuppressWarnings("unchecked")
  private static ExternalSpillableMap<String, byte[]> mockImageCache() {
    ExternalSpillableMap<String, byte[]> imageCache = mock(ExternalSpillableMap.class);
    Map<String, byte[]> records = new HashMap<>();
    when(imageCache.get(anyString())).thenAnswer(
        invocation -> records.get(invocation.getArgument(0)));
    when(imageCache.put(anyString(), any(byte[].class))).thenAnswer(
        invocation -> records.put(invocation.getArgument(0), invocation.getArgument(1)));
    when(imageCache.remove(anyString())).thenAnswer(
        invocation -> records.remove(invocation.getArgument(0)));
    return imageCache;
  }

  private static RowType rowType(String fieldName) {
    return RowType.of(
        new LogicalType[] {new VarCharType()},
        new String[] {fieldName});
  }

  private static FileSlice fileSlice(String instant) {
    return new FileSlice(
        new HoodieFileGroupId("partition", "file"), instant, null, java.util.Collections.emptyList());
  }
}

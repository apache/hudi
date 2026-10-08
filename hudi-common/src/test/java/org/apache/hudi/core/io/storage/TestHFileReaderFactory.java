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

package org.apache.hudi.core.io.storage;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.CachingHFileReaderImpl;
import org.apache.hudi.io.hfile.HFileContext;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderCacheManager;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.io.hfile.HFileWriter;
import org.apache.hudi.io.hfile.HFileWriterImpl;
import org.apache.hudi.io.hfile.KeyValue;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.hudi.io.hfile.HFileByteUtils.getValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestHFileReaderFactory {

  @Mock
  private HoodieStorage mockStorage;

  @Mock
  private StoragePath mockPath;

  @Mock
  private StoragePathInfo mockPathInfo;

  @Mock
  private SeekableDataInputStream mockInputStream;

  private TypedProperties properties;
  private final byte[] testContent = "test content".getBytes();

  @BeforeEach
  @AfterEach
  void setUp() {
    properties = new TypedProperties();
    properties.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), "false");
    CachingHFileReaderImpl.resetGlobalCache();
  }

  @Test
  void testCreateHFileReader_FileSizeBelowThreshold_ShouldUseContentCache() throws IOException {
    final long fileSizeBelow = 5L; // 5 bytes - below default threshold
    final int thresholdMB = 10; // 10MB threshold

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_MAX_SIZE_MB.key(), String.valueOf(thresholdMB));

    when(mockStorage.getPathInfo(mockPath)).thenReturn(mockPathInfo);
    when(mockPathInfo.getLength()).thenReturn(fileSizeBelow);
    when(mockStorage.openSeekable(mockPath, false)).thenReturn(mockInputStream);
    doAnswer(invocation -> {
      byte[] buffer = invocation.getArgument(0);
      System.arraycopy(testContent, 0, buffer, 0, Math.min(testContent.length, buffer.length));
      return null;
    }).when(mockInputStream).readFully(any(byte[].class));

    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();
    HFileReader result = factory.createHFileReader();

    assertNotNull(result);
    assertInstanceOf(HFileReaderImpl.class, result);

    // Verify that content was downloaded (cache was used)
    verify(mockStorage, times(1)).getPathInfo(mockPath); // Once for size determination, which is reused for download
    verify(mockStorage, times(1)).openSeekable(mockPath, false); // For content download
    verify(mockInputStream, times(1)).readFully(any(byte[].class));
  }

  @Test
  void testCreateHFileReader_FileSizeAboveThreshold_ShouldNotUseContentCache() throws IOException {
    final long fileSizeAbove = 15L * 1024L * 1024L; // 15MB - above 10MB threshold
    final int thresholdMB = 10; // 10MB threshold

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_MAX_SIZE_MB.key(), String.valueOf(thresholdMB));

    when(mockStorage.getPathInfo(mockPath)).thenReturn(mockPathInfo);
    when(mockPathInfo.getLength()).thenReturn(fileSizeAbove);
    when(mockStorage.openSeekable(mockPath, false)).thenReturn(mockInputStream);

    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();
    HFileReader result = factory.createHFileReader();

    assertNotNull(result);
    assertInstanceOf(HFileReaderImpl.class, result);

    // Verify that content was NOT downloaded (cache was not used)
    verify(mockStorage, times(1)).getPathInfo(mockPath); // Only once for size determination
    verify(mockStorage, times(1)).openSeekable(mockPath, false); // For creating input stream directly
    verify(mockInputStream, never()).readFully(any(byte[].class)); // Content not downloaded
  }

  @Test
  void testCreateHFileReader_ContentProvidedInConstructor_ShouldUseProvidedContent() throws IOException {
    final int thresholdMB = 10; // 10MB threshold

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_MAX_SIZE_MB.key(), String.valueOf(thresholdMB));

    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withContent(testContent)
        .build();
    HFileReader result = factory.createHFileReader();

    assertNotNull(result);
    assertInstanceOf(HFileReaderImpl.class, result);

    // Verify that storage was never accessed since content was provided
    verify(mockStorage, never()).getPathInfo(any());
    verify(mockStorage, never()).openSeekable(any(), anyBoolean());
  }

  @Test
  void testCreateHFileReader_ContentProvidedAndPathProvided_ShouldFail() throws IOException {
    final int thresholdMB = 10;

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_MAX_SIZE_MB.key(), String.valueOf(thresholdMB));

    IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .withContent(testContent)
        .build());
    assertEquals("HFile source already set, cannot set bytes content", exception.getMessage());

    exception = Assertions.assertThrows(IllegalStateException.class, () -> HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withContent(testContent)
        .withPath(mockPath)
        .build());
    assertEquals("HFile source already set, cannot set path", exception.getMessage());
  }

  @Test
  void testCreateHFileReader_NoPathOrContent_ShouldThrowException() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      HFileReaderFactory.builder()
          .withStorage(mockStorage)
          .withProps(properties)
          .build();
    });
    assertEquals("HFile source cannot be null", exception.getMessage());
  }

  @Test
  void testBuilder_WithNullStorage_ShouldThrowException() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      HFileReaderFactory.builder()
          .withStorage(null)
          .withPath(mockPath)
          .build();
    });
    assertEquals("Storage cannot be null", exception.getMessage());
  }

  @Test
  void testBuilder_WithoutPropertiesProvided_ShouldCreateCachingReaderWithoutStorageAccess() throws IOException {
    // Not providing properties, should use defaults
    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withPath(mockPath)
        .build();

    HFileReader result = factory.createHFileReader();
    assertInstanceOf(CachingHFileReaderImpl.class, result);
    verifyNoInteractions(mockStorage);
    assertFalse(HFileReaderCacheManager.getInstanceIfInitialized().get().isExplicitlyConfigured());
  }

  @Test
  void testCreateHFileReader_ExplicitCacheConfig_ShouldReplaceDefaultConfiguredCaches() throws IOException {
    HFileReaderFactory.builder().withStorage(mockStorage).withPath(mockPath).build().createHFileReader();
    HFileReaderCacheManager defaultManager = HFileReaderCacheManager.getInstanceIfInitialized().get();

    properties.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), "true");
    properties.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE.key(), "7");
    HFileReaderFactory.builder().withStorage(mockStorage).withProps(properties).withPath(mockPath).build().createHFileReader();

    HFileReaderCacheManager explicitManager = HFileReaderCacheManager.getInstanceIfInitialized().get();
    assertNotSame(defaultManager, explicitManager);
    assertTrue(explicitManager.isExplicitlyConfigured());
    verifyNoInteractions(mockStorage);
  }

  @Test
  void testCreateHFileReader_WithCachingAndContent_ShouldNotCacheTrailerAndLoadOnOpenBlocks() throws IOException {
    properties.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), "true");
    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withContent(writeHFile("key", 3))
        .build();

    try (HFileReader reader = factory.createHFileReader()) {
      assertInstanceOf(CachingHFileReaderImpl.class, reader);
      assertEquals(3, reader.getNumKeyValueEntries());
      assertEquals("key0", readFirstKey(reader));
    }

    HFileReaderCacheManager manager = HFileReaderCacheManager.getInstanceIfInitialized().get();
    assertEquals(0, manager.getTrailerAndLoadOnOpenCacheSize(),
        "In-memory content has no I/O to save, so its trailer and load-on-open section are not cached");
    assertTrue(manager.getBlockCacheSize() > 0);
    verifyNoInteractions(mockStorage);
  }

  @Test
  void testCreateHFileReader_WithPathInfo_ShouldNotServeCachedEntriesForRewrittenFile() throws IOException {
    properties.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), "true");
    StoragePath path = new StoragePath("file:///rewritten.hfile");
    byte[] firstFile = writeHFile("first", 3);
    byte[] secondFile = writeHFile("second", 5);

    when(mockStorage.openSeekable(path, false)).thenReturn(toStream(firstFile));
    try (HFileReader reader = createReader(new StoragePathInfo(path, firstFile.length, false, (short) 1, 0L, 1_000L))) {
      assertEquals(3, reader.getNumKeyValueEntries());
      assertEquals("first0", readFirstKey(reader));
    }

    // the same path now holds a different file, as after a rollback and a re-attempt
    when(mockStorage.openSeekable(path, false)).thenReturn(toStream(secondFile));
    try (HFileReader reader = createReader(new StoragePathInfo(path, secondFile.length, false, (short) 1, 0L, 2_000L))) {
      assertEquals(5, reader.getNumKeyValueEntries());
      assertEquals("second0", readFirstKey(reader));
    }
    verify(mockStorage, never()).getPathInfo(any());
  }

  private HFileReader createReader(StoragePathInfo pathInfo) throws IOException {
    return HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPathInfo(pathInfo)
        .build()
        .createHFileReader();
  }

  private static String readFirstKey(HFileReader reader) throws IOException {
    assertTrue(reader.seekTo());
    KeyValue keyValue = reader.getKeyValue().get();
    assertEquals(keyValue.getKey().getContentInString() + "-value", getValue(keyValue));
    return keyValue.getKey().getContentInString();
  }

  private static SeekableDataInputStream toStream(byte[] content) {
    return new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(content));
  }

  private static byte[] writeHFile(String keyPrefix, int numEntries) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (HFileWriter writer = new HFileWriterImpl(HFileContext.builder().build(), out)) {
      for (int i = 0; i < numEntries; i++) {
        writer.append(keyPrefix + i, (keyPrefix + i + "-value").getBytes(StandardCharsets.UTF_8));
      }
    }
    return out.toByteArray();
  }

  @Test
  void testCreateHFileReader_WithCachingEnabled_ShouldLazilyOpenStream() throws IOException {
    properties.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), "true");

    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();

    HFileReader reader = factory.createHFileReader();
    assertInstanceOf(CachingHFileReaderImpl.class, reader);
    verify(mockStorage, never()).openSeekable(mockPath, false);
  }

  @Test
  void testCreateHFileReader_WithoutCaching_ShouldSurfaceFileSizeIOException() throws IOException {
    IOException sizeFailure = new IOException("size lookup failed");
    when(mockStorage.getPathInfo(mockPath)).thenThrow(sizeFailure);

    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();

    assertSame(sizeFailure, assertThrows(IOException.class, factory::createHFileReader));
    verify(mockStorage, never()).openSeekable(mockPath, false);
  }

  @Test
  void testCreateHFileReader_WithCaching_ShouldSurfaceFileSizeIOExceptionOnFirstRead() throws IOException {
    properties.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), "true");
    IOException sizeFailure = new IOException("size lookup failed");
    when(mockStorage.getPathInfo(mockPath)).thenThrow(sizeFailure);

    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();

    HFileReader reader = factory.createHFileReader();
    assertSame(sizeFailure, assertThrows(IOException.class, reader::initializeMetadata));
    verify(mockStorage, never()).openSeekable(mockPath, false);
  }
}

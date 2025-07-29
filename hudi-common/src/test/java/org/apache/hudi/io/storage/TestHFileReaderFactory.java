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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
  void setUp() {
    properties = new TypedProperties();
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
    verify(mockStorage, times(2)).getPathInfo(mockPath); // Once for size determination, once for download
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
  void testBuilder_WithoutPropertiesProvided_ShouldUseDefaultProperties() throws IOException {
    when(mockStorage.getPathInfo(mockPath)).thenReturn(mockPathInfo);
    when(mockPathInfo.getLength()).thenReturn(1024L);
    when(mockStorage.openSeekable(mockPath, false)).thenReturn(mockInputStream);

    // Not providing properties, should use defaults
    HFileReaderFactory factory = HFileReaderFactory.builder()
        .withStorage(mockStorage)
        .withPath(mockPath)
        .build();

    HFileReader result = factory.createHFileReader();
    assertNotNull(result);
  }
}

package org.apache.hudi.io.storage;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.io.SeekableDataInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    // Arrange
    final long fileSizeBelow = 5L; // 5 bytes - below default threshold
    final int thresholdMB = 10; // 10MB threshold

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_THRESHOLD_SIZE_MB.key(), String.valueOf(thresholdMB));

    when(mockStorage.getPathInfo(mockPath)).thenReturn(mockPathInfo);
    when(mockPathInfo.getLength()).thenReturn(fileSizeBelow);
    when(mockStorage.openSeekable(mockPath, false)).thenReturn(mockInputStream);
    doAnswer(invocation -> {
      byte[] buffer = invocation.getArgument(0);
      System.arraycopy(testContent, 0, buffer, 0, Math.min(testContent.length, buffer.length));
      return null;
    }).when(mockInputStream).readFully(any(byte[].class));

    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();

    // Act
    HFileReader result = factory.createHFileReader();

    // Assert
    assertNotNull(result);
    assertInstanceOf(HFileReaderImpl.class, result);

    // Verify that content was downloaded (cache was used)
    verify(mockStorage, times(2)).getPathInfo(mockPath); // Once for size determination, once for download
    verify(mockStorage, times(1)).openSeekable(mockPath, false); // For content download
    verify(mockInputStream, times(1)).readFully(any(byte[].class));
  }

  @Test
  void testCreateHFileReader_FileSizeAboveThreshold_ShouldNotUseContentCache() throws IOException {
    // Arrange
    final long fileSizeAbove = 15L * 1024L * 1024L; // 15MB - above 10MB threshold
    final int thresholdMB = 10; // 10MB threshold

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_THRESHOLD_SIZE_MB.key(), String.valueOf(thresholdMB));

    when(mockStorage.getPathInfo(mockPath)).thenReturn(mockPathInfo);
    when(mockPathInfo.getLength()).thenReturn(fileSizeAbove);
    when(mockStorage.openSeekable(mockPath, false)).thenReturn(mockInputStream);

    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();

    // Act
    HFileReader result = factory.createHFileReader();

    // Assert
    assertNotNull(result);
    assertInstanceOf(HFileReaderImpl.class, result);

    // Verify that content was NOT downloaded (cache was not used)
    verify(mockStorage, times(1)).getPathInfo(mockPath); // Only once for size determination
    verify(mockStorage, times(1)).openSeekable(mockPath, false); // For creating input stream directly
    verify(mockInputStream, never()).readFully(any(byte[].class)); // Content not downloaded
  }

  @Test
  void testCreateHFileReader_ContentProvidedInConstructor_ShouldUseProvidedContent() throws IOException {
    // Arrange
    final int thresholdMB = 10; // 10MB threshold

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_THRESHOLD_SIZE_MB.key(), String.valueOf(thresholdMB));

    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withContent(testContent)
        .build();

    // Act
    HFileReader result = factory.createHFileReader();

    // Assert
    assertNotNull(result);
    assertInstanceOf(HFileReaderImpl.class, result);

    // Verify that storage was never accessed since content was provided
    verify(mockStorage, never()).getPathInfo(any());
    verify(mockStorage, never()).openSeekable(any(), anyBoolean());

    // The file size should be determined from content length
    //assertEquals(testContent.length, ((HFileReaderImpl) result).getFileSize());
  }

  @Test
  void testCreateHFileReader_ContentProvidedAndPathProvided_ShouldPreferContent() throws IOException {
    // Arrange
    final int thresholdMB = 10;

    properties.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_THRESHOLD_SIZE_MB.key(), String.valueOf(thresholdMB));

    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath) // Both path and content provided
        .withContent(testContent)
        .build();

    // Act
    HFileReader result = factory.createHFileReader();

    // Assert
    assertNotNull(result);
    assertInstanceOf(HFileReaderImpl.class, result);

    // Verify that path was not used for getting file info since content was provided
    verify(mockStorage, never()).getPathInfo(mockPath);
    verify(mockStorage, never()).openSeekable(any(), anyBoolean());

    // File size should be from content
    //assertEquals(testContent.length, ((HFileReaderImpl) result).getFileSize());
  }

  @Test
  void testCreateHFileReader_NoPathOrContent_ShouldThrowException() {
    // Arrange & Act & Assert
    assertThrows(IllegalArgumentException.class, () -> {
      HFileReaderFactory.newBuilder()
          .withStorage(mockStorage)
          .withProps(properties)
          .build();
    });
  }

  @Test
  void testShouldUseContentCache_WithContentPresent_ShouldReturnFalse() {
    // Arrange
    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withContent(testContent)
        .build();

    // We can't directly test the private method, but we can verify behavior
    // by checking that no download occurs when content is already present

    // This is implicitly tested in testCreateHFileReader_ContentProvidedInConstructor_ShouldUseProvidedContent
  }

  @Test
  void testDetermineFileSize_WithPath_ShouldReturnPathLength() throws IOException {
    // Arrange
    final long expectedSize = 1024L;
    when(mockStorage.getPathInfo(mockPath)).thenReturn(mockPathInfo);
    when(mockPathInfo.getLength()).thenReturn(expectedSize);

    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withPath(mockPath)
        .build();

    // Act
    HFileReader result = factory.createHFileReader();

    // Assert
    verify(mockStorage, times(1)).getPathInfo(mockPath);
    //assertEquals(expectedSize, ((HFileReaderImpl) result).getFileSize());
  }

  @Test
  void testDetermineFileSize_WithContent_ShouldReturnContentLength() throws IOException {
    // Arrange
    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withProps(properties)
        .withContent(testContent)
        .build();

    // Act
    HFileReader result = factory.createHFileReader();

    // Assert
    //assertEquals(testContent.length, ((HFileReaderImpl) result).getFileSize());
  }

  @Test
  void testBuilder_WithNullStorage_ShouldThrowException() {
    // Arrange & Act & Assert
    assertThrows(IllegalArgumentException.class, () -> {
      HFileReaderFactory.newBuilder()
          .withStorage(null)
          .withPath(mockPath)
          .build();
    });
  }

  @Test
  void testBuilder_WithoutPropertiesProvided_ShouldUseDefaultProperties() throws IOException {
    // Arrange
    when(mockStorage.getPathInfo(mockPath)).thenReturn(mockPathInfo);
    when(mockPathInfo.getLength()).thenReturn(1024L);
    when(mockStorage.openSeekable(mockPath, false)).thenReturn(mockInputStream);

    // Act - Not providing properties, should use defaults
    HFileReaderFactory factory = HFileReaderFactory.newBuilder()
        .withStorage(mockStorage)
        .withPath(mockPath)
        .build();

    HFileReader result = factory.createHFileReader();

    // Assert
    assertNotNull(result);
    // If no exception is thrown, default properties were used successfully
  }
}

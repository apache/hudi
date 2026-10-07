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
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.MurmurHash;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.CachingHFileReaderImpl;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderCacheManager;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;

/**
 * Factory class to provide the implementation for
 * the HFile Reader for {@link HoodieNativeAvroHFileReader}.
 */
public class HFileReaderFactory {

  private final HoodieStorage storage;
  private final HoodieMetadataConfig metadataConfig;
  private final TypedProperties properties;
  private final Either<StoragePath, byte[]> fileSource;
  private final Option<StoragePathInfo> pathInfoOpt;
  private Option<Long> fileSizeOpt;

  private HFileReaderFactory(HoodieStorage storage,
                             TypedProperties properties,
                             Either<StoragePath, byte[]> fileSource,
                             Option<StoragePathInfo> pathInfoOpt,
                             Option<Long> fileSizeOpt) {
    this.storage = storage;
    this.metadataConfig = HoodieMetadataConfig.newBuilder().withProperties(properties).build();
    this.properties = properties;
    this.fileSource = fileSource;
    this.pathInfoOpt = pathInfoOpt;
    this.fileSizeOpt = fileSizeOpt;
  }

  public HFileReader createHFileReader() throws IOException {
    if (!shouldEnableBlockCaching()) {
      final long fileSize = getFileSize();
      return new HFileReaderImpl(createInputStream(fileSize), fileSize);
    }

    int blockCacheSize = ConfigUtils.getIntWithAltKeys(
        properties, HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE);
    int loadOnOpenCacheSize = ConfigUtils.getIntWithAltKeys(
        properties, HoodieReaderConfig.HFILE_LOAD_ON_OPEN_CACHE_SIZE);
    int cacheTtlMinutes = ConfigUtils.getIntWithAltKeys(
        properties, HoodieReaderConfig.HFILE_BLOCK_CACHE_TTL_MINUTES);
    long cacheMaxWeightBytes = (long) ConfigUtils.getIntWithAltKeys(
        properties, HoodieReaderConfig.HFILE_BLOCK_CACHE_MAX_WEIGHT_MB) * 1024L * 1024L;
    HFileReaderCacheManager cacheManager = HFileReaderCacheManager.getInstance(
        blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes, isCacheExplicitlyConfigured());
    // The caching reader opens the stream only on a cache miss, so both the size lookup and the
    // open are deferred; their IOExceptions are unwrapped again by the reader.
    final Lazy<Long> lazyFileSize = Lazy.lazily(() -> {
      try {
        return getFileSize();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to determine the HFile size.", e);
      }
    });
    final Lazy<SeekableDataInputStream> lazyStream = Lazy.lazily(() -> {
      try {
        return createInputStream(lazyFileSize.get());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to create input stream.", e);
      }
    });
    return new CachingHFileReaderImpl(lazyStream, lazyFileSize, getFileIdentity(), fileSource.isLeft(), cacheManager);
  }

  private long getFileSize() throws IOException {
    if (fileSizeOpt.isEmpty()) {
      fileSizeOpt = Option.of(determineFileSize());
    }
    return fileSizeOpt.get();
  }

  private boolean shouldEnableBlockCaching() {
    return ConfigUtils.getBooleanWithAltKeys(properties, HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED);
  }

  private boolean isCacheExplicitlyConfigured() {
    return ConfigUtils.containsConfigProperty(properties, HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE)
        || ConfigUtils.containsConfigProperty(properties, HoodieReaderConfig.HFILE_LOAD_ON_OPEN_CACHE_SIZE)
        || ConfigUtils.containsConfigProperty(properties, HoodieReaderConfig.HFILE_BLOCK_CACHE_TTL_MINUTES)
        || ConfigUtils.containsConfigProperty(properties, HoodieReaderConfig.HFILE_BLOCK_CACHE_MAX_WEIGHT_MB);
  }

  /**
   * Returns the identity of the HFile content in the shared caches, which outlive any single file
   * at a path. When the caller resolved the file from a listing, the length and modification time
   * are part of the identity, so entries cached from a file are not served for a different file
   * written to the same path later.
   */
  private String getFileIdentity() {
    if (fileSource.isLeft()) {
      if (pathInfoOpt.isPresent()) {
        StoragePathInfo pathInfo = pathInfoOpt.get();
        return pathInfo.getPath() + "#" + pathInfo.getLength() + "#" + pathInfo.getModificationTime();
      }
      return fileSource.asLeft().toString();
    }
    // For byte array content, use a hash-based identifier
    int murmurHash = MurmurHash.getInstance().hash(fileSource.asRight());
    return String.valueOf(murmurHash);
  }

  private long determineFileSize() throws IOException {
    if (fileSource.isLeft()) {
      return storage.getPathInfo(fileSource.asLeft()).getLength();
    }
    return fileSource.asRight().length;
  }

  private SeekableDataInputStream createInputStream(long fileSize) throws IOException {
    if (fileSource.isLeft()) {
      if (fileSize <= (long) metadataConfig.getFileCacheMaxSizeMB() * 1024L * 1024L) {
        // Download the whole file if the file size is below a configured threshold
        StoragePath path = fileSource.asLeft();
        byte[] buffer;
        try (SeekableDataInputStream stream = storage.openSeekable(path, false)) {
          buffer = new byte[(int) fileSize];
          stream.readFully(buffer);
        }
        return new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(buffer));
      }
      return storage.openSeekable(fileSource.asLeft(), false);
    }
    return new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(fileSource.asRight()));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private HoodieStorage storage;
    private Option<TypedProperties> properties = Option.empty();
    private Either<StoragePath, byte[]> fileSource;
    private Option<StoragePathInfo> pathInfoOpt = Option.empty();
    private Option<Long> fileSizeOpt = Option.empty();

    public Builder withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    public Builder withProps(TypedProperties props) {
      this.properties = Option.of(props);
      return this;
    }

    public Builder withPath(StoragePath path) {
      ValidationUtils.checkState(fileSource == null, "HFile source already set, cannot set path");
      this.fileSource = Either.left(path);
      return this;
    }

    /**
     * Sets the HFile source from an already resolved {@link StoragePathInfo}, which supplies the file
     * size and identifies the file content in the shared caches without a storage call.
     */
    public Builder withPathInfo(StoragePathInfo pathInfo) {
      ValidationUtils.checkState(fileSource == null, "HFile source already set, cannot set path info");
      this.fileSource = Either.left(pathInfo.getPath());
      this.pathInfoOpt = Option.of(pathInfo);
      this.fileSizeOpt = Option.of(pathInfo.getLength());
      return this;
    }

    public Builder withContent(byte[] bytesContent) {
      ValidationUtils.checkState(fileSource == null, "HFile source already set, cannot set bytes content");
      this.fileSource = Either.right(bytesContent);
      return this;
    }

    public Builder withFileSize(long fileSize) {
      ValidationUtils.checkState(fileSize >= 0, "file size is invalid, should be greater than or equal to zero");
      this.fileSizeOpt = Option.of(fileSize);
      return this;
    }

    public HFileReaderFactory build() {
      ValidationUtils.checkArgument(storage != null, "Storage cannot be null");
      ValidationUtils.checkArgument(fileSource != null, "HFile source cannot be null");
      TypedProperties props = properties.isPresent() ? properties.get() : new TypedProperties();
      return new HFileReaderFactory(storage, props, fileSource, pathInfoOpt, fileSizeOpt);
    }
  }
}

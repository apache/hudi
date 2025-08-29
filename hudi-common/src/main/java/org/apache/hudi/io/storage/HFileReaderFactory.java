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
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;

/**
 * Factory class to provide the implementation for
 * the HFile Reader for {@link HoodieNativeAvroHFileReader}.
 */
public class HFileReaderFactory {

  private final HoodieStorage storage;
  private final HoodieMetadataConfig metadataConfig;
  private final Either<StoragePath, byte[]> fileSource;
  private Option<Long> fileSizeOpt;

  public HFileReaderFactory(HoodieStorage storage,
                            TypedProperties properties,
                            Either<StoragePath, byte[]> fileSource,
                            Option<Long> fileSizeOpt) {
    this.storage = storage;
    this.metadataConfig = HoodieMetadataConfig.newBuilder().withProperties(properties).build();
    this.fileSource = fileSource;
    this.fileSizeOpt = fileSizeOpt;
  }

  public HFileReader createHFileReader() throws IOException {
    if (fileSizeOpt.isEmpty()) {
      fileSizeOpt = Option.of(determineFileSize());
    }

    final SeekableDataInputStream inputStream = createInputStream(fileSizeOpt.get());
    return new HFileReaderImpl(inputStream, fileSizeOpt.get());
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
          buffer = new byte[(int) storage.getPathInfo(path).getLength()];
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
    private Option<Long> fileSizeOpt;

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
      Option<Long> newFileSizeOpt = fileSizeOpt == null ? Option.empty() : fileSizeOpt;
      return new HFileReaderFactory(storage, props, fileSource, newFileSizeOpt);
    }
  }
}

package org.apache.hudi.io.storage;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;

public class HFileReaderFactory {

  private final HoodieStorage storage;
  private final TypedProperties properties;
  private final Option<StoragePath> path;
  private final Option<byte[]> content;

  public HFileReaderFactory(HoodieStorage storage,
                            TypedProperties properties,
                            Option<StoragePath> path,
                            Option<byte[]> content) {
    this.storage = storage;
    this.properties = properties;
    this.path = path;
    this.content = content;
  }

  public HFileReader createHFileReader() throws IOException {
    final long fileSize = determineFileSize();
    final Option<byte[]> finalContent = shouldUseContentCache(fileSize) ? downloadContent() : content;
    final SeekableDataInputStream inputStream = createInputStream(finalContent);
    return new HFileReaderImpl(inputStream, fileSize);
  }

  private long determineFileSize() throws IOException {
    if (path.isPresent()) {
      return storage.getPathInfo(path.get()).getLength();
    } else if (content.isPresent()) {
      return content.get().length;
    } else {
      throw new IllegalStateException("Cannot determine file size without path or content");
    }
  }

  private boolean shouldUseContentCache(long fileSize) {
    return content.isEmpty() && fileSize <= properties.getHfileCacheMinThreshold();
  }

  private SeekableDataInputStream createInputStream(Option<byte[]> contentToUse) throws IOException {
    if (contentToUse.isPresent()) {
      return new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(contentToUse.get()));
    } else if (path.isPresent()) {
      return storage.openSeekable(path.get(), false);
    } else {
      throw new IllegalStateException("Cannot create input stream without content or path");
    }
  }

  // Helper method for downloading content (assuming this exists)
  private Option<byte[]> downloadContent() throws IOException {
    if (path.isEmpty()) {
      throw new IllegalStateException("Cannot download content without path");
    }

    // Implementation depends on your existing downloadContent logic
    try (SeekableDataInputStream stream = storage.openSeekable(path.get(), false)) {
      byte[] buffer = new byte[(int) storage.getPathInfo(path.get()).getLength()];
      stream.readFully(buffer);
      return Option.of(buffer);
    }
  }

  public static class Builder {
    private HoodieStorage storage;
    private Option<TypedProperties> properties = Option.empty();
    private Option<StoragePath> path = Option.empty();
    private Option<byte[]> bytesContent = Option.empty();

    public Builder withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    public Builder withProps(TypedProperties props) {
      this.properties = Option.of(props);
      return this;
    }

    public Builder withPath(StoragePath path) {
      this.path = Option.of(path);
      return this;
    }

    public Builder withContent(byte[] bytesContent) {
      this.bytesContent = Option.of(bytesContent);
      return this;
    }

    public HFileReaderFactory build() {
      if (storage == null) {
        throw new IllegalArgumentException("Storage cannot be null");
      }

      if (path.isEmpty() && bytesContent.isEmpty()) {
        throw new IllegalArgumentException("Either path or content needs to be passed in.");
      }

      TypedProperties props = properties.isPresent() ? properties.get() : new TypedProperties();
      return new HFileReaderFactory(storage, props, path, bytesContent);
    }
  }
}

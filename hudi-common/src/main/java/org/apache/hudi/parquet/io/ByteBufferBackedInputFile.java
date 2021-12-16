package org.apache.hudi.parquet.io;

import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Implementation of {@link InputFile} backed by {@code byte[]} buffer
 */
public class ByteBufferBackedInputFile implements InputFile {
  private final byte[] buffer;
  private final int offset;
  private final int length;

  public ByteBufferBackedInputFile(byte[] buffer, int offset, int length) {
    this.buffer = buffer;
    this.offset = offset;
    this.length = length;
  }

  public ByteBufferBackedInputFile(byte[] buffer) {
    this(buffer, 0, buffer.length);
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new DelegatingSeekableInputStream(new ByteBufferBackedInputStream(buffer, offset, length)) {
      @Override
      public long getPos() {
        return ((ByteBufferBackedInputStream) getStream()).getPosition();
      }

      @Override
      public void seek(long newPos) {
        ((ByteBufferBackedInputStream) getStream()).seek(newPos);
      }
    };
  }
}

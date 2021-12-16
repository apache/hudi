package org.apache.hudi.parquet.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Implementation of the {@link OutputFile} backed by {@link java.io.OutputStream}
 */
public class OutputStreamBackedOutputFile implements OutputFile {

  private static final long DEFAULT_BLOCK_SIZE = 1024L * 1024L;

  private final FSDataOutputStream outputStream;

  public OutputStreamBackedOutputFile(FSDataOutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return new PositionOutputStreamAdapter(outputStream);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return create(blockSizeHint);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return DEFAULT_BLOCK_SIZE;
  }

  private static class PositionOutputStreamAdapter extends PositionOutputStream {
    private final FSDataOutputStream delegate;

    PositionOutputStreamAdapter(FSDataOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
    }

    @Override
    public void write(@Nonnull byte[] buffer, int off, int len) throws IOException {
      delegate.write(buffer, off, len);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() {
      // we do not actually close the internal stream here, to prevent that the finishing
      // of the Parquet Writer closes the target output stream
    }
  }
}

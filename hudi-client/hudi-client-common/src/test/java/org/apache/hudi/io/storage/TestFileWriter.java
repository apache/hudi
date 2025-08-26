package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.Properties;

public class TestFileWriter implements HoodieFileWriter {
  private boolean closed = false;
  private boolean failOnWrite = false;

  public TestFileWriter(StoragePath filePath, HoodieStorage storage, boolean failOnInitialization) throws IOException {
    this(filePath, storage, failOnInitialization, false);
  }

  public TestFileWriter(StoragePath filePath, HoodieStorage storage, boolean failOnInitialization, boolean failOnWrite) throws IOException {
    this.failOnWrite = failOnWrite;
    
    if (failOnInitialization) {
      throw new IOException("Simulated file writer initialization failure");
    }

    storage.create(filePath, false);
  }

  @Override
  public boolean canWrite() {
    return !closed;
  }

  @Override
  public void writeWithMetadata(HoodieKey key, HoodieRecord record, Schema schema, Properties props) throws IOException {
    if (closed) {
      throw new IOException("Writer is closed");
    }
    if (failOnWrite) {
      throw new IOException("Simulated file writer write failure");
    }
  }

  @Override
  public void write(String recordKey, HoodieRecord record, Schema schema, Properties props) throws IOException {
    if (closed) {
      throw new IOException("Writer is closed");
    }
    if (failOnWrite) {
      throw new IOException("Simulated file writer write failure");
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}

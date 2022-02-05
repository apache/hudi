package org.apache.hudi.common.io.storage;

import org.apache.hudi.common.model.HoodieRecord;

import java.io.IOException;

public interface HoodieRecordFileWriter<R> extends HoodieFileWriter {

  boolean canWrite();

  void writeWithMetadata(R newRecord, HoodieRecord record) throws IOException;

  void write(String key, R oldRecord) throws IOException;

  void close() throws IOException;

}

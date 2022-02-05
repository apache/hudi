package org.apache.hudi.common.io.storage;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieRecord;

import java.io.IOException;
import java.util.Properties;

public interface HoodieFileWriter {

  // TODO rename
  void writeWithMetadata(HoodieRecord record, Schema schema, Properties props) throws IOException;

  // TODO rename
  void write(HoodieRecord record, Schema schema, Properties props) throws IOException;

}

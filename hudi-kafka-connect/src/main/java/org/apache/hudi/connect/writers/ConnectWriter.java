package org.apache.hudi.connect.writers;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.List;

public interface ConnectWriter<T> {

  void writeRecord(SinkRecord record) throws IOException;

  List<T> close() throws IOException;
}

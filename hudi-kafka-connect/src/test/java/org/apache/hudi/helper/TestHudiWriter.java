package org.apache.hudi.helper;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.connect.writers.ConnectWriter;
import org.apache.hudi.connect.writers.ConnectWriterProvider;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

/**
 * Helper class the provides a Hudi writer and
 * maintains stats that are used for test validation.
 */
public class TestHudiWriter implements ConnectWriterProvider<WriteStatus>, ConnectWriter<WriteStatus> {

  private int numberRecords;
  private boolean isClosed;

  public TestHudiWriter() {
    this.numberRecords = 0;
    this.isClosed = false;
  }

  public int getNumberRecords() {
    return numberRecords;
  }

  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void writeRecord(SinkRecord record) {
    numberRecords++;
  }

  @Override
  public List<WriteStatus> close() {
    isClosed = false;
    return null;
  }

  @Override
  public ConnectWriter<WriteStatus> getWriter(String commitTime) {
    return this;
  }
}

package org.apache.hudi.common.table.read.buffer;

import java.util.Objects;

class TestRecord {
  private final String recordKey;
  private final int value;

  public TestRecord(String recordKey, int value) {
    this.recordKey = recordKey;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestRecord that = (TestRecord) o;
    return value == that.value && Objects.equals(recordKey, that.recordKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordKey, value);
  }

  public String getRecordKey() {
    return recordKey;
  }

  public int getValue() {
    return value;
  }
}

package org.apache.hudi.common.serialization;

import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;

public class DefaultRecordSerializer<T> implements RecordSerializer<T> {
  @Override
  public byte[] serialize(T record) {
    try {
      return SerializationUtils.serialize(record);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to serialize record", e);
    }
  }

  @Override
  public T deserialize(byte[] bytes, int schemaId) {
    return SerializationUtils.deserialize(bytes);
  }
}

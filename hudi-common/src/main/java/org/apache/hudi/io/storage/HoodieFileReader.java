package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface HoodieFileReader {
  String[] readMinMaxRecordKeys();

  BloomFilter readBloomFilter();

  Set<String> filterRowKeys(Set<String> candidateRowKeys);

  default Map<String, HoodieRecord> getRecordsByKeys(List<String> rowKeys, HoodieRecord.Mapper mapper) throws IOException {
    throw new UnsupportedOperationException();
  }

  Iterator<HoodieRecord> getRecordIterator(Schema readerSchema, HoodieRecord.Mapper mapper) throws IOException;

  default Iterator<HoodieRecord> getRecordIterator(HoodieRecord.Mapper mapper) throws IOException {
    return getRecordIterator(getSchema(), mapper);
  }

  default Option<HoodieRecord> getRecordByKey(String key, Schema readerSchema, HoodieRecord.Mapper mapper) throws IOException {
    throw new UnsupportedOperationException();
  }

  default Option<HoodieRecord> getRecordByKey(String key, HoodieRecord.Mapper mapper) throws IOException {
    return getRecordByKey(key, getSchema(), mapper);
  }

  Schema getSchema();

  void close();

  long getTotalRecords();
}

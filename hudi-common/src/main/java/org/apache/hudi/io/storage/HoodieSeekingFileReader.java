package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public interface HoodieSeekingFileReader<T> extends HoodieFileReader<T> {

  default Option<HoodieRecord<T>> getRecordByKey(String key, Schema readerSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default Option<HoodieRecord<T>> getRecordByKey(String key) throws IOException {
    return getRecordByKey(key, getSchema());
  }

  default ClosableIterator<HoodieRecord<T>> getRecordsByKeysIterator(List<String> keys, Schema schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<HoodieRecord<T>> getRecordsByKeysIterator(List<String> keys) throws IOException {
    return getRecordsByKeysIterator(keys, getSchema());
  }

  default ClosableIterator<HoodieRecord<T>> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema) throws IOException {
    throw new UnsupportedEncodingException();
  }

  default ClosableIterator<HoodieRecord<T>> getRecordsByKeyPrefixIterator(List<String> keyPrefixes) throws IOException {
    return getRecordsByKeyPrefixIterator(keyPrefixes, getSchema());
  }

}

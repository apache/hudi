package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;

import java.io.IOException;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * Base class for every {@link HoodieAvroFileReader}
 */
abstract class HoodieAvroFileReaderBase implements HoodieAvroFileReader {

  @Override
  public ClosableIterator<HoodieRecord<IndexedRecord>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordIterator(readerSchema, requestedSchema);
    return new MappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  protected ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema) throws IOException {
    return getIndexedRecordIterator(readerSchema, readerSchema);
  }

  protected abstract ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException;
}

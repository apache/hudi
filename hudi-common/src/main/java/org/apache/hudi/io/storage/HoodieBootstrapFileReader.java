package org.apache.hudi.io.storage;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Set;


public class HoodieBootstrapFileReader<T> implements HoodieFileReader<T> {

  private HoodieFileReader<T> skeletonFileReader;
  private HoodieFileReader<T> dataFileReader;
  private Boolean isConsistentLogicalTimestampEnabled;

  public HoodieBootstrapFileReader(HoodieFileReader<T> skeletonFileReader, HoodieFileReader<T> dataFileReader, Boolean isConsistentLogicalTimestampEnabled) {
    this.skeletonFileReader = skeletonFileReader;
    this.dataFileReader = dataFileReader;
    this.isConsistentLogicalTimestampEnabled = isConsistentLogicalTimestampEnabled;
  }
  @Override
  public String[] readMinMaxRecordKeys() {
    return skeletonFileReader.readMinMaxRecordKeys();
  }

  @Override
  public BloomFilter readBloomFilter() {
    return skeletonFileReader.readBloomFilter();
  }

  @Override
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    return skeletonFileReader.filterRowKeys(candidateRowKeys);
  }

  @Override
  public ClosableIterator<HoodieRecord<T>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    ClosableIterator<HoodieRecord<T>> skeletonIterator = skeletonFileReader.getRecordIterator(readerSchema, requestedSchema);
    ClosableIterator<HoodieRecord<T>> dataFileIterator = dataFileReader.getRecordIterator(HoodieAvroUtils.removeMetadataFields(readerSchema), requestedSchema);

    return new ClosableIterator<HoodieRecord<T>>() {
      @Override
      public void close() {
          skeletonIterator.close();
          dataFileIterator.close();
      }

      @Override
      public boolean hasNext() {
        return skeletonIterator.hasNext() && dataFileIterator.hasNext();
      }

      @Override
      public HoodieRecord<T> next() {
        HoodieRecord<T> dataRecord = dataFileIterator.next();
        HoodieRecord<T> skeletonRecord = skeletonIterator.next();
        HoodieRecord<T> ret = dataRecord.prependMetaFields(readerSchema, readerSchema, new MetadataValues().
            setCommitTime(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.COMMIT_TIME_METADATA_FIELD ))
            .setCommitSeqno(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD))
            .setRecordKey(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD))
            .setPartitionPath(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.PARTITION_PATH_METADATA_FIELD))
            .setFileName(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.FILENAME_METADATA_FIELD)),null);
        return ret;
      }
    };
  }

  @Override
  public Schema getSchema() {
    return skeletonFileReader.getSchema();
  }

  @Override
  public void close() {
    skeletonFileReader.close();
    dataFileReader.close();
  }

  @Override
  public long getTotalRecords() {
    return Math.min(skeletonFileReader.getTotalRecords(), dataFileReader.getTotalRecords());
  }
}

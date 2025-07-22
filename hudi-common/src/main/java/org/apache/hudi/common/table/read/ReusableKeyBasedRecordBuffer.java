package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

public class ReusableKeyBasedRecordBuffer<T> extends KeyBasedFileGroupRecordBuffer<T> {
  private final Set<String> validKeys;

  public ReusableKeyBasedRecordBuffer(HoodieReaderContext<T> readerContext, HoodieTableMetaClient hoodieTableMetaClient,
                                      RecordMergeMode recordMergeMode, PartialUpdateMode partialUpdateMode,
                                      TypedProperties props, HoodieReadStats readStats, Option<String> orderingFieldName, UpdateProcessor<T> updateProcessor) {
    this(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, readStats, orderingFieldName, updateProcessor, Collections.emptySet(), null);
  }

  private ReusableKeyBasedRecordBuffer(HoodieReaderContext<T> readerContext, HoodieTableMetaClient hoodieTableMetaClient,
                                       RecordMergeMode recordMergeMode, PartialUpdateMode partialUpdateMode,
                                       TypedProperties props, HoodieReadStats readStats, Option<String> orderingFieldName,
                                       UpdateProcessor<T> updateProcessor, Set<String> validKeys, ExternalSpillableMap<Serializable, BufferedRecord<T>> records) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, readStats, orderingFieldName, updateProcessor, records);
    this.validKeys = validKeys;
  }

  public ReusableKeyBasedRecordBuffer<T> withKeyPredicate(Set<String> validKeys, HoodieReadStats stats) {
    return new ReusableKeyBasedRecordBuffer<>(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode,
        props, stats, orderingFieldName, updateProcessor, validKeys, records);
  }

  @Override
  protected void initializeLogRecordIterator() {
    logRecordIterator = validKeys.stream().map(records::get).iterator();
  }

  @Override
  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordKey(baseRecord, readerSchema);
    BufferedRecord<T> logRecordInfo = records.get(recordKey);
    if (logRecordInfo != null) {
      validKeys.remove(recordKey);
    }
    return hasNextBaseRecord(baseRecord, logRecordInfo);
  }
}
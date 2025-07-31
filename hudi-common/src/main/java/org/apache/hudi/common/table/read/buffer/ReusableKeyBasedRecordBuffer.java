/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Predicates;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link HoodieFileGroupRecordBuffer <T>} implementation that allows the user to create a record buffer with an existing mapping of key to latest record from the log files.
 * The implementation is used when point lookups are required for the same file group, such as the metadata table's files partition.
 * As a result, the buffer also requires a set of valid keys to be provided to avoid returning all records in the processed logs when doing these point or small-batch lookups.
 * @param <T> the engine specific record type
 */
public class ReusableKeyBasedRecordBuffer<T> extends FileGroupRecordBuffer<T> {
  private final Set<String> validKeys;
  private final Map<Serializable, BufferedRecord<T>> existingRecords;

  ReusableKeyBasedRecordBuffer(HoodieReaderContext<T> readerContext, HoodieTableMetaClient hoodieTableMetaClient,
                               RecordMergeMode recordMergeMode, PartialUpdateMode partialUpdateMode,
                               TypedProperties props, List<String> orderingFieldNames,
                               UpdateProcessor<T> updateProcessor, Map<Serializable, BufferedRecord<T>> records) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, orderingFieldNames, updateProcessor);
    this.existingRecords = records;
    ValidationUtils.checkArgument(readerContext.getKeyFilterOpt().orElse(null) instanceof Predicates.In,
        () -> "Key filter should be of type Predicates.In, but found: " + readerContext.getKeyFilterOpt().map(filter -> filter.getClass().getSimpleName()).orElse("NULL"));
    List<Expression> children = ((Predicates.In) readerContext.getKeyFilterOpt().get()).getRightChildren();
    this.validKeys = children.stream().map(e -> (String) e.eval(null)).collect(Collectors.toSet());
  }

  @Override
  protected ExternalSpillableMap<Serializable, BufferedRecord<T>> initializeRecordsMap(String spillableMapBasePath) {
    return null;
  }

  @Override
  protected void initializeLogRecordIterator() {
    logRecordIterator = new RemainingRecordIterator<>(validKeys, existingRecords);
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Handle merging.
    while (baseFileIterator.hasNext()) {
      if (hasNextBaseRecord(baseFileIterator.next())) {
        return true;
      }
    }

    // Handle records solely from log files.
    return hasNextLogRecord();
  }

  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema);
    // Avoid removing from the map so the map can be reused later
    BufferedRecord<T> logRecordInfo = existingRecords.get(recordKey);
    if (logRecordInfo != null) {
      validKeys.remove(recordKey);
    }
    return hasNextBaseRecord(baseRecord, logRecordInfo);
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) {
    throw new HoodieNotSupportedException("Reusable record buffer does not perform the processing of the data blocks");
  }

  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable index) {
    throw new HoodieNotSupportedException("Reusable record buffer does not process the data records from the logs");

  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) {
    throw new HoodieNotSupportedException("Reusable record buffer does not perform the processing of the delete blocks");

  }

  @Override
  public void processNextDeletedRecord(DeleteRecord record, Serializable index) {
    throw new HoodieNotSupportedException("Reusable record buffer does not process the delete records from the logs");
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return validKeys.contains(recordKey) && existingRecords.containsKey(recordKey);
  }

  /**
   * The close method is a no-op for this buffer implementation since the record map is managed by the {@link ReusableFileGroupRecordBufferLoader<T>}.
   */
  @Override
  public void close() {
    // no-op
  }

  private static class RemainingRecordIterator<T> implements Iterator<T> {
    private final Iterator<String> validKeys;
    private final Map<Serializable, T> recordsByKey;
    private String nextKey;

    public RemainingRecordIterator(Set<String> validKeys, Map<Serializable, T> recordsByKey) {
      this.validKeys = validKeys.iterator();
      this.recordsByKey = recordsByKey;
    }

    @Override
    public boolean hasNext() {
      if (nextKey != null) {
        return true;
      }
      while (nextKey == null && validKeys.hasNext()) {
        String candidateKey = validKeys.next();
        if (recordsByKey.containsKey(candidateKey)) {
          nextKey = candidateKey;
        }
      }
      return nextKey != null;
    }

    @Override
    public T next() {
      T result = recordsByKey.get(nextKey);
      nextKey = null;
      return result;
    }
  }
}
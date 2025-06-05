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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Util class for creating {@link IteratorConverter} for iterator returned by {@link HoodieFileGroupReader}.
 */
public class IteratorConverters {

  /**
   * Converter for converting engine-specific row or {@link BufferedRecord}
   * into elements for iterator returned by FileGroup reader.
   *
   * @param <T> Type for engine-specific row
   * @param <O> Type for iterator elements
   */
  public interface IteratorConverter<T, O> {

    /**
     * @return {@link IteratorMode} for FileGroup reader
     */
    IteratorMode getIteratorMode();

    /**
     * Converting engine-specific row into the iterator element.
     *
     * @param row               Engine-specific row
     * @param orderingFieldName Name for the ordering field
     * @param isDelete Supplier for the DELETE indicator
     *
     * @return Iterator element of type O.
     */
    O convert(T row, Option<String> orderingFieldName, Supplier<Boolean> isDelete);

    /**
     * Converting {@link BufferedRecord} into the iterator element.
     *
     * @param record        Buffered record
     *
     * @return Iterator element of type O.
     */
    O convert(BufferedRecord<T> record);
  }

  /**
   * Creating {@link IteratorConverter} from given {@link IteratorMode} and reader context.
   *
   * @param readerContext Hoodie reader context
   * @param iteratorMode  Mode for the iterator
   *
   * @return The {@link IteratorConverter}.
   */
  public static <T> IteratorConverter<T, ?> createIteratorConverter(HoodieReaderContext<T> readerContext, IteratorMode iteratorMode) {
    switch (iteratorMode) {
      case RECORD_KEY:
        return new RecordKeyIteratorConverter<>(readerContext);
      case ENGINE_RECORD:
        return new RowIteratorConverter<>(readerContext);
      case HOODIE_RECORD:
      default:
        return new HoodieRecordIteratorConverter<>(readerContext);
    }
  }

  /**
   * Base implementation for the {@link IteratorConverter}.
   */
  private abstract static class BaseIteratorConverter<T, O> implements IteratorConverter<T, O> {
    protected final HoodieReaderContext<T> readerContext;
    protected final Option<UnaryOperator<T>> outputConverter;

    public BaseIteratorConverter(HoodieReaderContext<T> readerContext) {
      this.readerContext = readerContext;
      this.outputConverter = readerContext.getSchemaHandler().getOutputConverter();
    }
  }

  /**
   * Implementation of {@link IteratorConverter} for {@code RECORD_KEY} iterator mode.
   */
  public static class RecordKeyIteratorConverter<T> extends BaseIteratorConverter<T, String> {
    public RecordKeyIteratorConverter(HoodieReaderContext<T> readerContext) {
      super(readerContext);
    }

    @Override
    public IteratorMode getIteratorMode() {
      return IteratorMode.RECORD_KEY;
    }

    @Override
    public String convert(T row, Option<String> orderingFieldName, Supplier<Boolean> isDelete) {
      return readerContext.getRecordKey(row, readerContext.getSchemaHandler().getRequiredSchema());
    }

    @Override
    public String convert(BufferedRecord<T> record) {
      return record.getRecordKey();
    }
  }

  /**
   * Implementation of {@link IteratorConverter} for {@code ENGINE_RECORD} iterator mode.
   */
  public static class RowIteratorConverter<T> extends BaseIteratorConverter<T, T> {
    public RowIteratorConverter(HoodieReaderContext<T> readerContext) {
      super(readerContext);
    }

    @Override
    public IteratorMode getIteratorMode() {
      return IteratorMode.ENGINE_RECORD;
    }

    @Override
    public T convert(T row, Option<String> orderingFieldName, Supplier<Boolean> isDelete) {
      T sealedRow = readerContext.seal(row);
      return outputConverter.map(converter -> converter.apply(sealedRow)).orElse(sealedRow);
    }

    @Override
    public T convert(BufferedRecord<T> record) {
      // do not need seal for DELETE records as they come from buffered log records from spillable map
      T row = record.isDelete() ? record.getRecord() : readerContext.seal(record.getRecord());
      return outputConverter.map(converter -> converter.apply(row)).orElse(row);
    }
  }

  /**
   * Implementation of {@link IteratorConverter} for {@code HOODIE_RECORD} iterator mode.
   */
  public static class HoodieRecordIteratorConverter<T> extends BaseIteratorConverter<T, HoodieRecord<T>> {
    public HoodieRecordIteratorConverter(HoodieReaderContext<T> readerContext) {
      super(readerContext);
    }

    @Override
    public IteratorMode getIteratorMode() {
      return IteratorMode.HOODIE_RECORD;
    }

    @Override
    public HoodieRecord<T> convert(T row, Option<String> orderingFieldName, Supplier<Boolean> isDelete) {
      T sealedRow = readerContext.seal(row);
      T convertedRow = outputConverter.map(converter -> converter.apply(sealedRow)).orElse(sealedRow);
      BufferedRecord<T> bufferedRecord = BufferedRecord.forRecordWithContext(
          convertedRow, readerContext.getSchemaHandler().getRequestedSchema(), readerContext, orderingFieldName, isDelete.get());
      return readerContext.constructHoodieRecord(bufferedRecord);
    }

    @Override
    public HoodieRecord<T> convert(BufferedRecord<T> record) {
      // do not need seal for DELETE records as they come from buffered log records from spillable map
      BufferedRecord<T> sealedRecord = record.isDelete() ? record : record.seal(readerContext);
      record = outputConverter.map(
          converter -> sealedRecord.copy(
              converter.apply(sealedRecord.getRecord()),
              readerContext.getSchemaHandler().getRequestSchemaId())
          ).orElse(record);
      return readerContext.constructHoodieRecord(record);
    }
  }
}
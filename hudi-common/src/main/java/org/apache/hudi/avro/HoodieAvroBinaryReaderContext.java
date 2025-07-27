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

package org.apache.hudi.avro;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroBinaryRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class HoodieAvroBinaryReaderContext extends HoodieReaderContext<byte[]> {

  private final Map<StoragePath, HoodieAvroFileReader> reusableFileReaders;

  public HoodieAvroBinaryReaderContext(StorageConfiguration<?> storageConfiguration, HoodieTableConfig tableConfig,
                                       Option<InstantRange> instantRangeOpt, Option<Predicate> keyFilterOpt) {
    this(storageConfiguration, tableConfig, instantRangeOpt, keyFilterOpt, Collections.emptyMap());
  }

  /**
   * Constructs an instance of the reader context with an optional cache of reusable file readers.
   * This provides an opportunity for increased performance when repeatedly reading from the same files.
   * The caller of this constructor is responsible for managing the lifecycle of the reusable file readers.
   * @param storageConfiguration the storage configuration to use for reading files
   * @param tableConfig the configuration of the Hudi table being read
   * @param instantRangeOpt the set of valid instants for this read
   * @param filterOpt an optional filter to apply on the record keys
   * @param reusableFileReaders a map of reusable file readers, keyed by their storage paths.
   */
  public HoodieAvroBinaryReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt,
      Option<Predicate> filterOpt,
      Map<StoragePath, HoodieAvroFileReader> reusableFileReaders) {
    super(storageConfiguration, tableConfig, instantRangeOpt, filterOpt);
    this.typeConverter = new AvroReaderContextTypeConverter();
    this.reusableFileReaders = reusableFileReaders;
  }

  @Override
  public ClosableIterator<byte[]> getFileRecordIterator(StoragePath filePath, long start, long length, Schema dataSchema, Schema requiredSchema, HoodieStorage storage) throws IOException {
    HoodieAvroFileReader reader;
    if (reusableFileReaders.containsKey(filePath)) {
      reader = reusableFileReaders.get(filePath);
    } else {
      reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(new HoodieConfig(),
              filePath, baseFileFormat, Option.empty());
    }
    if (keyFilterOpt.isEmpty()) {
      return new CloseableMappingIterator(reader.getIndexedRecordIterator(dataSchema, requiredSchema), indexedRecord -> HoodieAvroUtils.avroToBytes((IndexedRecord) indexedRecord));
    }
    if (reader.supportKeyPredicate()) {
      List<String> keys = reader.extractKeys(keyFilterOpt);
      if (!keys.isEmpty()) {
        return new CloseableMappingIterator(reader.getIndexedRecordsByKeysIterator(keys, requiredSchema), indexedRecord -> HoodieAvroUtils.avroToBytes((IndexedRecord) indexedRecord));
      }
    }
    if (reader.supportKeyPrefixPredicate()) {
      List<String> keyPrefixes = reader.extractKeyPrefixes(keyFilterOpt);
      if (!keyPrefixes.isEmpty()) {
        return new CloseableMappingIterator(reader.getIndexedRecordsByKeyPrefixIterator(keyPrefixes, requiredSchema), indexedRecord -> HoodieAvroUtils.avroToBytes((IndexedRecord) indexedRecord));
      }
    }
    return new CloseableMappingIterator(reader.getIndexedRecordIterator(dataSchema, requiredSchema), indexedRecord -> HoodieAvroUtils.avroToBytes((IndexedRecord) indexedRecord));
  }

  @Override
  public byte[] convertAvroRecord(IndexedRecord avroRecord) {
    return HoodieAvroUtils.avroToBytes(avroRecord);
  }

  @Override
  public GenericRecord convertToAvroRecord(byte[] record, Schema schema) {
    try {
      return HoodieAvroUtils.bytesToAvro(record, schema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize to GenericRecord ", e);
    }
  }

  @Nullable
  @Override
  public byte[] getDeleteRow(byte[] record, String recordKey) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Object getValue(byte[] record, Schema schema, String fieldName) {
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(record, schema);
      // to fix. Consistent Logical timestamp.
      return HoodieAvroUtils.getNestedFieldVal(avroRecord,
        fieldName, true, false);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser record ", e);
    }
  }

  @Override
  public String getMetaFieldValue(byte[] record, int pos) {
    throw new HoodieIOException("Unsupported operation with HoodieAvroBinaryReaderContext");
  }

  @Override
  public HoodieRecord<byte[]> constructHoodieRecord(BufferedRecord<byte[]> bufferedRecord) {
    if (bufferedRecord.isDelete()) {
      return SpillableMapUtils.generateAvroBinaryForDelete(bufferedRecord.getRecordKey(), partitionPath, bufferedRecord.getOrderingValue());
    }
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    return new HoodieAvroBinaryRecord(hoodieKey, bufferedRecord.getRecord(), bufferedRecord.getOrderingValue());
  }

  @Override
  public byte[] constructEngineRecord(Schema schema, Map<Integer, Object> updateValues, BufferedRecord<byte[]> baseRecord) {
    try {
      IndexedRecord engineRecord = HoodieAvroUtils.bytesToAvro(baseRecord.getRecord(), schema);
      for (Map.Entry<Integer, Object> value : updateValues.entrySet()) {
        engineRecord.put(value.getKey(), value.getValue());
      }
      return HoodieAvroUtils.avroToBytes(engineRecord);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser Avro record ", e);
    }
  }

  @Override
  public byte[] seal(byte[] record) {
    return record;
  }

  @Override
  public byte[] toBinaryRow(Schema avroSchema, byte[] record) {
    return record;
  }

  @Override
  public ClosableIterator<byte[]> mergeBootstrapReaders(ClosableIterator<byte[]> skeletonFileIterator, Schema skeletonRequiredSchema, ClosableIterator<byte[]> dataFileIterator,
                                                        Schema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues) {
    return new BootstrapIterator(skeletonFileIterator, skeletonRequiredSchema, dataFileIterator, dataRequiredSchema, requiredPartitionFieldAndValues);
  }

  @Override
  public UnaryOperator<byte[]> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    if (!renamedColumns.isEmpty()) {
      throw new UnsupportedOperationException("Column renaming is not supported for the HoodieAvroReaderContext");
    }
    Map<String, Integer> fromFields = IntStream.range(0, from.getFields().size())
        .boxed()
        .collect(Collectors.toMap(
            i -> from.getFields().get(i).name(), i -> i));
    Map<String, Integer> toFields = IntStream.range(0, to.getFields().size())
        .boxed()
        .collect(Collectors.toMap(
            i -> to.getFields().get(i).name(), i -> i));

    // Check if source schema contains all fields from target schema.
    List<Schema.Field> missingFields = to.getFields().stream()
        .filter(f -> !fromFields.containsKey(f.name())).collect(Collectors.toList());
    if (!missingFields.isEmpty()) {
      throw new HoodieException("There are some fields missing in source schema: "
          + missingFields);
    }

    // Build the mapping from source schema to target schema.
    Map<Integer, Integer> fieldMap = toFields.entrySet().stream()
        .filter(e -> fromFields.containsKey(e.getKey()))
        .collect(Collectors.toMap(
            e -> fromFields.get(e.getKey()), Map.Entry::getValue));

    // Do the transformation.
    return recordBytes -> {
      try {
        IndexedRecord record = null;
        record = HoodieAvroUtils.bytesToAvro(recordBytes, to);
        IndexedRecord outputRecord = new GenericData.Record(to);
        for (int i = 0; i < from.getFields().size(); i++) {
          if (!fieldMap.containsKey(i)) {
            continue;
          }
          int j = fieldMap.get(i);
          outputRecord.put(j, record.get(i));
        }
        return HoodieAvroUtils.avroToBytes(outputRecord);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to deser avro record ", e);
      }
    };
  }

  /**
   * Iterator that traverses the skeleton file and the base file in tandem.
   * The iterator will only extract the fields requested in the provided schemas.
   */
  private static class BootstrapIterator implements ClosableIterator<byte[]> {
    private final ClosableIterator<byte[]> skeletonFileIterator;
    private final Schema skeletonRequiredSchema;
    private final ClosableIterator<byte[]> dataFileIterator;
    private final Schema dataRequiredSchema;
    private final Schema mergedSchema;
    private final int skeletonFields;
    private final int[] partitionFieldPositions;
    private final Object[] partitionValues;

    public BootstrapIterator(ClosableIterator<byte[]> skeletonFileIterator, Schema skeletonRequiredSchema,
                             ClosableIterator<byte[]> dataFileIterator, Schema dataRequiredSchema,
                             List<Pair<String, Object>> partitionFieldAndValues) {
      this.skeletonFileIterator = skeletonFileIterator;
      this.skeletonRequiredSchema = skeletonRequiredSchema;
      this.dataFileIterator = dataFileIterator;
      this.dataRequiredSchema = dataRequiredSchema;
      this.mergedSchema = AvroSchemaUtils.mergeSchemas(skeletonRequiredSchema, dataRequiredSchema);
      this.skeletonFields = skeletonRequiredSchema.getFields().size();
      this.partitionFieldPositions = partitionFieldAndValues.stream().map(Pair::getLeft).map(field -> mergedSchema.getField(field).pos()).mapToInt(Integer::intValue).toArray();
      this.partitionValues = partitionFieldAndValues.stream().map(Pair::getValue).toArray();
    }

    @Override
    public void close() {
      skeletonFileIterator.close();
      dataFileIterator.close();
    }

    @Override
    public boolean hasNext() {
      checkState(dataFileIterator.hasNext() == skeletonFileIterator.hasNext(),
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!");
      return skeletonFileIterator.hasNext();
    }

    @Override
    public byte[] next() {
      try {
        byte[] skeletonRecordBytes = skeletonFileIterator.next();
        IndexedRecord skeletonRecord = HoodieAvroUtils.bytesToAvro(skeletonRecordBytes, skeletonRequiredSchema);
        byte[] dataRecordBytes = dataFileIterator.next();
        IndexedRecord dataRecord = HoodieAvroUtils.bytesToAvro(dataRecordBytes, dataRequiredSchema);
        GenericRecord mergedRecord = new GenericData.Record(mergedSchema);

        for (Schema.Field skeletonField : skeletonRequiredSchema.getFields()) {
          Schema.Field sourceField = skeletonRecord.getSchema().getField(skeletonField.name());
          mergedRecord.put(skeletonField.pos(), skeletonRecord.get(sourceField.pos()));
        }
        for (Schema.Field dataField : dataRequiredSchema.getFields()) {
          Schema.Field sourceField = dataRecord.getSchema().getField(dataField.name());
          mergedRecord.put(dataField.pos() + skeletonFields, dataRecord.get(sourceField.pos()));
        }
        for (int i = 0; i < partitionFieldPositions.length; i++) {
          if (mergedRecord.get(partitionFieldPositions[i]) == null) {
            mergedRecord.put(partitionFieldPositions[i], partitionValues[i]);
          }
        }
        return HoodieAvroUtils.avroToBytes(mergedRecord);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to deser avro record ", e);
      }
    }
  }
}

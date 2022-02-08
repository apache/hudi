/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieRecordFileWriter;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.TypeUtils.unsafeCast;

public class HoodieAvroRecord<T extends HoodieRecordPayload> extends HoodieRecord<T> {
  public HoodieAvroRecord(HoodieKey key, T data) {
    super(key, data);
  }

  public HoodieAvroRecord(HoodieKey key, T data, HoodieOperation operation) {
    super(key, data, operation);
  }

  public HoodieAvroRecord(HoodieRecord<T> record) {
    super(record);
  }

  public HoodieAvroRecord() {
  }

  @Override
  public HoodieRecord<T> newInstance() {
    return new HoodieAvroRecord<>(this);
  }

  @Override
  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  @Override
  public void writeWithMetadata(HoodieFileWriter writer, Schema schema, Properties props) throws IOException {
    HoodieRecordFileWriter<IndexedRecord> avroWriter = unsafeCast(writer);
    IndexedRecord avroPayload = (IndexedRecord) getData().getInsertValue(schema, props).get();

    avroWriter.writeWithMetadata(avroPayload, this);
  }

  @Override
  public void write(HoodieFileWriter writer, Schema schema, Properties props) throws IOException {
    HoodieRecordFileWriter<IndexedRecord> avroWriter = unsafeCast(writer);
    IndexedRecord avroPayload = (IndexedRecord) getData().getInsertValue(schema, props).get();

    avroWriter.write(getRecordKey(), avroPayload);
  }

  // TODO remove
  public Option<GenericRecord> asAvro(Schema schema) throws IOException {
    return getData().getInsertValue(schema);
  }

  //////////////////////////////////////////////////////////////////////////////

  //
  // NOTE: This method duplicates those ones of the HoodieRecordPayload and are placed here
  //       for the duration of RFC-46 implementation, until migration off `HoodieRecordPayload`
  //       is complete
  //
  // TODO cleanup

  // NOTE: This method is assuming semantic that `preCombine` operation is bound to pick one or the other
  //       object, and may not create a new one
  @Override
  public HoodieRecord<T> preCombine(HoodieRecord<T> previousRecord) {
    T picked = unsafeCast(getData().preCombine(previousRecord.getData()));
    return picked.equals(getData()) ? this : previousRecord;
  }

  // NOTE: This method is assuming semantic that only records bearing the same (partition, key) could
  //       be combined
  @Override
  public Option<HoodieRecord<T>> combineAndGetUpdateValue(HoodieRecord<T> previousRecord, Schema schema, Properties props) throws IOException {
    ValidationUtils.checkState(Objects.equals(getKey(), previousRecord.getKey()));

    Option<IndexedRecord> previousRecordAvroPayload = previousRecord.getData().getInsertValue(schema, props);
    if (!previousRecordAvroPayload.isPresent()) {
      return Option.empty();
    }

    return getData().combineAndGetUpdateValue(previousRecordAvroPayload.get(), schema, props)
        .map(combinedAvroPayload -> {
          // NOTE: It's assumed that records aren't precombined more than once in its lifecycle,
          //       therefore we simply stub out precombine value here
          int newPreCombineVal = 0;
          T combinedPayload = instantiateRecordPayloadWrapper(combinedAvroPayload, newPreCombineVal);
          return new HoodieAvroRecord<>(getKey(), combinedPayload, getOperation());
        });
  }

  @Override
  public HoodieRecord mergeWith(HoodieRecord other, Schema readerSchema, Schema writerSchema) throws IOException {
    ValidationUtils.checkState(other instanceof HoodieAvroRecord);
    GenericRecord mergedPayload = HoodieAvroUtils.stitchRecords(
        asAvro(readerSchema).get(),
        ((HoodieAvroRecord<?>) other).asAvro(readerSchema).get(),
        writerSchema);
    return new HoodieAvroRecord(getKey(), instantiateRecordPayloadWrapper(mergedPayload, getPrecombineValue(getData())), getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema, TypedProperties props) throws IOException {
    Option<IndexedRecord> avroRecordPayloadOpt = getData().getInsertValue(recordSchema, props);
    GenericRecord avroPayloadInNewSchema =
        HoodieAvroUtils.rewriteRecord((GenericRecord) avroRecordPayloadOpt.get(), targetSchema);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(avroPayloadInNewSchema), getOperation());
  }

  @Override
  public HoodieRecord addMetadataValues(Map<HoodieMetadataField, String> metadataValues) throws IOException {
    // NOTE: RewriteAvroPayload is expected here
    GenericRecord avroRecordPayload = (GenericRecord) getData().getInsertValue(null).get();

    Arrays.stream(HoodieMetadataField.values()).forEach(metadataField -> {
      String value = metadataValues.get(metadataField);
      if (value != null) {
        avroRecordPayload.put(metadataField.getFieldName(), metadataValues.get(metadataField));
      }
    });

    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(avroRecordPayload), getOperation());
  }

  @Override
  public HoodieRecord overrideMetadataValue(HoodieMetadataField metadataField, String value) throws IOException {
    // NOTE: RewriteAvroPayload is expected here
    Option<IndexedRecord> avroPayloadOpt = getData().getInsertValue(null);
    IndexedRecord avroPayload = avroPayloadOpt.get();

    avroPayload.put(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(metadataField.getFieldName()), value);

    return new HoodieAvroRecord(getKey(), new RewriteAvroPayload((GenericRecord) avroPayload), getOperation());
  }

  public Option<Map<String, String>> getMetadata() {
    return getData().getMetadata();
  }

  @Override
  public boolean canBeIgnored() {
    return getData().canBeIgnored();
  }

  @Nonnull
  private T instantiateRecordPayloadWrapper(Object combinedAvroPayload, Comparable newPreCombineVal) {
    return unsafeCast(
        ReflectionUtils.loadPayload(
            getData().getClass().getCanonicalName(),
            new Object[]{combinedAvroPayload, newPreCombineVal},
            GenericRecord.class,
            Comparable.class));
  }

  private static <T extends HoodieRecordPayload> Comparable getPrecombineValue(T data) {
    if (data instanceof BaseAvroPayload) {
      return ((BaseAvroPayload) data).orderingVal;
    }

    return -1;
  }

  //////////////////////////////////////////////////////////////////////////////
}

/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util.collection.converter;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.exception.HoodieNotSerializableException;
import java.io.IOException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A default converter implementation for HoodieRecord
 */
public class HoodieRecordConverter<V> implements
    Converter<HoodieRecord<? extends HoodieRecordPayload>> {

  // Schema used to get GenericRecord from HoodieRecordPayload then convert to bytes and vice-versa
  private final Schema schema;
  // The client implementation of HoodieRecordPayload used to re-create HoodieRecord from bytes
  private final String payloadClazz;

  private static Logger log = LogManager.getLogger(HoodieRecordConverter.class);

  public HoodieRecordConverter(Schema schema, String payloadClazz) {
    this.schema = schema;
    this.payloadClazz = payloadClazz;
  }

  @Override
  public byte[] getBytes(HoodieRecord hoodieRecord) {
    try {
      // Need to initialize this to 0 bytes since deletes are handled by putting an empty record in HoodieRecord
      byte[] val = new byte[0];
      if (hoodieRecord.getData().getInsertValue(schema).isPresent()) {
        val = HoodieAvroUtils
            .avroToBytes((GenericRecord) hoodieRecord.getData().getInsertValue(schema).get());
      }
      Pair<Pair<String, String>, byte[]> data =
          Pair.of(Pair.of(hoodieRecord.getKey().getRecordKey(),
              hoodieRecord.getKey().getPartitionPath()), val);
      return SerializationUtils.serialize(data);
    } catch (IOException io) {
      throw new HoodieNotSerializableException("Cannot serialize value to bytes", io);
    }
  }

  @Override
  public HoodieRecord getData(byte[] bytes) {
    try {
      Pair<Pair<String, String>, byte[]> data = SerializationUtils.deserialize(bytes);
      Optional<GenericRecord> payload = Optional.empty();
      if (data.getValue().length > 0) {
        // This can happen if the record is deleted, the payload is optional with 0 bytes
        payload = Optional.of(HoodieAvroUtils.bytesToAvro(data.getValue(), schema));
      }
      HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = new HoodieRecord<>(
          new HoodieKey(data.getKey().getKey(), data.getKey().getValue()),
          ReflectionUtils
              .loadPayload(payloadClazz,
                  new Object[]{payload}, Optional.class));
      return hoodieRecord;
    } catch (IOException io) {
      throw new HoodieNotSerializableException("Cannot de-serialize value from bytes", io);
    }
  }
}

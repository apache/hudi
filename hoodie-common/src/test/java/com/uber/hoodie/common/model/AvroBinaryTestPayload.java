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

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

public class AvroBinaryTestPayload implements HoodieRecordPayload {

  private final byte[] recordBytes;

  public AvroBinaryTestPayload(Optional<GenericRecord> record) {

    try {
      if (record.isPresent()) {
        recordBytes = HoodieAvroUtils.avroToBytes(record.get());
      } else {
        recordBytes = new byte[0];
      }
    } catch (IOException io) {
      throw new HoodieIOException("unable to convert payload to bytes");
    }
  }

  @Override
  public HoodieRecordPayload preCombine(HoodieRecordPayload another) {
    return this;
  }

  @Override
  public Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    return Optional.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));
  }
}
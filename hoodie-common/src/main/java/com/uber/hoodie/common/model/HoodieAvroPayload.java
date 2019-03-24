/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

/**
 * This is a payload to wrap a existing Hoodie Avro Record. Useful to create a HoodieRecord over
 * existing GenericRecords in a hoodie datasets (useful in compactions)
 */
public class HoodieAvroPayload implements HoodieRecordPayload<HoodieAvroPayload> {

  private static final long serialVersionUID = -505257251712222212L;
  /**
   * Store the GenericRecord converted to bytes
   * <ol>
   * <li>Doesn't store schema hence memory efficient</li>
   * <li>Makes the payload java serializable</li>
   * </ol>
   */
  private byte[] recordBytes;

  // To be used only for serialization
  public HoodieAvroPayload() {}

  public HoodieAvroPayload(Optional<GenericRecord> record) {
    try {
      if (record.isPresent()) {
        this.recordBytes = HoodieAvroUtils.avroToBytes(record.get());
      } else {
        this.recordBytes = new byte[0];
      }
    } catch (IOException io) {
      throw new HoodieIOException("Cannot convert record to bytes", io);
    }
  }

  @Override
  public HoodieAvroPayload preCombine(HoodieAvroPayload another) {
    return this;
  }

  @Override
  public Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
      throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Optional.empty();
    }
    return Optional.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));
  }

  @Override
  public HoodieAvroPayload fromBytes(byte[] bytes) {
    this.recordBytes = bytes;
    return this;
  }

  @Override
  public byte[] getBytes() {
    return this.recordBytes;
  }
}

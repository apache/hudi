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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * This is a payload to wrap a existing Hoodie Avro Record. Useful to create a HoodieRecord over existing GenericRecords
 * in a hoodie datasets (useful in compactions)
 */
public class HoodieAvroPayload implements HoodieRecordPayload<HoodieAvroPayload> {

  // Store the GenericRecord converted to bytes - 1) Doesn't store schema hence memory efficient 2) Makes the payload
  // java serializable
  private final byte[] recordBytes;

  public HoodieAvroPayload(Option<GenericRecord> record) {
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
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    return Option.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));
  }
}

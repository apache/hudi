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

import org.apache.hudi.common.serialization.RecordSerializer;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.function.Function;

/**
 * An implementation of {@link RecordSerializer} for Avro {@link IndexedRecord}.
 */
public class AvroRecordSerializer implements RecordSerializer<IndexedRecord> {
  private final Function<Integer, Schema> schemaFunc;

  public AvroRecordSerializer(Function<Integer, Schema> schemaFunc) {
    this.schemaFunc = schemaFunc;
  }

  @Override
  public byte[] serialize(IndexedRecord input) {
    return HoodieAvroUtils.avroToBytes(input);
  }

  @Override
  public IndexedRecord deserialize(byte[] bytes, int schemaId) {
    try {
      return HoodieAvroUtils.bytesToAvro(bytes, schemaFunc.apply(schemaId));
    } catch (IOException e) {
      throw new HoodieException("Failed to deserialize Avro record bytes.",e);
    }
  }
}

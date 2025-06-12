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

import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.serialization.RecordSerializer;
import org.apache.hudi.common.util.SerializationUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * An implementation of {@link CustomSerializer} for {@link BufferedRecord}.
 *
 */
public class BufferedRecordSerializer<T> implements CustomSerializer<BufferedRecord<T>> {
  public static final int KRYO_SERIALIZER_INITIAL_BUFFER_SIZE = 1048576;
  private final Kryo kryo;
  // Caching ByteArrayOutputStream to avoid recreating it for every operation
  private final ByteArrayOutputStream baos;
  private final RecordSerializer<T> recordSerializer;

  public BufferedRecordSerializer(RecordSerializer<T> recordSerializer) {
    SerializationUtils.KryoInstantiator kryoInstantiator = new SerializationUtils.KryoInstantiator();
    this.kryo = kryoInstantiator.newKryo();
    this.baos = new ByteArrayOutputStream(KRYO_SERIALIZER_INITIAL_BUFFER_SIZE);
    this.kryo.setRegistrationRequired(false);
    this.recordSerializer = recordSerializer;
  }

  @Override
  public byte[] serialize(BufferedRecord<T> record) throws IOException {
    kryo.reset();
    baos.reset();
    try (Output output = new Output(baos)) {
      output.writeString(record.getRecordKey());
      output.writeInt(record.getSchemaId());
      output.writeBoolean(record.isDelete());
      kryo.writeClassAndObject(output, record.getOrderingValue());

      byte[] avroBytes = recordSerializer.serialize(record.getRecord());
      output.writeInt(avroBytes.length);
      output.writeBytes(avroBytes);
    }
    return baos.toByteArray();
  }

  @Override
  public BufferedRecord<T> deserialize(byte[] bytes) {
    try (Input input = new Input(bytes)) {
      String recordKey = input.readString();
      int schemaId = input.readInt();
      boolean isDelete = input.readBoolean();
      Comparable orderingValue = (Comparable) kryo.readClassAndObject(input);

      int recordLength = input.readInt();
      byte[] recordBytes = input.readBytes(recordLength);
      T record = recordSerializer.deserialize(recordBytes, schemaId);
      return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, isDelete);
    }
  }
}

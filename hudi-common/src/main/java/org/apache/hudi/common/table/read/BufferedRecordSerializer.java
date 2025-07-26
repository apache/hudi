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
 */
public class BufferedRecordSerializer<T> implements CustomSerializer<BufferedRecord<T>> {
  // Caching kryo serializer to avoid creating kryo instance for every serde operation
  private static final ThreadLocal<InternalSerializerInstance> SERIALIZER_REF =
      ThreadLocal.withInitial(InternalSerializerInstance::new);
  private final RecordSerializer<T> recordSerializer;

  public BufferedRecordSerializer(RecordSerializer<T> recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  private static class InternalSerializerInstance {
    private static final int KRYO_SERIALIZER_INITIAL_BUFFER_SIZE = 1048576;
    private final Kryo kryo;
    // Caching ByteArrayOutputStream to avoid recreating it for every operation
    private final ByteArrayOutputStream baos;

    public InternalSerializerInstance() {
      SerializationUtils.KryoInstantiator kryoInstantiator = new SerializationUtils.KryoInstantiator();
      this.kryo = kryoInstantiator.newKryo();
      this.baos = new ByteArrayOutputStream(KRYO_SERIALIZER_INITIAL_BUFFER_SIZE);
      this.kryo.setRegistrationRequired(false);
    }

    <T> byte[] serialize(BufferedRecord<T> record, RecordSerializer<T> recordSerializer) {
      kryo.reset();
      baos.reset();
      try (Output output = new Output(baos)) {
        output.writeString(record.getRecordKey());
        boolean hasSchemaId = record.getSchemaId() != null;
        output.writeBoolean(hasSchemaId);
        if (hasSchemaId) {
          output.writeVarInt(record.getSchemaId(), true);
        }
        output.writeBoolean(record.isDelete());
        kryo.writeClassAndObject(output, record.getOrderingValue());

        byte[] recordBytes = record.getRecord() == null ? new byte[0] : recordSerializer.serialize(record.getRecord());
        output.writeVarInt(recordBytes.length, true);
        output.writeBytes(recordBytes);
      }
      return baos.toByteArray();
    }

    <T> BufferedRecord<T> deserialize(byte[] bytes, RecordSerializer<T> recordSerializer) {
      try (Input input = new Input(bytes)) {
        String recordKey = input.readString();
        boolean hasSchemaId = input.readBoolean();
        Integer schemaId = null;
        if (hasSchemaId) {
          schemaId = input.readVarInt(true);
        }
        boolean isDelete = input.readBoolean();
        Comparable orderingValue = (Comparable) kryo.readClassAndObject(input);

        T record;
        // Read the length of the serialized record
        int recordLength = input.readVarInt(true);
        if (recordLength == 0) {
          record = null;
        } else {
          record = recordSerializer.deserialize(input.readBytes(recordLength), schemaId);
        }
        return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, isDelete);
      }
    }
  }

  @Override
  public byte[] serialize(BufferedRecord<T> record) throws IOException {
    return SERIALIZER_REF.get().serialize(record, recordSerializer);
  }

  @Override
  public BufferedRecord<T> deserialize(byte[] bytes) {
    return SERIALIZER_REF.get().deserialize(bytes, recordSerializer);
  }
}

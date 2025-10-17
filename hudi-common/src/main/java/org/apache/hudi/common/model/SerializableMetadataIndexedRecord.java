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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.hfile.KeyValue;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;

public class SerializableMetadataIndexedRecord implements GenericRecord, KryoSerializable, Serializable {
  private static final long serialVersionUID = 1L;
  private static final ConcurrentHashMap<Schema, GenericDatumReader<GenericRecord>> CACHED_DATUM_READER_MAP = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Schema, Schema.Field> CACHED_KEY_SCHEMA_MAP = new ConcurrentHashMap<>();
  private IndexedRecord record;
  private Schema schema;
  String key;
  private byte[] keyValueBytes;
  int valueOffset;
  int valueLength;

  private SerializableMetadataIndexedRecord(Schema schema,
                                            String key,
                                            byte[] keyValueBytes,
                                            int valueOffset,
                                            int valueLength) {
    this.key = key;
    this.keyValueBytes = keyValueBytes;
    this.valueOffset = valueOffset;
    this.valueLength = valueLength;
    this.schema = schema;
    this.record = null;
  }

  public static SerializableMetadataIndexedRecord fromHFileKeyValueBytes(Schema schema,
                                                                         Schema.Field keyFieldSchema,
                                                                         KeyValue hfileKeyValue) {
    CACHED_KEY_SCHEMA_MAP.computeIfAbsent(schema, k -> keyFieldSchema);
    return new SerializableMetadataIndexedRecord(
        schema,
        fromUTF8Bytes(hfileKeyValue.getBytes(), hfileKeyValue.getKeyContentOffset(), hfileKeyValue.getKeyContentLength()),
        hfileKeyValue.getBytes(), hfileKeyValue.getValueOffset(),
        hfileKeyValue.getValueLength());
  }

  @Override
  public void put(int i, Object v) {
    getData().put(i, v);
  }

  @Override
  public Object get(int i) {
    if (i == CACHED_KEY_SCHEMA_MAP.get(schema).pos()) {
      return key;
    }
    return getData().get(i);
  }

  @Override
  public Schema getSchema() {
    return schema != null ? schema : record.getSchema();
  }

  byte[] encodeRecord() {
    if (keyValueBytes == null) {
      // TODO(yihua): fix this, though this should not be triggered
      throw new IllegalStateException("This should not be called");
    }
    return keyValueBytes;
  }

  void decodeRecord(Schema schema) {
    if (record == null) {
      this.schema = schema;
    }
  }

  @VisibleForTesting
  public IndexedRecord getData() {
    if (record == null) {
      try {
        GenericDatumReader<GenericRecord> datumReader = CACHED_DATUM_READER_MAP.computeIfAbsent(
            schema, GenericDatumReader::new);
        record = HoodieAvroHFileReaderImplBase.deserialize(
            key, keyValueBytes, valueOffset, valueLength, datumReader,
            CACHED_KEY_SCHEMA_MAP.get(schema));
        keyValueBytes = null;
        schema = null;
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse record into provided schema", e);
      }
    }
    return record;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    byte[] bytes = encodeRecord();
    output.writeInt(bytes.length, true);
    output.writeBytes(bytes);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int length = input.readInt(true);
    this.keyValueBytes = input.readBytes(length);
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    byte[] bytes = encodeRecord();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private void readObject(ObjectInputStream ois)
      throws ClassNotFoundException, IOException {
    int length = ois.readInt();
    this.keyValueBytes = new byte[length];
    ois.read(keyValueBytes, 0, length);
  }

  @Override
  public void put(String key, Object v) {
    getData();
    Schema.Field field = record.getSchema().getField(key);
    record.put(field.pos(), v);
  }

  @Override
  public Object get(String key) {
    if (CACHED_KEY_SCHEMA_MAP.get(schema).name().equals(key)) {
      return key;
    }
    getData();
    Schema.Field field = record.getSchema().getField(key);
    return record.get(field.pos());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (o instanceof SerializableMetadataIndexedRecord) {
      SerializableMetadataIndexedRecord that = (SerializableMetadataIndexedRecord) o;
      getData();
      ValidationUtils.checkArgument(record != null && that.record != null, "Records must be deserialized before equality check");
      return record.equals(that.record);
    } else if (o instanceof IndexedRecord) {
      // If the other object is an IndexedRecord, we can compare it directly
      IndexedRecord that = (IndexedRecord) o;
      return record != null && record.equals(that);
    }
    return false;
  }

  IndexedRecord getRecord() {
    ValidationUtils.checkArgument(record != null || schema != null, "Record must be deserialized before accessing");
    return getData();
  }

  @Override
  public int hashCode() {
    return Objects.hash(record);
  }

  @Override
  public String toString() {
    return record == null ? "SERIALIZED: " + new String(keyValueBytes) : record.toString();
  }

}

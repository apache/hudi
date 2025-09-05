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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * Wrapper class for {@link IndexedRecord} that will serialize the record as bytes without the schema for efficient shuffling performance in Spark.
 * After deserialization, the schema can be set using {@link #decodeRecord(Schema)} and that will also trigger deserialization of the record.
 * This allows the record to stay in the serialized form until the data needs to be accessed, which allows deserialization to be avoided if data is not read.
 */
public class SerializableIndexedRecord implements GenericRecord, KryoSerializable, Serializable {
  private static final long serialVersionUID = 1L;

  private IndexedRecord record;
  private byte[] recordBytes;

  static SerializableIndexedRecord createInstance(IndexedRecord record, boolean validateRecord) {
    return record == null ? null : new SerializableIndexedRecord(record, validateRecord);
  }

  private SerializableIndexedRecord(IndexedRecord record, boolean validateRecord) {
    if (validateRecord) {
      Schema schema = record.getSchema();
      if (!GenericData.get().validate(schema, record)) {
        throw new HoodieException("The record does not match the schema: " + schema);
      }
    }
    this.record = record;
    this.recordBytes = null; // Initialize recordBytes to null, will be set when encodeRecord is called
  }

  @Override
  public void put(int i, Object v) {
    record.put(i, v);
  }

  @Override
  public Object get(int i) {
    return record.get(i);
  }

  @Override
  public Schema getSchema() {
    return record.getSchema();
  }

  byte[] encodeRecord() {
    if (recordBytes == null) {
      recordBytes = HoodieAvroUtils.avroToBytes(record);
    }
    return recordBytes;
  }

  void decodeRecord(Schema schema) {
    if (record == null) {
      try {
        record = HoodieAvroUtils.bytesToAvro(recordBytes, schema);
        recordBytes = null;
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse record into provided schema", e);
      }
    }
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
    this.recordBytes = input.readBytes(length);
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    byte[] bytes = encodeRecord();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private void readObject(ObjectInputStream ois)
      throws ClassNotFoundException, IOException {
    int length = ois.readInt();
    this.recordBytes = new byte[length];
    ois.read(recordBytes, 0, length);
  }

  @Override
  public void put(String key, Object v) {
    Schema.Field field = record.getSchema().getField(key);
    record.put(field.pos(), v);
  }

  @Override
  public Object get(String key) {
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
    if (o instanceof SerializableIndexedRecord) {
      SerializableIndexedRecord that = (SerializableIndexedRecord) o;
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
    ValidationUtils.checkArgument(record != null, "Record must be deserialized before accessing");
    return record;
  }

  @Override
  public int hashCode() {
    return Objects.hash(record);
  }

  @Override
  public String toString() {
    return record == null ? "SERIALIZED: " + new String(recordBytes) : record.toString();
  }
}

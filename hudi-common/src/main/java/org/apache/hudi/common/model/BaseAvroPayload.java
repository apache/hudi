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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Base class for all AVRO record based payloads, that can be ordered based on a field.
 */
public abstract class BaseAvroPayload implements Serializable, KryoSerializable {
  /**
   * Avro data extracted from the source converted to bytes.
   */
  private byte[] recordBytes;

  /**
   * For purposes of preCombining.
   */
  protected Comparable orderingVal;

  protected boolean isDeletedRecord;

  private transient GenericRecord record;

  /**
   * Instantiate {@link BaseAvroPayload}.
   *
   * @param record      Generic record for the payload.
   * @param orderingVal {@link Comparable} to be used in pre combine.
   */
  public BaseAvroPayload(GenericRecord record, Comparable orderingVal) {
    this.record = record;
    this.recordBytes = null; // only initialized when needed
    this.orderingVal = orderingVal;
    this.isDeletedRecord = record == null || isDeleteRecord(record);

    if (orderingVal == null) {
      throw new HoodieException("Ordering value is null for record: " + record);
    }
  }

  public Comparable getOrderingVal() {
    return orderingVal;
  }

  /**
   * Defines whether this implementation of {@link HoodieRecordPayload} is deleted.
   * We will not do deserialization in this method.
   */
  public boolean isDeleted(Schema schema, Properties props) {
    return isDeletedRecord;
  }

  /**
   * Defines whether this implementation of {@link HoodieRecordPayload} could produce
   * {@link HoodieRecord#SENTINEL}
   */
  public boolean canProduceSentinel() {
    return false;
  }

  /**
   * @param genericRecord instance of {@link GenericRecord} of interest.
   * @returns {@code true} if record represents a delete record. {@code false} otherwise.
   */
  protected boolean isDeleteRecord(GenericRecord genericRecord) {
    final String isDeleteKey = HoodieRecord.HOODIE_IS_DELETED_FIELD;
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(isDeleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(isDeleteKey);
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  public byte[] getRecordBytes() {
    if (recordBytes == null) {
      if (record == null) {
        recordBytes = new byte[0];
      } else {
        recordBytes = HoodieAvroUtils.avroToBytes(record);
      }
    }
    return recordBytes;
  }

  public Option<IndexedRecord> getIndexedRecord(Schema schema, Properties properties) throws IOException {
    return getRecord(schema);
  }

  protected boolean isEmptyRecord() {
    if (recordBytes == null) {
      return record == null;
    }
    return recordBytes.length == 0;
  }

  protected Option<IndexedRecord> getRecord(Schema schema) throws IOException {
    if (record != null) {
      if (record.getSchema() == schema) {
        return Option.of(record);
      }
      // if the schema does not match, we need to deserialize with the proper schema to match legacy behavior
      recordBytes = getRecordBytes();
    }
    if (recordBytes == null || recordBytes.length == 0) {
      return Option.empty();
    }
    record = HoodieAvroUtils.bytesToAvro(recordBytes, schema);
    return Option.of(record);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    byte[] bytes = getRecordBytes();
    output.writeInt(bytes.length);
    output.writeBytes(bytes);
    kryo.writeClassAndObject(output, orderingVal);
    output.writeBoolean(isDeletedRecord);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int length = input.readInt();
    this.recordBytes = input.readBytes(length);
    this.orderingVal = (Comparable) kryo.readClassAndObject(input);
    this.isDeletedRecord = input.readBoolean();
  }
}

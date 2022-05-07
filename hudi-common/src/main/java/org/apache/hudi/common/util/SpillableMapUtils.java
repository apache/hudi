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

package org.apache.hudi.common.util;

import org.apache.hudi.common.fs.SizeAwareDataOutputStream;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.BitCaskDiskMap.FileEntry;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieCorruptedDataException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.RandomAccessFile;

import static org.apache.hudi.avro.HoodieAvroUtils.getNullableValAsString;
import static org.apache.hudi.common.util.BinaryUtil.generateChecksum;

/**
 * A utility class supports spillable map.
 */
public class SpillableMapUtils {

  /**
   * Using the schema and payload class, read and convert the bytes on disk to a HoodieRecord.
   */
  public static byte[] readBytesFromDisk(RandomAccessFile file, long valuePosition, int valueLength)
      throws IOException {
    FileEntry fileEntry = readInternal(file, valuePosition, valueLength);
    return fileEntry.getValue();
  }

  /**
   * Reads the given file with specific pattern(|crc|timestamp|sizeOfKey|SizeOfValue|key|value|) then
   * returns an instance of {@link FileEntry}.
   */
  private static FileEntry readInternal(RandomAccessFile file, long valuePosition, int valueLength) throws IOException {
    file.seek(valuePosition);
    long crc = file.readLong();
    long timestamp = file.readLong();
    int keySize = file.readInt();
    int valueSize = file.readInt();
    byte[] key = new byte[keySize];
    file.readFully(key, 0, keySize);
    byte[] value = new byte[valueSize];
    if (valueSize != valueLength) {
      throw new HoodieCorruptedDataException("unequal size of payload written to external file, data may be corrupted");
    }
    file.readFully(value, 0, valueSize);
    long crcOfReadValue = generateChecksum(value);
    if (crc != crcOfReadValue) {
      throw new HoodieCorruptedDataException(
          "checksum of payload written to external disk does not match, data may be corrupted");
    }
    return new FileEntry(crc, keySize, valueSize, key, value, timestamp);
  }

  /**
   * Write Value and other metadata necessary to disk. Each entry has the following sequence of data
   * <p>
   * |crc|timestamp|sizeOfKey|SizeOfValue|key|value|
   */
  public static long spillToDisk(SizeAwareDataOutputStream outputStream, FileEntry fileEntry) throws IOException {
    return spill(outputStream, fileEntry);
  }

  private static long spill(SizeAwareDataOutputStream outputStream, FileEntry fileEntry) throws IOException {
    outputStream.writeLong(fileEntry.getCrc());
    outputStream.writeLong(fileEntry.getTimestamp());
    outputStream.writeInt(fileEntry.getSizeOfKey());
    outputStream.writeInt(fileEntry.getSizeOfValue());
    outputStream.write(fileEntry.getKey());
    outputStream.write(fileEntry.getValue());
    return outputStream.getSize();
  }

  /**
   * Compute a bytes representation of the payload by serializing the contents This is used to estimate the size of the
   * payload (either in memory or when written to disk).
   */
  public static <R> long computePayloadSize(R value, SizeEstimator<R> valueSizeEstimator) throws IOException {
    return valueSizeEstimator.sizeEstimate(value);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static <R> R convertToHoodieRecordPayload(GenericRecord rec, String payloadClazz, String preCombineField, boolean withOperationField) {
    return convertToHoodieRecordPayload(rec, payloadClazz, preCombineField,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, Option.empty());
  }

  public static <R> R convertToHoodieRecordPayload(GenericRecord record, String payloadClazz,
                                                   String preCombineField,
                                                   boolean withOperationField,
                                                   Option<String> partitionName) {
    return convertToHoodieRecordPayload(record, payloadClazz, preCombineField,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, partitionName);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static <R> R convertToHoodieRecordPayload(GenericRecord record, String payloadClazz,
                                                   String preCombineField,
                                                   Pair<String, String> recordKeyPartitionPathFieldPair,
                                                   boolean withOperationField,
                                                   Option<String> partitionName) {
    final String recKey = record.get(recordKeyPartitionPathFieldPair.getKey()).toString();
    final String partitionPath = (partitionName.isPresent() ? partitionName.get() :
        record.get(recordKeyPartitionPathFieldPair.getRight()).toString());

    Object preCombineVal = getPreCombineVal(record, preCombineField);
    HoodieOperation operation = withOperationField
        ? HoodieOperation.fromName(getNullableValAsString(record, HoodieRecord.OPERATION_METADATA_FIELD)) : null;
    HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = new HoodieAvroRecord<>(new HoodieKey(recKey, partitionPath),
        ReflectionUtils.loadPayload(payloadClazz, new Object[]{record, preCombineVal}, GenericRecord.class,
            Comparable.class), operation);

    return (R) hoodieRecord;
  }

  /**
   * Returns the preCombine value with given field name.
   *
   * @param rec The avro record
   * @param preCombineField The preCombine field name
   * @return the preCombine field value or 0 if the field does not exist in the avro schema
   */
  private static Object getPreCombineVal(GenericRecord rec, String preCombineField) {
    if (preCombineField == null) {
      return 0;
    }
    Schema.Field field = rec.getSchema().getField(preCombineField);
    return field == null ? 0 : rec.get(field.pos());
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static <R> R generateEmptyPayload(String recKey, String partitionPath, Comparable orderingVal, String payloadClazz) {
    HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = new HoodieAvroRecord<>(new HoodieKey(recKey, partitionPath),
        ReflectionUtils.loadPayload(payloadClazz, new Object[] {null, orderingVal}, GenericRecord.class, Comparable.class));
    return (R) hoodieRecord;
  }
}

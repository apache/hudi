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
package com.uber.hoodie.common.util;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.collection.DiskBasedMap;
import com.uber.hoodie.common.util.collection.io.storage.SizeAwareDataOutputStream;
import com.uber.hoodie.exception.HoodieCorruptedDataException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;
import java.util.zip.CRC32;

public class SpillableMapUtils {

  public static ObjectMapper objectMapper = new ObjectMapper();
  /**
   * Using the schema and payload class, read and convert the bytes on disk to a HoodieRecord
   * @param file
   * @param schema
   * @param payloadClazz
   * @param valuePosition
   * @param valueLength
   * @param <R>
   * @return
   * @throws IOException
   */
  public static <R> R readFromDisk(RandomAccessFile file, Schema schema, String payloadClazz,
                                   long valuePosition, int valueLength) throws IOException {

    DiskBasedMap.FileEntry fileEntry = readInternal(file, valuePosition, valueLength);
    return (R) convertToHoodieRecordPayload(HoodieAvroUtils.bytesToAvro(fileEntry.getValue(), schema),
        payloadClazz);
  }

  /**
   * |crc|timestamp|sizeOfKey|SizeOfValue|key|value|
   * @param file
   * @param valuePosition
   * @param valueLength
   * @return
   * @throws IOException
   */
  private static DiskBasedMap.FileEntry readInternal(RandomAccessFile file, long valuePosition, int valueLength) throws IOException {
    file.seek(valuePosition);
    long crc = file.readLong();
    long timestamp = file.readLong();
    int keySize = file.readInt();
    int valueSize = file.readInt();
    byte [] key = new byte[keySize];
    file.read(key, 0, keySize);
    byte [] value = new byte[valueSize];
    if(!(valueSize == valueLength)) {
      throw new HoodieCorruptedDataException("unequal size of payload written to external file, data may be corrupted");
    }
    file.read(value, 0, valueSize);
    long crcOfReadValue = generateChecksum(value);
    if(!(crc == crcOfReadValue)) {
      throw new HoodieCorruptedDataException("checksum of payload written to external disk does not match, " +
          "data may be corrupted");
    }
    return new DiskBasedMap.FileEntry(crc, keySize, valueSize, key, value, timestamp);
  }

  /**
   * Write Value and other metadata necessary to disk. Each entry has the following sequence of data
   *
   * |crc|timestamp|sizeOfKey|SizeOfValue|key|value|
   *
   * @param outputStream
   * @param fileEntry
   * @return
   * @throws IOException
   */
  public static long spillToDisk(SizeAwareDataOutputStream outputStream, DiskBasedMap.FileEntry fileEntry) throws IOException {
    return spill(outputStream, fileEntry);
  }

  private static long spill(SizeAwareDataOutputStream outputStream, DiskBasedMap.FileEntry fileEntry)
      throws IOException {
    outputStream.writeLong(fileEntry.getCrc());
    outputStream.writeLong(fileEntry.getTimestamp());
    outputStream.writeInt(fileEntry.getSizeOfKey());
    outputStream.writeInt(fileEntry.getSizeOfValue());
    outputStream.write(fileEntry.getKey());
    outputStream.write(fileEntry.getValue());
    return outputStream.getSize();
  }

  /**
   * Generate a checksum for a given set of bytes
   * @param data
   * @return
   */
  public static long generateChecksum(byte [] data) {
    CRC32 crc = new CRC32();
    crc.update(data);
    return crc.getValue();
  }

  /**
   * Compute a bytes representation of the payload by serializing the contents
   * This is used to estimate the size of the payload (either in memory or when written to disk)
   * @param <R>
   * @param value
   * @param schema
   * @return
   * @throws IOException
   */
  public static <R> int computePayloadSize(R value, Schema schema) throws IOException {
    HoodieRecord payload = (HoodieRecord) value;
    byte [] val = HoodieAvroUtils.avroToBytes((GenericRecord) payload.getData().getInsertValue(schema).get());
    return val.length;
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class
   * @param rec
   * @param payloadClazz
   * @param <R>
   * @return
   * @throws IOException
   */
  public static <R> R convertToHoodieRecordPayload(GenericRecord rec, String payloadClazz) {
    String recKey = rec.get(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .toString();
    String partitionPath =
        rec.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
            .toString();
    HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = new HoodieRecord<>(
        new HoodieKey(recKey, partitionPath),
        ReflectionUtils
            .loadPayload(payloadClazz, new Object[]{Optional.of(rec)}, Optional.class));
    return (R) hoodieRecord;
  }
}
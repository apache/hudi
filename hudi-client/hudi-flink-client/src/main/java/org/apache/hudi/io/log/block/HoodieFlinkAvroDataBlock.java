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

package org.apache.hudi.io.log.block;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.util.HoodieRowDataUtil;
import org.apache.hudi.util.RowDataToAvroConverters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *  HoodieFlinkAvroDataBlock is a HoodieAvroDataBlock used for Flink writing log with Avro format,
 *  where records are serialized using a RowDataToAvroConverter created at data block level.
 */
public class HoodieFlinkAvroDataBlock extends HoodieAvroDataBlock {

  public HoodieFlinkAvroDataBlock(
      @NotNull List<HoodieRecord> records,
      @NotNull Map<HeaderMetadataType, String> header,
      @NotNull String keyField) {
    super(records, header, keyField);
  }

  @Override
  protected byte[] serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException {
    Schema schema = AvroSchemaCache.intern(new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA)));
    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (DataOutputStream output = new DataOutputStream(baos)) {
      // 1. Write out the log block version
      output.writeInt(HoodieLogBlock.version);

      // 2. Write total number of records
      output.writeInt(records.size());

      // 3. Write the records
      boolean utcTimezone = storage.getConf().getBoolean(
          HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue());
      RowDataToAvroConverters.RowDataToAvroConverter rowDataToAvroConverter = HoodieRowDataUtil.getRowDataToAvroConverter(schema, utcTimezone);
      for (HoodieRecord<?> s : records) {
        ByteArrayOutputStream temp = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(temp, encoderCache.get());
        encoderCache.set(encoder);
        try {
          // we do not use HoodieRecord#toIndexedRecord() here because `rowDataToAvroConverter` should be created
          // at data block level, not at record level
          IndexedRecord data = (IndexedRecord) rowDataToAvroConverter.convert(schema, s.getData());
          writer.write(data, encoder);
          encoder.flush();

          // Write the record size
          output.writeInt(temp.size());
          // Write the content
          temp.writeTo(output);
        } catch (IOException e) {
          throw new HoodieIOException("IOException converting HoodieAvroDataBlock to bytes", e);
        }
      }
      encoderCache.remove();
    }
    return baos.toByteArray();
  }
}

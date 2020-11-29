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

package org.apache.hudi.io.storage;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.common.TaskContextSupplier;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class HoodieOrcWriter<T extends HoodieRecordPayload, R extends IndexedRecord>
    implements HoodieFileWriter<R> {

  private Writer writer = null;

  private final long maxFileSize;
  private final long orcStripeSize;
  private final long orcBlockSize;

  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final String commitTime;
  private final Configuration conf;
  private final TaskContextSupplier taskContextSupplier;

  private static final AtomicLong RECORD_INDEX = new AtomicLong(1);

  public HoodieOrcWriter(String commitTime, Path file, HoodieWriteConfig config,
                   Configuration hadoopConfig, TaskContextSupplier taskContextSupplier) throws IOException {
    // default value is 64M
    this.orcStripeSize = config.getOrcStripeSize();
    // default value is 256M
    this.orcBlockSize = config.getOrcBlockSize();
    this.maxFileSize = orcBlockSize + (2 * orcStripeSize);

    conf = OrcUtils.registerFileSystem(file, hadoopConfig);
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, conf);
    this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(conf);
    this.commitTime = commitTime;
    this.taskContextSupplier = taskContextSupplier;

  }

  @Override
  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {

    GenericRecord genericRecord = (GenericRecord) avroRecord;

    String seqId = HoodieRecord.generateSequenceId(commitTime, taskContextSupplier.getPartitionIdSupplier().get(),
        RECORD_INDEX.getAndIncrement());
    HoodieAvroUtils.addHoodieKeyToRecord(genericRecord, record.getRecordKey(),
        record.getPartitionPath(), file.getName());
    HoodieAvroUtils
        .addCommitMetadataToRecord(genericRecord, commitTime, seqId);

    writeAvro(null, avroRecord);
  }

  @Override
  public boolean canWrite() {
    if (writer == null) {
      return true;
    }
    return fs.getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void writeAvro(String key, R avroRecord) throws IOException {

    Schema avroSchema = avroRecord.getSchema();
    TypeInfo orcSchema = OrcUtils.getOrcField(avroSchema);

    if (Objects.isNull(writer)) {
      writer = OrcUtils.createWriter(
          file,
          conf,
          orcSchema,
          orcStripeSize,
          CompressionKind.ZLIB,
          (int) orcBlockSize,
          fs
      );
    }

    GenericRecord genericRecord = (GenericRecord) avroRecord;
    List<Schema.Field> fields = avroRecord.getSchema().getFields();

    if (fields != null) {
      Object[] row = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        Schema.Field field = fields.get(i);
        Schema fieldSchema = field.schema();
        Object o = genericRecord.get(field.name());
        try {
          row[i] = OrcUtils.convertToORCObject(OrcUtils.getOrcField(fieldSchema), o);
        } catch (ArrayIndexOutOfBoundsException aioobe) {
          throw new IOException(aioobe);
        }
      }
      writer.addRow(OrcUtils.createOrcStruct(orcSchema, row));
    }
  }

  @Override
  public void close() throws IOException {
    if (null != this.writer) {
      this.writer.close();
    }
  }

}

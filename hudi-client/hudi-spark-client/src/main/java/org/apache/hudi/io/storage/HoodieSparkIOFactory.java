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

package org.apache.hudi.io.storage;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.io.hadoop.HoodieHadoopIOFactory;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.avro.HoodieAvroDeserializer;
import org.apache.spark.sql.avro.HoodieAvroSerializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;
import java.util.function.Function;

import static org.apache.hudi.avro.AvroSchemaUtils.isNullable;

/**
 * Creates readers and writers for SPARK and AVRO record payloads
 */
public class HoodieSparkIOFactory extends HoodieHadoopIOFactory {

  public HoodieSparkIOFactory(HoodieStorage storage) {
    super(storage);
  }

  public static HoodieSparkIOFactory getHoodieSparkIOFactory(HoodieStorage storage) {
    return new HoodieSparkIOFactory(storage);
  }

  @Override
  public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.SPARK) {
      return new HoodieSparkFileReaderFactory(storage);
    }
    return super.getReaderFactory(recordType);
  }

  @Override
  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.SPARK) {
      return new HoodieSparkFileWriterFactory(storage);
    }
    return super.getWriterFactory(recordType);
  }

  @Override
  public Function<HoodieRecord<?>, IndexedRecord> toIndexedRecord(Schema recordSchema, Properties properties, HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.SPARK) {
      HoodieAvroSerializer serializer = SparkAdapterSupport$.MODULE$.sparkAdapter()
          .createAvroSerializer(HoodieInternalRowUtils.getCachedSchema(recordSchema), recordSchema, isNullable(recordSchema));
      return record -> (IndexedRecord) serializer.serialize(record.getData());
    }
    return super.toIndexedRecord(recordSchema, properties, recordType);
  }

  @Override
  public Function<IndexedRecord, HoodieRecord<?>> fromIndexedRecord(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.SPARK) {
      final Object[] deserializerAndSchema = {null, null};
      return record -> {
        if (deserializerAndSchema[0] == null) {
          deserializerAndSchema[0] = HoodieInternalRowUtils.getCachedSchema(record.getSchema());
          deserializerAndSchema[1] = SparkAdapterSupport$.MODULE$.sparkAdapter()
              .createAvroDeserializer(record.getSchema(), (StructType) deserializerAndSchema[0]);
        }
        return new HoodieSparkRecord(((InternalRow) ((HoodieAvroDeserializer) deserializerAndSchema[1]).deserialize(record).get()), (StructType) deserializerAndSchema[0]);
      };
    }
    return super.fromIndexedRecord(recordType);
  }
}

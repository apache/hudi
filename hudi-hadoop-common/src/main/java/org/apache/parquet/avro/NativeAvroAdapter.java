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

package org.apache.parquet.avro;

import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;

/**
 * uses the native avro schema converter from parquet java
 */
public class NativeAvroAdapter implements AvroAdapter {
  @Override
  public org.apache.hudi.AvroSchemaConverter getAvroSchemaConverter(StorageConfiguration<?> storageConfiguration) {
    org.apache.parquet.avro.AvroSchemaConverter avroSchemaConverter = new org.apache.parquet.avro.AvroSchemaConverter(storageConfiguration.unwrapAs(Configuration.class));
    return new org.apache.hudi.AvroSchemaConverter() {
      @Override
      public MessageType convert(Schema schema) {
        return avroSchemaConverter.convert(schema);
      }

      @Override
      public Schema convert(MessageType schema) {
        return avroSchemaConverter.convert(schema);
      }
    };
  }
}

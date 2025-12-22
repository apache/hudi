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

import org.apache.hudi.common.util.ReflectionUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parquet-Java AvroSchemaConverter doesn't support local timestamp types until version 1.14
 * for this reason we use a modified version of the AvroSchemaConverter that adds support for local timestamp types
 * Parquet-Java still supports local timestamp types from version 1.11.0, just that the AvroSchemaConverter
 * doesn't work.
 * <p>
 * However, for versions < 1.11.0, local timestamp is not supported at all. Therefore, we just use the
 * library AvroSchemaConverter in this case.
 *
 */
public abstract class HoodieAvroParquetSchemaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieAvroParquetSchemaConverter.class);
  public static HoodieAvroParquetSchemaConverter getAvroSchemaConverter(Configuration configuration) {
    try {
      return (HoodieAvroParquetSchemaConverter) ReflectionUtils.loadClass(AvroSchemaConverterWithTimestampNTZ.class.getName(),
          new Class<?>[] {Configuration.class}, configuration);
    } catch (Throwable t) {
      LOG.debug("Failed to load AvroSchemaConverterWithTimestampNTZ, using NativeAvroSchemaConverter instead", t);
      return (HoodieAvroParquetSchemaConverter) ReflectionUtils.loadClass(NativeAvroSchemaConverter.class.getName(),
          new Class<?>[] {Configuration.class}, configuration);
    }
  }

  public abstract MessageType convert(Schema schema);

  public abstract Schema convert(MessageType schema);
}

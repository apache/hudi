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

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.row.HoodieRowParquetConfig;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;

public class HoodieSparkFileWriterFactory {

  public static <T, I, K, O> HoodieFileWriter getFileWriter(
      String instantTime, Path path, HoodieTable<T, I, K, O> hoodieTable, HoodieWriteConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetFileWriter(instantTime, path, config, schema, hoodieTable.getHadoopConf(), taskContextSupplier, config.populateMetaFields());
    }
    if (HFILE.getFileExtension().equals(extension)) {
      // todo: support hfile
      return null;
    }
    if (ORC.getFileExtension().equals(extension)) {
      // todo: support orc
      return null;
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  private static HoodieSparkFileWriter newParquetFileWriter(
      String instantTime, Path path, HoodieWriteConfig config, Schema schema, Configuration conf,
      TaskContextSupplier taskContextSupplier, boolean populateMetaFields) throws IOException {
    HoodieRowParquetWriteSupport writeSupport = new HoodieRowParquetWriteSupport(conf,
        AvroConversionUtils.convertAvroSchemaToStructType(schema),
        HoodieFileWriterFactory.createBloomFilter(config), config);

    HoodieRowParquetConfig parquetConfig = new HoodieRowParquetConfig(writeSupport, config.getParquetCompressionCodec(),
        config.getParquetBlockSize(), config.getParquetPageSize(), config.getParquetMaxFileSize(),
        conf, config.getParquetCompressionRatio());

    return new HoodieSparkParquetWriter(path, parquetConfig);
  }

}

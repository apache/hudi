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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.internal.SQLConf;

import java.io.IOException;

public class HoodieSparkFileReaderFactory extends HoodieFileReaderFactory {

  @Override
  public HoodieFileReader newParquetFileReader(Configuration conf, Path path) {
    Configuration configClone = new Configuration(conf);
    configClone.setIfUnset(SQLConf.PARQUET_BINARY_AS_STRING().key(), SQLConf.PARQUET_BINARY_AS_STRING().defaultValueString());
    configClone.setIfUnset(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), SQLConf.PARQUET_INT96_AS_TIMESTAMP().defaultValueString());
    configClone.setIfUnset(SQLConf.CASE_SENSITIVE().key(), SQLConf.CASE_SENSITIVE().defaultValueString());
    // Using string value of this conf to preserve compatibility across spark versions.
    configClone.setIfUnset("spark.sql.legacy.parquet.nanosAsLong", "false");
    // This is a required config since Spark 3.4.0: SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED
    // Using string value of this conf to preserve compatibility across spark versions.
    configClone.setIfUnset("spark.sql.parquet.inferTimestampNTZ.enabled", "true");
    return new HoodieSparkParquetReader(configClone, path);
  }

  @Override
  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig,
                                                Configuration conf,
                                                Path path,
                                                Option<Schema> schemaOption) throws IOException {
    throw new HoodieIOException("Not support read HFile");
  }

  @Override
  protected HoodieFileReader newOrcFileReader(Configuration conf, Path path) {
    throw new HoodieIOException("Not support read orc file");
  }

  @Override
  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader, HoodieFileReader dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    return new HoodieSparkBootstrapFileReader(skeletonFileReader, dataFileReader, partitionFields, partitionValues);
  }
}

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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.internal.SQLConf;

import java.io.IOException;

public class HoodieSparkFileReaderFactory extends HoodieFileReaderFactory {

  protected HoodieFileReader newParquetFileReader(Configuration conf, Path path) {
    conf.setIfUnset(SQLConf.PARQUET_BINARY_AS_STRING().key(), SQLConf.PARQUET_BINARY_AS_STRING().defaultValueString());
    conf.setIfUnset(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), SQLConf.PARQUET_INT96_AS_TIMESTAMP().defaultValueString());
    conf.setIfUnset(SQLConf.CASE_SENSITIVE().key(), SQLConf.CASE_SENSITIVE().defaultValueString());
    // Using string value of this conf to preserve compatibility across spark versions.
    conf.setIfUnset("spark.sql.legacy.parquet.nanosAsLong", "false");
    return new HoodieSparkParquetReader(conf, path);
  }

  protected HoodieFileReader newHFileFileReader(Configuration conf, Path path) throws IOException {
    throw new HoodieIOException("Not support read HFile");
  }

  protected HoodieFileReader newOrcFileReader(Configuration conf, Path path) {
    throw new HoodieIOException("Not support read orc file");
  }

  @Override
  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader, HoodieFileReader dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    return new HoodieSparkBootstrapFileReader(skeletonFileReader, dataFileReader, partitionFields, partitionValues);
  }
}

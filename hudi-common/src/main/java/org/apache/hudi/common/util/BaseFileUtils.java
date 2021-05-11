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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.HoodieTableMetaClient;

public abstract class BaseFileUtils {

  public static BaseFileUtils getInstance(String path) {
    if (path.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
      return new ParquetUtils();
    }
    throw new UnsupportedOperationException("The format for file " + path + " is not supported yet.");
  }

  public static BaseFileUtils getInstance(HoodieFileFormat fileFormat) {
    if (HoodieFileFormat.PARQUET.equals(fileFormat)) {
      return new ParquetUtils();
    }
    throw new UnsupportedOperationException(fileFormat.name() + " format not supported yet.");
  }

  public static BaseFileUtils getInstance(HoodieTableMetaClient metaClient) {
    return getInstance(metaClient.getTableConfig().getBaseFileFormat());
  }

  public abstract Set<String> readRowKeys(Configuration configuration, Path filePath);

  public abstract Set<String> filterRowKeys(Configuration configuration, Path filePath, Set<String> filter);

  public abstract List<HoodieKey> fetchRecordKeyPartitionPath(Configuration configuration, Path filePath);

  public abstract Schema readAvroSchema(Configuration configuration, Path filePath);

  public abstract BloomFilter readBloomFilterFromMetadata(Configuration configuration, Path filePath);

  public abstract String[] readMinMaxRecordKeys(Configuration configuration, Path filePath);

  public abstract List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath);

  public abstract List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath, Schema schema);

  public abstract Map<String, String> readFooter(Configuration conf, boolean required, Path orcFilePath,
      String... footerNames);

  public abstract long getRowCount(Configuration conf, Path filePath);
}
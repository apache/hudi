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

package org.apache.hudi;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import java.io.IOException;

/**
 * test docs.
 */
public class ParquetWriteHelper {

  static Dataset<Boolean> writeToParquet(Dataset<Row> rows, String basePath, ExpressionEncoder<Row> encoder, SerializableConfiguration serConfig, int parallelism,
      String compressionCodec, String partitionPathProp, String recordKeyProp) throws IOException {

    try {
      Path basePathDir = new Path(basePath);
      final FileSystem fs = FSUtils.getFs(basePath, serConfig.get());
      if (!fs.exists(basePathDir)) {
        fs.mkdirs(basePathDir);
      }
      return rows.sort(partitionPathProp, recordKeyProp).coalesce(parallelism)
          .mapPartitions(new HudiRowParquetMapPartitionFunc(basePath, encoder, serConfig, compressionCodec), Encoders.BOOLEAN());
    } catch (Exception e) {
      System.err.println("Exception thrown in WriteHelper " + e.getCause() + " ... " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }
}

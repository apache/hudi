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

package org.apache.hudi.parquet.io;

import org.apache.hudi.io.storage.rewrite.HoodieFileMetadataMerger;
import org.apache.hudi.io.storage.rewrite.HoodieFileRewriter;
import org.apache.hudi.io.storage.rewrite.HoodieFileRewriterFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class HoodieParquetRewriterFactory extends HoodieFileRewriterFactory {

  @Override
  protected <T> HoodieFileRewriter newFileRewriter(
      Configuration conf,
      String compressionCodecName,
      HoodieFileMetadataMerger metadataMerger) {

    return new HoodieParquetFileRewriter(
        conf,
        CompressionCodecName.fromConf(compressionCodecName.isEmpty() ? null : compressionCodecName),
        metadataMerger);
  }
}

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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.avro.HoodieMinMaxWriteSupport;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;
import java.util.Map;

/**
 * Hoodie Write Support for directly writing Row to Parquet.
 */
public class HoodieLSMRowParquetWriteSupport extends HoodieRowParquetWriteSupport {
  private final Option<HoodieMinMaxRowWriteSupport> minMaxWriteSupportOpt;

  public HoodieLSMRowParquetWriteSupport(Configuration conf, StructType structType, HoodieStorageConfig config) {
    super(conf, structType, Option.empty(), config, false);
    this.minMaxWriteSupportOpt = Option.of(new HoodieMinMaxRowWriteSupport());
  }

  @Override
  public FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        minMaxWriteSupportOpt.map(HoodieMinMaxWriteSupport::finalizeMetadata)
            .orElse(Collections.emptyMap());

    return new FinalizedWriteContext(extraMetadata);
  }

  @Override
  public void add(UTF8String recordKey) {
    this.minMaxWriteSupportOpt.ifPresent(writeSupport ->
        writeSupport.addKey(recordKey));
  }

  private static class HoodieMinMaxRowWriteSupport extends HoodieMinMaxWriteSupport<UTF8String> {
    public HoodieMinMaxRowWriteSupport() {
      super();
    }

    @Override
    protected UTF8String dereference(UTF8String key) {
      // NOTE: [[clone]] is performed here (rather than [[copy]]) to only copy underlying buffer in
      //       cases when [[UTF8String]] is pointing into a buffer storing the whole containing record,
      //       and simply do a pass over when it holds a (immutable) buffer holding just the string
      return key.clone();
    }
  }

}

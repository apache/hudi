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
import org.apache.hudi.common.util.Option;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.Map;

/**
 * Hoodie Write Support for directly writing {@link RowData} to Parquet.
 */
public class HoodieLSMRowDataParquetWriteSupport extends HoodieRowDataParquetWriteSupport {
  private final Option<HoodieMinMaxWriteSupport<String>> minMaxWriteSupportOpt;

  public HoodieLSMRowDataParquetWriteSupport(Configuration conf, RowType rowType) {
    super(conf, rowType, null, false);
    this.minMaxWriteSupportOpt = Option.of(new HoodieMinMaxWriteSupport<>());
  }

  @Override
  public FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        minMaxWriteSupportOpt.map(HoodieMinMaxWriteSupport::finalizeMetadata)
            .orElse(Collections.emptyMap());

    return new FinalizedWriteContext(extraMetadata);
  }

  @Override
  public void add(String recordKey) {
    this.minMaxWriteSupportOpt.ifPresent(writeSupport ->
        writeSupport.addKey(recordKey));
  }
}

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

package org.apache.hudi.common.table.log.block;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hudi.common.util.Option;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HoodieCDCDataBlock extends HoodieAvroDataBlock {

  public HoodieCDCDataBlock(
      FSDataInputStream inputStream,
      Option<byte[]> content,
      boolean readBlockLazily,
      HoodieLogBlockContentLocation logBlockContentLocation,
      Schema readerSchema,
      Map<HeaderMetadataType, String> header,
      String keyField) {
    super(inputStream, content, readBlockLazily, logBlockContentLocation,
        Option.of(readerSchema), header, new HashMap<>(), keyField, null);
  }

  public HoodieCDCDataBlock(@Nonnull List<IndexedRecord> records,
                            @Nonnull Map<HeaderMetadataType, String> header,
                            @Nonnull String keyField) {
    super(records, header, keyField);
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.CDC_DATA_BLOCK;
  }

}

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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Corrupt block is emitted whenever the scanner finds the length of the block written at the beginning does not match
 * (did not find a EOF or a sync marker after the length).
 */
public class HoodieCorruptBlock extends HoodieLogBlock {

  public HoodieCorruptBlock(Option<byte[]> corruptedBytes, Supplier<SeekableDataInputStream> inputStreamSupplier, boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation, Map<HeaderMetadataType, String> header,
                            Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockContentLocation, corruptedBytes, inputStreamSupplier, readBlockLazily);
  }

  @Override
  public byte[] getContentBytes(HoodieStorage storage) throws IOException {
    if (!getContent().isPresent() && readBlockLazily) {
      // read content from disk
      inflate();
    }
    return getContent().get();
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.CORRUPT_BLOCK;
  }

}

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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Command block issues a specific command to the scanner.
 */
public class HoodieCommandBlock extends HoodieLogBlock {

  private final HoodieCommandBlockTypeEnum type;

  /**
   * Hoodie command block type enum.
   */
  public enum HoodieCommandBlockTypeEnum {
    ROLLBACK_BLOCK
  }

  public HoodieCommandBlock(Map<HeaderMetadataType, String> header) {
    this(Option.empty(), null, false, Option.empty(), header, new HashMap<>());
  }

  public HoodieCommandBlock(Option<byte[]> content, Supplier<SeekableDataInputStream> inputStreamSupplier, boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation, Map<HeaderMetadataType, String> header,
                            Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockContentLocation, content, inputStreamSupplier, readBlockLazily);
    this.type =
        HoodieCommandBlockTypeEnum.values()[Integer.parseInt(header.get(HeaderMetadataType.COMMAND_BLOCK_TYPE))];
  }

  public HoodieCommandBlockTypeEnum getType() {
    return type;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.COMMAND_BLOCK;
  }

  @Override
  public byte[] getContentBytes(HoodieStorage storage) {
    return new byte[0];
  }
}

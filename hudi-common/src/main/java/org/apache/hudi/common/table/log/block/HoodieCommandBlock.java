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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FSDataInputStream;

import java.util.HashMap;
import java.util.Map;

/**
 * Command block issues a specific command to the scanner.
 */
public class HoodieCommandBlock extends HoodieLogBlock {

  private final HoodieCommandBlockTypeEnum type;

  /**
   * Hoodie command block type enum.
   */
  public enum HoodieCommandBlockTypeEnum {
    ROLLBACK_PREVIOUS_BLOCK
  }

  public HoodieCommandBlock(Map<HeaderMetadataType, String> header) {
    this(Option.empty(), null, false, Option.empty(), header, new HashMap<>());
  }

  private HoodieCommandBlock(Option<byte[]> content, FSDataInputStream inputStream, boolean readBlockLazily,
      Option<HoodieLogBlockContentLocation> blockContentLocation, Map<HeaderMetadataType, String> header,
      Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockContentLocation, content, inputStream, readBlockLazily);
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
  public byte[] getContentBytes() {
    return new byte[0];
  }

  public static HoodieLogBlock getBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
      boolean readBlockLazily, long position, long blockSize, long blockEndpos, Map<HeaderMetadataType, String> header,
      Map<HeaderMetadataType, String> footer) {

    return new HoodieCommandBlock(content, inputStream, readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)), header, footer);
  }
}

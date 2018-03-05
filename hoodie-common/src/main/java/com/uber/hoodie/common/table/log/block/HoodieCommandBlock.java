/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log.block;

import com.uber.hoodie.common.model.HoodieLogFile;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Command block issues a specific command to the scanner
 */
public class HoodieCommandBlock extends HoodieLogBlock {

  private final HoodieCommandBlockTypeEnum type;

  private byte [] content;

  public enum HoodieCommandBlockTypeEnum {ROLLBACK_PREVIOUS_BLOCK};

  public HoodieCommandBlock(Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    this(Optional.ofNullable(null), header, footer);
  }

  public HoodieCommandBlock(Map<HeaderMetadataType, String> header) {
    this(Optional.ofNullable(null), header, new HashMap<>());
  }

  private HoodieCommandBlock(byte [] content,
                             Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(header, footer, Optional.ofNullable(null));
    this.content = content;
    this.type = HoodieCommandBlockTypeEnum.values()[Integer.parseInt(header.get(HeaderMetadataType.COMMAND_BLOCK_TYPE))];
  }

  private HoodieCommandBlock(Optional<HoodieLogBlockContentLocation> blockLocation,
                             Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockLocation);
    this.type = HoodieCommandBlockTypeEnum.values()[Integer.parseInt(header.get(HeaderMetadataType.COMMAND_BLOCK_TYPE))];
  }

  public HoodieCommandBlockTypeEnum getType() {
    return type;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.COMMAND_BLOCK;
  }

  @Override
  public byte [] getContentBytes() {
    return new byte[0];
  }

  public static HoodieLogBlock getBlock(byte [] content,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) throws IOException {

    return new HoodieCommandBlock(content, header, footer);
  }

  public static HoodieLogBlock getBlock(HoodieLogFile logFile,
                                        long position,
                                        long blockSize,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) throws IOException {

    return new HoodieCommandBlock(Optional.of(new HoodieLogBlockContentLocation(logFile, position, blockSize)), header, footer);
  }
}

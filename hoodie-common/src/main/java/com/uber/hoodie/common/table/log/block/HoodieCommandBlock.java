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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Command block issues a specific command to the scanner
 */
public class HoodieCommandBlock extends HoodieLogBlock {

  private final HoodieCommandBlockTypeEnum type;

  public enum HoodieCommandBlockTypeEnum {ROLLBACK_PREVIOUS_BLOCK}

  public HoodieCommandBlock(HoodieCommandBlockTypeEnum type, Map<LogMetadataType, String> metadata) {
    super(metadata);
    this.type = type;
  }

  public HoodieCommandBlock(HoodieCommandBlockTypeEnum type) {
    this(type, null);
  }

  @Override
  public byte[] getBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    if(super.getLogMetadata() != null) {
      output.write(HoodieLogBlock.getLogMetadataBytes(super.getLogMetadata()));
    }
    output.writeInt(type.ordinal());
    output.close();
    return baos.toByteArray();
  }

  public HoodieCommandBlockTypeEnum getType() {
    return type;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.COMMAND_BLOCK;
  }

  public static HoodieLogBlock fromBytes(byte[] content, boolean readMetadata) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(content));
    Map<LogMetadataType, String> metadata = null;
    if(readMetadata) {
      metadata = HoodieLogBlock.getLogMetadata(dis);
    }
    int ordinal = dis.readInt();
    return new HoodieCommandBlock(HoodieCommandBlockTypeEnum.values()[ordinal], metadata);
  }
}

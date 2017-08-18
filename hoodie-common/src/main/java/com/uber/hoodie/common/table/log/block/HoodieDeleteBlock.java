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

import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Delete block contains a list of keys to be deleted from scanning the blocks so far
 */
public class HoodieDeleteBlock extends HoodieLogBlock {

  private final String[] keysToDelete;

  public HoodieDeleteBlock(String[] keysToDelete, Map<LogMetadataType, String> metadata) {
    super(metadata);
    this.keysToDelete = keysToDelete;
  }

  public HoodieDeleteBlock(String[] keysToDelete) {
    this(keysToDelete, null);
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    if(super.getLogMetadata() != null) {
      output.write(HoodieLogBlock.getLogMetadataBytes(super.getLogMetadata()));
    }
    byte [] bytesToWrite = StringUtils.join(keysToDelete, ',').getBytes(Charset.forName("utf-8"));
    output.writeInt(bytesToWrite.length);
    output.write(bytesToWrite);
    return baos.toByteArray();
  }

  public String[] getKeysToDelete() {
    return keysToDelete;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.DELETE_BLOCK;
  }

  public static HoodieLogBlock fromBytes(byte[] content, boolean readMetadata) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(content));
    Map<LogMetadataType, String> metadata = null;
    if(readMetadata) {
      metadata = HoodieLogBlock.getLogMetadata(dis);
    }
    int dataLength = dis.readInt();
    byte [] data = new byte[dataLength];
    dis.readFully(data);
    return new HoodieDeleteBlock(new String(data).split(","), metadata);
  }
}

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
import com.uber.hoodie.common.storage.SizeAwareDataInputStream;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Delete block contains a list of keys to be deleted from scanning the blocks so far
 */
public class HoodieDeleteBlock extends HoodieLogBlock {

  private String[] keysToDelete;

  private byte[] content;

  public HoodieDeleteBlock(String[] keysToDelete,
                           Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    this(keysToDelete, null, header, footer);
  }

  public HoodieDeleteBlock(String[] keysToDelete,
                           Map<HeaderMetadataType, String> header) {
    this(keysToDelete, null, header, new HashMap<>());
  }


  private HoodieDeleteBlock(String[] keysToDelete, Optional<HoodieLogBlockContentLocation> blockLocation,
                            Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockLocation);
    this.keysToDelete = keysToDelete;
    this.content = null;
  }

  private HoodieDeleteBlock(Optional<HoodieLogBlockContentLocation> blockLocation,
                            Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    this(null, blockLocation, header, footer);
  }

  private HoodieDeleteBlock(byte [] content,
                            Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    this(null, Optional.empty(), header, footer);
    this.content = content;
  }

  @Override
  public byte[] getContentBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    byte[] bytesToWrite = StringUtils.join(getKeysToDelete(), ',').getBytes(Charset.forName("utf-8"));
    output.writeInt(bytesToWrite.length);
    output.write(bytesToWrite);
    return baos.toByteArray();
  }

  public String[] getKeysToDelete() {
    try {
      if (keysToDelete == null) {
        if(content == null) {
          throw new HoodieException("Content is empty, use HoodieLazyBlockReader to read contents lazily");
        }
        SizeAwareDataInputStream dis =
            new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(content)));
        int dataLength = dis.readInt();
        byte[] data = new byte[dataLength];
        dis.readFully(data);
        this.keysToDelete = new String(data).split(",");
      }
      return keysToDelete;
    } catch(IOException io) {
      throw new HoodieIOException("Unable to generate keys to delete from block content", io);
    }
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.DELETE_BLOCK;
  }

  public static HoodieLogBlock getBlock(byte [] content,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) throws IOException {

    return new HoodieDeleteBlock(content, header, footer);
  }

  public static HoodieLogBlock getBlock(HoodieLogFile logFile,
                                        long position,
                                        long blockSize,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) throws IOException {

    return new HoodieDeleteBlock(Optional.of(new HoodieLogBlockContentLocation(logFile, position, blockSize)),
        header, footer);
  }
}

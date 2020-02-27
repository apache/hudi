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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.storage.SizeAwareDataInputStream;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Delete block contains a list of keys to be deleted from scanning the blocks so far.
 */
public class HoodieDeleteBlock extends HoodieLogBlock {

  private HoodieKey[] keysToDelete;

  public HoodieDeleteBlock(HoodieKey[] keysToDelete, Map<HeaderMetadataType, String> header) {
    this(Option.empty(), null, false, Option.empty(), header, new HashMap<>());
    this.keysToDelete = keysToDelete;
  }

  private HoodieDeleteBlock(Option<byte[]> content, FSDataInputStream inputStream, boolean readBlockLazily,
      Option<HoodieLogBlockContentLocation> blockContentLocation, Map<HeaderMetadataType, String> header,
      Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockContentLocation, content, inputStream, readBlockLazily);
  }

  @Override
  public byte[] getContentBytes() throws IOException {

    // In case this method is called before realizing keys from content
    if (getContent().isPresent()) {
      return getContent().get();
    } else if (readBlockLazily && !getContent().isPresent() && keysToDelete == null) {
      // read block lazily
      getKeysToDelete();
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    byte[] bytesToWrite = SerializationUtils.serialize(getKeysToDelete());
    output.writeInt(version);
    output.writeInt(bytesToWrite.length);
    output.write(bytesToWrite);
    return baos.toByteArray();
  }

  public HoodieKey[] getKeysToDelete() {
    try {
      if (keysToDelete == null) {
        if (!getContent().isPresent() && readBlockLazily) {
          // read content from disk
          inflate();
        }
        SizeAwareDataInputStream dis =
            new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(getContent().get())));
        int version = dis.readInt();
        int dataLength = dis.readInt();
        byte[] data = new byte[dataLength];
        dis.readFully(data);
        this.keysToDelete = SerializationUtils.<HoodieKey[]>deserialize(data);
        deflate();
      }
      return keysToDelete;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to generate keys to delete from block content", io);
    }
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.DELETE_BLOCK;
  }

  public static HoodieLogBlock getBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
      boolean readBlockLazily, long position, long blockSize, long blockEndPos, Map<HeaderMetadataType, String> header,
      Map<HeaderMetadataType, String> footer) throws IOException {

    return new HoodieDeleteBlock(content, inputStream, readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndPos)), header, footer);
  }
}

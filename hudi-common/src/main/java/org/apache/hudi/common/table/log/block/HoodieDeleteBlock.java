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

import org.apache.hudi.common.fs.SizeAwareDataInputStream;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Delete block contains a list of keys to be deleted from scanning the blocks so far.
 */
public class HoodieDeleteBlock extends HoodieLogBlock {

  private DeleteRecord[] recordsToDelete;

  public HoodieDeleteBlock(DeleteRecord[] recordsToDelete, Map<HeaderMetadataType, String> header) {
    this(Option.empty(), null, false, Option.empty(), header, new HashMap<>());
    this.recordsToDelete = recordsToDelete;
  }

  public HoodieDeleteBlock(Option<byte[]> content, FSDataInputStream inputStream, boolean readBlockLazily,
      Option<HoodieLogBlockContentLocation> blockContentLocation, Map<HeaderMetadataType, String> header,
      Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockContentLocation, content, inputStream, readBlockLazily);
  }

  @Override
  public byte[] getContentBytes() throws IOException {
    Option<byte[]> content = getContent();

    // In case this method is called before realizing keys from content
    if (content.isPresent()) {
      return content.get();
    } else if (readBlockLazily && recordsToDelete == null) {
      // read block lazily
      getRecordsToDelete();
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    byte[] bytesToWrite = SerializationUtils.serialize(getRecordsToDelete());
    output.writeInt(version);
    output.writeInt(bytesToWrite.length);
    output.write(bytesToWrite);
    return baos.toByteArray();
  }

  public DeleteRecord[] getRecordsToDelete() {
    try {
      if (recordsToDelete == null) {
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
        this.recordsToDelete = deserialize(version, data);
        deflate();
      }
      return recordsToDelete;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to generate keys to delete from block content", io);
    }
  }

  private static DeleteRecord[] deserialize(int version, byte[] data) {
    if (version == 1) {
      // legacy version
      HoodieKey[] keys = SerializationUtils.<HoodieKey[]>deserialize(data);
      return Arrays.stream(keys).map(DeleteRecord::create).toArray(DeleteRecord[]::new);
    } else {
      return SerializationUtils.<DeleteRecord[]>deserialize(data);
    }
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.DELETE_BLOCK;
  }

}

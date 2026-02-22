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

package org.apache.hudi.source.split;

import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for Hoodie source split.
 */
@Internal
public class HoodieSourceSplitSerializer implements SimpleVersionedSerializer<HoodieSourceSplit> {
  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(HoodieSourceSplit obj) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream out = new DataOutputStream(baos)) {

      // Serialize splitNum
      out.writeInt(obj.getSplitNum());

      // Serialize basePath (Option<String>)
      out.writeBoolean(obj.getBasePath().isPresent());
      if (obj.getBasePath().isPresent()) {
        out.writeUTF(obj.getBasePath().get());
      }

      // Serialize logPaths (Option<List<String>>)
      out.writeBoolean(obj.getLogPaths().isPresent());
      if (obj.getLogPaths().isPresent()) {
        List<String> logPaths = obj.getLogPaths().get();
        out.writeInt(logPaths.size());
        for (String logPath : logPaths) {
          out.writeUTF(logPath);
        }
      }

      // Serialize tablePath
      out.writeUTF(obj.getTablePath());
      // Serialize partitionPath
      out.writeUTF(obj.getPartitionPath());
      // Serialize mergeType
      out.writeUTF(obj.getMergeType());
      // Serialize latest commit
      out.writeUTF(obj.getLatestCommit());
      // Serialize fileId
      out.writeUTF(obj.getFileId());
      // Serialize consumed
      out.writeLong(obj.getConsumed());
      // Serialize fileOffset
      out.writeInt(obj.getFileOffset());

      // Serialize instant range (Option<InstantRange>)
      out.writeBoolean(obj.getInstantRange().isPresent());
      if (obj.getInstantRange().isPresent()) {
        InstantRange instantRange = obj.getInstantRange().get();
        out.writeBoolean(instantRange.getStartInstant().isPresent());
        if (instantRange.getStartInstant().isPresent()) {
          out.writeUTF(instantRange.getStartInstant().get());
        }
        out.writeBoolean(instantRange.getEndInstant().isPresent());
        if (instantRange.getEndInstant().isPresent()) {
          out.writeUTF(instantRange.getEndInstant().get());
        }
        out.writeUTF(instantRange.getRangeType().name());
      }

      out.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public HoodieSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
         DataInputStream in = new DataInputStream(bais)) {

      // Deserialize splitNum
      int splitNum = in.readInt();

      // Deserialize basePath (Option<String>)
      String basePath = null;
      if (in.readBoolean()) {
        basePath = in.readUTF();
      }

      // Deserialize logPaths (Option<List<String>>)
      Option<List<String>> logPaths;
      if (in.readBoolean()) {
        int logPathsCount = in.readInt();
        List<String> logPathsList = new ArrayList<>(logPathsCount);
        for (int i = 0; i < logPathsCount; i++) {
          logPathsList.add(in.readUTF());
        }
        logPaths = Option.of(logPathsList);
      } else {
        logPaths = Option.empty();
      }

      // Deserialize tablePath
      String tablePath = in.readUTF();
      // Deserialize partitionPath
      String partitionPath = in.readUTF();
      // Deserialize mergeType
      String mergeType = in.readUTF();
      // Deserialize latestCommit
      String latestCommit = in.readUTF();
      // Deserialize fileId
      String fileId = in.readUTF();
      // Deserialize consumed
      long consumed = in.readLong();
      // Deserialize fileOffset
      int fileOffset = in.readInt();

      // Deserialize instantRange (Option<InstantRange>)
      Option<InstantRange> instantRangeOption;
      if (in.readBoolean()) {
        InstantRange.Builder builder = InstantRange.builder();
        // Deserialize startInstant
        if (in.readBoolean()) {
          builder.startInstant(in.readUTF());
        }

        // Deserialize endInstant
        if (in.readBoolean()) {
          builder.endInstant(in.readUTF());
        }

        builder.rangeType(InstantRange.RangeType.valueOf(in.readUTF()));
        instantRangeOption = Option.of(builder.build());
      } else {
        instantRangeOption = Option.empty();
      }

      // Create HoodieSourceSplit object
      HoodieSourceSplit split = new HoodieSourceSplit(
          splitNum,
          basePath,
          logPaths,
          tablePath,
          partitionPath,
          mergeType,
          latestCommit,
          fileId,
          instantRangeOption);

      // Update position to restore consumed and fileOffset
      split.updatePosition(fileOffset, consumed);

      return split;
    }
  }
}

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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;

import java.io.IOException;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;

public class CommitMetadataSerDeV1 implements CommitMetadataSerDe {

  @Override
  public <T> T deserialize(HoodieInstant instant, byte[] bytes, Class<T> clazz) throws IOException {
    try {
      if (bytes.length == 0) {
        return clazz.newInstance();
      }
      return fromJsonString(fromUTF8Bytes(bytes), clazz);
    } catch (Exception e) {
      throw new IOException("unable to read commit metadata for instant " + instant + " bytes length: " + bytes.length, e);
    }
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }

  @Override
  public Option<byte[]> serialize(HoodieCommitMetadata commitMetadata) throws IOException {
    return Option.ofNullable(commitMetadata.toJsonString().getBytes());
  }
}

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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.avro.model.HoodieCommitMetadata;
import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.hudi.common.table.timeline.MetadataConversionUtils.convertCommitMetadataToPojo;
import static org.apache.hudi.common.table.timeline.MetadataConversionUtils.convertReplaceCommitMetadataToPojo;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeCommitMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeReplaceCommitMetadata;

public class CommitMetadataSerDeV2 implements CommitMetadataSerDe {

  @Override
  public <T> T deserialize(HoodieInstant instant, Option<InputStream> inputStream, Class<T> clazz) throws IOException {
    try {
      if (instant.isLegacy()) {
        // For legacy instant, delegate to legacy SerDe.
        try {
          return new CommitMetadataSerDeV1().deserialize(instant, inputStream, clazz);
        } catch (Exception e) {
          throw new IOException("unable to read legacy commit metadata for instant " + instant, e);
        }
      }
      // For any new commit metadata class being added, we need the corresponding logic added here
      if (org.apache.hudi.common.model.HoodieReplaceCommitMetadata.class.isAssignableFrom(clazz)) {
        return (T) convertReplaceCommitMetadataToPojo(deserializeReplaceCommitMetadata(inputStream));
      }
      return (T) convertCommitMetadataToPojo(deserializeCommitMetadata(inputStream));
    } catch (Exception e) {
      throw new IOException("unable to read commit metadata for instant " + instant, e);
    }
  }

  public static <T> T fromJsonString(InputStream inputStream, Class<T> clazz) throws Exception {
    return JsonUtils.getObjectMapper().readValue(inputStream, clazz);
  }

  @Override
  public Option<byte[]> serialize(org.apache.hudi.common.model.HoodieCommitMetadata commitMetadata) throws IOException {
    if (commitMetadata instanceof org.apache.hudi.common.model.HoodieReplaceCommitMetadata) {
      return serializeAvroMetadata(MetadataConversionUtils.convertCommitMetadata(commitMetadata), HoodieReplaceCommitMetadata.class);
    }
    return serializeAvroMetadata(MetadataConversionUtils.convertCommitMetadata(commitMetadata), HoodieCommitMetadata.class);
  }

  public static <T extends SpecificRecordBase> Option<byte[]> serializeAvroMetadata(T metadata, Class<T> clazz)
      throws IOException {
    DatumWriter<T> datumWriter = new SpecificDatumWriter<>(clazz);
    DataFileWriter<T> fileWriter = new DataFileWriter<>(datumWriter);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    fileWriter.create(metadata.getSchema(), baos);
    fileWriter.append(metadata);
    fileWriter.flush();
    return Option.of(baos.toByteArray());
  }
}

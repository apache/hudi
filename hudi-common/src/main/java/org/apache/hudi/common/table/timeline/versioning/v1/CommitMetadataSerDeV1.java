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
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieInstantWriter;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BooleanSupplier;

import static org.apache.hudi.common.table.timeline.MetadataConversionUtils.removeNullKeyFromMapMembersForCommitMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeAvroMetadata;

public class CommitMetadataSerDeV1 implements CommitMetadataSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(CommitMetadataSerDeV1.class);

  @Override
  public <T> T deserialize(HoodieInstant instant, InputStream inputStream, BooleanSupplier isEmptyInstant, Class<T> clazz) throws IOException {
    try {
      // For commit metadata we need special case handling as they are using serialized to JSON in V1
      if (HoodieCommitMetadata.class.isAssignableFrom(clazz)) {
        return JsonUtils.getObjectMapper().readValue(inputStream, clazz);
      } else {
        if (!SpecificRecordBase.class.isAssignableFrom(clazz)) {
          throw new IllegalArgumentException("Class must extend SpecificRecordBase: " + clazz.getName());
        }
        @SuppressWarnings("unchecked")
        Class<? extends SpecificRecordBase> avroClass = (Class<? extends SpecificRecordBase>) clazz;
        return (T) deserializeAvroMetadata(inputStream, avroClass);
      }
    } catch (Exception e) {
      // Empty file does not conform to avro format, in that case we return newInstance.
      if (isEmptyInstant.getAsBoolean()) {
        try {
          return clazz.newInstance();
        } catch (Exception ex) {
          throw new IOException("unable to read commit metadata for instant " + instant, ex);
        }
      }
      throw new IOException("Unable to read commit metadata for instant " + instant, e);
    }
  }

  @Override
  public <T> Option<HoodieInstantWriter> getInstantWriter(T metadata) {
    // If it is hoodie commit metadata, write json string.
    removeNullKeyFromMapMembersForCommitMetadata(metadata);

    if (metadata instanceof HoodieReplaceCommitMetadata) {
      return Option.of(outputStream -> JsonUtils.getObjectMapper().writerFor(HoodieReplaceCommitMetadata.class)
          .withDefaultPrettyPrinter().writeValue(outputStream, metadata));
    }
    if (metadata instanceof HoodieCommitMetadata) {
      return Option.of(outputStream -> JsonUtils.getObjectMapper().writerFor(HoodieCommitMetadata.class)
          .withDefaultPrettyPrinter().writeValue(outputStream, metadata));
    }

    // For others, write avro format.
    @SuppressWarnings("unchecked")
    SpecificRecordBase avroMetadata = (SpecificRecordBase) metadata;
    return CommitMetadataSerDe.getInstantWriter(Option.of(avroMetadata));
  }
}

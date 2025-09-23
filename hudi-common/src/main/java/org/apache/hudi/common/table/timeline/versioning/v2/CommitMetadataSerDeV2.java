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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieInstantWriter;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BooleanSupplier;

import static org.apache.hudi.common.table.timeline.MetadataConversionUtils.convertReplaceCommitMetadataToPojo;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeAvroMetadata;
import static org.apache.hudi.common.util.DeserializationUtils.deserializeHoodieCommitMetadata;
import static org.apache.hudi.common.util.DeserializationUtils.deserializeHoodieReplaceCommitMetadata;

public class CommitMetadataSerDeV2 implements CommitMetadataSerDe {

  /**
   * Convert commit metadata from avro to pojo.
   */
  private HoodieCommitMetadata convertCommitMetadataToPojo(org.apache.hudi.avro.model.HoodieCommitMetadata hoodieCommitMetadata) {
    // While it is valid to have a null key in the hash map in java, avro map could not accommodate this, so we need to remove null key explicitly before the conversion.
    hoodieCommitMetadata.getPartitionToWriteStats().remove(null);
    return JsonUtils.getObjectMapper().convertValue(hoodieCommitMetadata, HoodieCommitMetadata.class);
  }

  @Override
  public <T> T deserialize(HoodieInstant instant, InputStream inputStream, BooleanSupplier isEmptyInstant, Class<T> clazz) throws IOException {
    try {
      if (instant.isLegacy()) {
        // For legacy instant, delegate to legacy SerDe.
        try {
          return new CommitMetadataSerDeV1().deserialize(instant, inputStream, isEmptyInstant, clazz);
        } catch (Exception e) {
          throw new IOException("unable to read legacy commit metadata for instant " + instant, e);
        }
      }
      // For commit metadata and replace commit metadata need special case handling since it requires in memory object in POJO form.
      if (org.apache.hudi.common.model.HoodieReplaceCommitMetadata.class.isAssignableFrom(clazz)) {
        return (T) convertReplaceCommitMetadataToPojo(deserializeHoodieReplaceCommitMetadata(inputStream));
      }
      // For any new commit metadata class being added, we need the corresponding logic added here
      if (org.apache.hudi.common.model.HoodieCommitMetadata.class.isAssignableFrom(clazz)) {
        return (T) convertCommitMetadataToPojo(deserializeHoodieCommitMetadata(inputStream));
      }
      // For all the other cases they must be SpecificRecordBase
      if (!SpecificRecordBase.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("Class must extend SpecificRecordBase: " + clazz.getName());
      }
      @SuppressWarnings("unchecked")
      Class<? extends SpecificRecordBase> avroClass = (Class<? extends SpecificRecordBase>) clazz;
      return (T) deserializeAvroMetadata(inputStream, avroClass);
    } catch (Exception e) {
      // Empty file does not conform to avro format, in that case we return newInstance.
      if (isEmptyInstant.getAsBoolean()) {
        try {
          return clazz.newInstance();
        } catch (Exception ex) {
          throw new IOException("unable to read commit metadata for instant " + instant, ex);
        }
      }
      throw new IOException("unable to read commit metadata for instant " + instant, e);
    }
  }

  @Override
  public <T> Option<HoodieInstantWriter> getInstantWriter(T metadata) {
    if (metadata instanceof org.apache.hudi.common.model.HoodieCommitMetadata) {
      return CommitMetadataSerDe.getInstantWriter(Option.of(MetadataConversionUtils.convertCommitMetadataToAvro((HoodieCommitMetadata) metadata)));
    }
    return CommitMetadataSerDe.getInstantWriter(Option.of((SpecificRecordBase) metadata));
  }
}

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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.HoodieRollbackStat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A utility class for avro.
 */
public class AvroUtils {

  private static final Integer DEFAULT_VERSION = 1;

  public static HoodieRestoreMetadata convertRestoreMetadata(String startRestoreTime, Option<Long> durationInMs,
      List<String> commits, Map<String, List<HoodieRollbackStat>> commitToStats) {
    ImmutableMap.Builder<String, List<HoodieRollbackMetadata>> commitToStatBuilder = ImmutableMap.builder();
    for (Map.Entry<String, List<HoodieRollbackStat>> commitToStat : commitToStats.entrySet()) {
      commitToStatBuilder.put(commitToStat.getKey(),
          Arrays.asList(convertRollbackMetadata(startRestoreTime, durationInMs, commits, commitToStat.getValue())));
    }
    return new HoodieRestoreMetadata(startRestoreTime, durationInMs.orElseGet(() -> -1L), commits,
        commitToStatBuilder.build(), DEFAULT_VERSION);
  }

  public static HoodieRollbackMetadata convertRollbackMetadata(String startRollbackTime, Option<Long> durationInMs,
      List<String> commits, List<HoodieRollbackStat> rollbackStats) {
    ImmutableMap.Builder<String, HoodieRollbackPartitionMetadata> partitionMetadataBuilder = ImmutableMap.builder();
    int totalDeleted = 0;
    for (HoodieRollbackStat stat : rollbackStats) {
      HoodieRollbackPartitionMetadata metadata = new HoodieRollbackPartitionMetadata(stat.getPartitionPath(),
          stat.getSuccessDeleteFiles(), stat.getFailedDeleteFiles());
      partitionMetadataBuilder.put(stat.getPartitionPath(), metadata);
      totalDeleted += stat.getSuccessDeleteFiles().size();
    }

    return new HoodieRollbackMetadata(startRollbackTime, durationInMs.orElseGet(() -> -1L), totalDeleted, commits,
        partitionMetadataBuilder.build(), DEFAULT_VERSION);
  }

  public static HoodieSavepointMetadata convertSavepointMetadata(String user, String comment,
      Map<String, List<String>> latestFiles) {
    ImmutableMap.Builder<String, HoodieSavepointPartitionMetadata> partitionMetadataBuilder = ImmutableMap.builder();
    for (Map.Entry<String, List<String>> stat : latestFiles.entrySet()) {
      HoodieSavepointPartitionMetadata metadata = new HoodieSavepointPartitionMetadata(stat.getKey(), stat.getValue());
      partitionMetadataBuilder.put(stat.getKey(), metadata);
    }
    return new HoodieSavepointMetadata(user, System.currentTimeMillis(), comment, partitionMetadataBuilder.build(),
        DEFAULT_VERSION);
  }

  public static Option<byte[]> serializeCompactionPlan(HoodieCompactionPlan compactionWorkload) throws IOException {
    return serializeAvroMetadata(compactionWorkload, HoodieCompactionPlan.class);
  }

  public static Option<byte[]> serializeCleanerPlan(HoodieCleanerPlan cleanPlan) throws IOException {
    return serializeAvroMetadata(cleanPlan, HoodieCleanerPlan.class);
  }

  public static Option<byte[]> serializeCleanMetadata(HoodieCleanMetadata metadata) throws IOException {
    return serializeAvroMetadata(metadata, HoodieCleanMetadata.class);
  }

  public static Option<byte[]> serializeSavepointMetadata(HoodieSavepointMetadata metadata) throws IOException {
    return serializeAvroMetadata(metadata, HoodieSavepointMetadata.class);
  }

  public static Option<byte[]> serializeRollbackMetadata(HoodieRollbackMetadata rollbackMetadata) throws IOException {
    return serializeAvroMetadata(rollbackMetadata, HoodieRollbackMetadata.class);
  }

  public static Option<byte[]> serializeRestoreMetadata(HoodieRestoreMetadata restoreMetadata) throws IOException {
    return serializeAvroMetadata(restoreMetadata, HoodieRestoreMetadata.class);
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

  public static HoodieCleanerPlan deserializeCleanerPlan(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieCleanerPlan.class);
  }

  public static HoodieCompactionPlan deserializeCompactionPlan(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieCompactionPlan.class);
  }

  public static HoodieCleanMetadata deserializeHoodieCleanMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieCleanMetadata.class);
  }

  public static HoodieSavepointMetadata deserializeHoodieSavepointMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieSavepointMetadata.class);
  }

  public static <T extends SpecificRecordBase> T deserializeAvroMetadata(byte[] bytes, Class<T> clazz)
      throws IOException {
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
    FileReader<T> fileReader = DataFileReader.openReader(new SeekableByteArrayInput(bytes), reader);
    Preconditions.checkArgument(fileReader.hasNext(), "Could not deserialize metadata of type " + clazz);
    return fileReader.next();
  }
}

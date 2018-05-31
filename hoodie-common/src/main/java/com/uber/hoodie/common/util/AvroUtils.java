/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieCleanPartitionMetadata;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.avro.model.HoodieRollbackPartitionMetadata;
import com.uber.hoodie.avro.model.HoodieSavepointMetadata;
import com.uber.hoodie.avro.model.HoodieSavepointPartitionMetadata;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroUtils {

  public static List<HoodieRecord<HoodieAvroPayload>> loadFromFiles(FileSystem fs,
      List<String> deltaFilePaths, Schema expectedSchema) {
    List<HoodieRecord<HoodieAvroPayload>> loadedRecords = Lists.newArrayList();
    deltaFilePaths.forEach(s -> {
      List<HoodieRecord<HoodieAvroPayload>> records = loadFromFile(fs, s, expectedSchema);
      loadedRecords.addAll(records);
    });
    return loadedRecords;
  }

  public static List<HoodieRecord<HoodieAvroPayload>> loadFromFile(FileSystem fs,
      String deltaFilePath, Schema expectedSchema) {
    List<HoodieRecord<HoodieAvroPayload>> loadedRecords = Lists.newArrayList();
    Path path = new Path(deltaFilePath);
    try {
      SeekableInput input = new FsInput(path, fs.getConf());
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
      // Set the expected schema to be the current schema to account for schema evolution
      reader.setExpected(expectedSchema);

      FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
      for (GenericRecord deltaRecord : fileReader) {
        String key = deltaRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
        String partitionPath =
            deltaRecord.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
        loadedRecords.add(new HoodieRecord<>(new HoodieKey(key, partitionPath),
            new HoodieAvroPayload(Optional.of(deltaRecord))));
      }
      fileReader.close(); // also closes underlying FsInput
    } catch (IOException e) {
      throw new HoodieIOException("Could not read avro records from path " + deltaFilePath,
          e);
    }
    return loadedRecords;
  }

  public static HoodieCleanMetadata convertCleanMetadata(String startCleanTime,
      Optional<Long> durationInMs, List<HoodieCleanStat> cleanStats) {
    ImmutableMap.Builder<String, HoodieCleanPartitionMetadata> partitionMetadataBuilder =
        ImmutableMap.builder();
    int totalDeleted = 0;
    String earliestCommitToRetain = null;
    for (HoodieCleanStat stat : cleanStats) {
      HoodieCleanPartitionMetadata metadata =
          new HoodieCleanPartitionMetadata(stat.getPartitionPath(), stat.getPolicy().name(),
              stat.getDeletePathPatterns(), stat.getSuccessDeleteFiles(),
              stat.getDeletePathPatterns());
      partitionMetadataBuilder.put(stat.getPartitionPath(), metadata);
      totalDeleted += stat.getSuccessDeleteFiles().size();
      if (earliestCommitToRetain == null) {
        // This will be the same for all partitions
        earliestCommitToRetain = stat.getEarliestCommitToRetain();
      }
    }
    return new HoodieCleanMetadata(startCleanTime, durationInMs.orElseGet(() -> -1L),
        totalDeleted, earliestCommitToRetain, partitionMetadataBuilder.build());
  }

  public static HoodieRollbackMetadata convertRollbackMetadata(String startRollbackTime,
      Optional<Long> durationInMs, List<String> commits, List<HoodieRollbackStat> stats) {
    ImmutableMap.Builder<String, HoodieRollbackPartitionMetadata> partitionMetadataBuilder =
        ImmutableMap.builder();
    int totalDeleted = 0;
    for (HoodieRollbackStat stat : stats) {
      HoodieRollbackPartitionMetadata metadata =
          new HoodieRollbackPartitionMetadata(stat.getPartitionPath(),
              stat.getSuccessDeleteFiles(), stat.getFailedDeleteFiles());
      partitionMetadataBuilder.put(stat.getPartitionPath(), metadata);
      totalDeleted += stat.getSuccessDeleteFiles().size();
    }
    return new HoodieRollbackMetadata(startRollbackTime, durationInMs.orElseGet(() -> -1L),
        totalDeleted, commits, partitionMetadataBuilder.build());
  }

  public static HoodieSavepointMetadata convertSavepointMetadata(String user, String comment,
      Map<String, List<String>> latestFiles) {
    ImmutableMap.Builder<String, HoodieSavepointPartitionMetadata> partitionMetadataBuilder =
        ImmutableMap.builder();
    for (Map.Entry<String, List<String>> stat : latestFiles.entrySet()) {
      HoodieSavepointPartitionMetadata metadata =
          new HoodieSavepointPartitionMetadata(stat.getKey(), stat.getValue());
      partitionMetadataBuilder.put(stat.getKey(), metadata);
    }
    return new HoodieSavepointMetadata(user, System.currentTimeMillis(), comment,
        partitionMetadataBuilder.build());
  }

  public static Optional<byte[]> serializeCompactionPlan(HoodieCompactionPlan compactionWorkload)
      throws IOException {
    return serializeAvroMetadata(compactionWorkload, HoodieCompactionPlan.class);
  }

  public static Optional<byte[]> serializeCleanMetadata(HoodieCleanMetadata metadata)
      throws IOException {
    return serializeAvroMetadata(metadata, HoodieCleanMetadata.class);
  }

  public static Optional<byte[]> serializeSavepointMetadata(HoodieSavepointMetadata metadata)
      throws IOException {
    return serializeAvroMetadata(metadata, HoodieSavepointMetadata.class);
  }

  public static Optional<byte[]> serializeRollbackMetadata(
      HoodieRollbackMetadata rollbackMetadata) throws IOException {
    return serializeAvroMetadata(rollbackMetadata, HoodieRollbackMetadata.class);
  }

  public static <T extends SpecificRecordBase> Optional<byte[]> serializeAvroMetadata(T metadata,
      Class<T> clazz) throws IOException {
    DatumWriter<T> datumWriter = new SpecificDatumWriter<>(clazz);
    DataFileWriter<T> fileWriter = new DataFileWriter<>(datumWriter);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    fileWriter.create(metadata.getSchema(), baos);
    fileWriter.append(metadata);
    fileWriter.flush();
    return Optional.of(baos.toByteArray());
  }

  public static HoodieCompactionPlan deserializeCompactionPlan(byte[] bytes)
      throws IOException {
    return deserializeAvroMetadata(bytes, HoodieCompactionPlan.class);
  }

  public static HoodieCleanMetadata deserializeHoodieCleanMetadata(byte[] bytes)
      throws IOException {
    return deserializeAvroMetadata(bytes, HoodieCleanMetadata.class);
  }

  public static HoodieSavepointMetadata deserializeHoodieSavepointMetadata(byte[] bytes)
      throws IOException {
    return deserializeAvroMetadata(bytes, HoodieSavepointMetadata.class);
  }

  public static <T extends SpecificRecordBase> T deserializeAvroMetadata(byte[] bytes,
      Class<T> clazz) throws IOException {
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
    FileReader<T> fileReader =
        DataFileReader.openReader(new SeekableByteArrayInput(bytes), reader);
    Preconditions
        .checkArgument(fileReader.hasNext(), "Could not deserialize metadata of type " + clazz);
    return fileReader.next();
  }
}

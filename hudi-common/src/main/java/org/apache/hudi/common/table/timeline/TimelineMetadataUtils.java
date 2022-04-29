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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimelineMetadataUtils {

  private static final Integer DEFAULT_VERSION = 1;

  public static HoodieRestoreMetadata convertRestoreMetadata(String startRestoreTime,
                                                             long durationInMs,
                                                             List<HoodieInstant> instants,
                                                             Map<String, List<HoodieRollbackMetadata>> instantToRollbackMetadata) {
    return new HoodieRestoreMetadata(startRestoreTime, durationInMs,
        instants.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()),
        Collections.unmodifiableMap(instantToRollbackMetadata), DEFAULT_VERSION,
        instants.stream().map(instant -> new HoodieInstantInfo(instant.getTimestamp(), instant.getAction())).collect(Collectors.toList()));
  }

  public static HoodieRollbackMetadata convertRollbackMetadata(String startRollbackTime, Option<Long> durationInMs,
      List<HoodieInstant> instants, List<HoodieRollbackStat> rollbackStats) {
    Map<String, HoodieRollbackPartitionMetadata> partitionMetadataBuilder = new HashMap<>();
    int totalDeleted = 0;
    for (HoodieRollbackStat stat : rollbackStats) {
      Map<String, Long> rollbackLogFiles = stat.getCommandBlocksCount().keySet().stream()
          .collect(Collectors.toMap(f -> f.getPath().toString(), FileStatus::getLen));
      HoodieRollbackPartitionMetadata metadata = new HoodieRollbackPartitionMetadata(stat.getPartitionPath(),
          stat.getSuccessDeleteFiles(), stat.getFailedDeleteFiles(), rollbackLogFiles);
      partitionMetadataBuilder.put(stat.getPartitionPath(), metadata);
      totalDeleted += stat.getSuccessDeleteFiles().size();
    }

    return new HoodieRollbackMetadata(startRollbackTime, durationInMs.orElseGet(() -> -1L), totalDeleted,
      instants.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()),
      Collections.unmodifiableMap(partitionMetadataBuilder), DEFAULT_VERSION,
      instants.stream().map(instant -> new HoodieInstantInfo(instant.getTimestamp(), instant.getAction())).collect(Collectors.toList()));
  }

  public static HoodieSavepointMetadata convertSavepointMetadata(String user, String comment,
      Map<String, List<String>> latestFiles) {
    Map<String, HoodieSavepointPartitionMetadata> partitionMetadataBuilder = new HashMap<>();
    for (Map.Entry<String, List<String>> stat : latestFiles.entrySet()) {
      HoodieSavepointPartitionMetadata metadata = new HoodieSavepointPartitionMetadata(stat.getKey(), stat.getValue());
      partitionMetadataBuilder.put(stat.getKey(), metadata);
    }
    return new HoodieSavepointMetadata(user, System.currentTimeMillis(), comment,
      Collections.unmodifiableMap(partitionMetadataBuilder), DEFAULT_VERSION);
  }

  public static Option<byte[]> serializeCompactionPlan(HoodieCompactionPlan compactionWorkload) throws IOException {
    return serializeAvroMetadata(compactionWorkload, HoodieCompactionPlan.class);
  }

  public static Option<byte[]> serializeCleanerPlan(HoodieCleanerPlan cleanPlan) throws IOException {
    return serializeAvroMetadata(cleanPlan, HoodieCleanerPlan.class);
  }

  public static Option<byte[]> serializeRollbackPlan(HoodieRollbackPlan rollbackPlan) throws IOException {
    return serializeAvroMetadata(rollbackPlan, HoodieRollbackPlan.class);
  }

  public static Option<byte[]> serializeRestorePlan(HoodieRestorePlan restorePlan) throws IOException {
    return serializeAvroMetadata(restorePlan, HoodieRestorePlan.class);
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

  public static Option<byte[]> serializeRequestedReplaceMetadata(HoodieRequestedReplaceMetadata clusteringPlan) throws IOException {
    return serializeAvroMetadata(clusteringPlan, HoodieRequestedReplaceMetadata.class);
  }

  public static Option<byte[]> serializeIndexPlan(HoodieIndexPlan indexPlan) throws IOException {
    return serializeAvroMetadata(indexPlan, HoodieIndexPlan.class);
  }

  public static Option<byte[]> serializeIndexCommitMetadata(HoodieIndexCommitMetadata indexCommitMetadata) throws IOException {
    return serializeAvroMetadata(indexCommitMetadata, HoodieIndexCommitMetadata.class);
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

  public static HoodieRollbackMetadata deserializeHoodieRollbackMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieRollbackMetadata.class);
  }

  public static HoodieRestoreMetadata deserializeHoodieRestoreMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieRestoreMetadata.class);
  }

  public static HoodieSavepointMetadata deserializeHoodieSavepointMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieSavepointMetadata.class);
  }

  public static HoodieRequestedReplaceMetadata deserializeRequestedReplaceMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieRequestedReplaceMetadata.class);
  }

  public static HoodieReplaceCommitMetadata deserializeHoodieReplaceMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieReplaceCommitMetadata.class);
  }

  public static HoodieIndexPlan deserializeIndexPlan(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieIndexPlan.class);
  }

  public static HoodieIndexCommitMetadata deserializeIndexCommitMetadata(byte[] bytes) throws IOException {
    return deserializeAvroMetadata(bytes, HoodieIndexCommitMetadata.class);
  }

  public static <T extends SpecificRecordBase> T deserializeAvroMetadata(byte[] bytes, Class<T> clazz)
      throws IOException {
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
    FileReader<T> fileReader = DataFileReader.openReader(new SeekableByteArrayInput(bytes), reader);
    ValidationUtils.checkArgument(fileReader.hasNext(), "Could not deserialize metadata of type " + clazz);
    return fileReader.next();
  }

  public static <T extends SpecificRecordBase> T deserializeAvroRecordMetadata(byte[] bytes, Schema schema)
      throws IOException {
    return  deserializeAvroRecordMetadata(HoodieAvroUtils.bytesToAvro(bytes, schema), schema);
  }

  public static <T extends SpecificRecordBase> T deserializeAvroRecordMetadata(Object object, Schema schema) {
    return  (T) SpecificData.get().deepCopy(schema, object);
  }
}

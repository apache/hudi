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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto.UserMetadataItem;
import org.apache.orc.Reader;
import org.apache.orc.Reader.Options;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

/**
 * Utility functions for ORC files.
 */
public class OrcUtils extends BaseFileUtils {

  /**
   * Fetch {@link HoodieKey}s from the given ORC file.
   *
   * @param filePath      The ORC file path.
   * @param configuration configuration to build fs object
   * @return {@link List} of {@link HoodieKey}s fetched from the ORC file
   */
  @Override
  public List<HoodieKey> fetchRecordKeyPartitionPath(Configuration configuration, Path filePath) {
    List<HoodieKey> hoodieKeys = new ArrayList<>();
    try {
      if (!filePath.getFileSystem(configuration).exists(filePath)) {
        return new ArrayList<>();
      }

      Configuration conf = new Configuration(configuration);
      conf.addResource(FSUtils.getFs(filePath.toString(), conf).getConf());
      Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));

      Schema readSchema = HoodieAvroUtils.getRecordKeyPartitionPathSchema();
      TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(readSchema);
      List<String> fieldNames = orcSchema.getFieldNames();
      VectorizedRowBatch batch = orcSchema.createRowBatch();
      RecordReader recordReader = reader.rows(new Options(conf).schema(orcSchema));

      // column indices for the RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD fields
      int keyCol = -1;
      int partitionCol = -1;
      for (int i = 0; i < fieldNames.size(); i++) {
        if (fieldNames.get(i).equals(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
          keyCol = i;
        }
        if (fieldNames.get(i).equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD)) {
          partitionCol = i;
        }
      }
      if (keyCol == -1 || partitionCol == -1) {
        throw new HoodieException(String.format("Couldn't find row keys or partition path in %s.", filePath));
      }
      while (recordReader.nextBatch(batch)) {
        BytesColumnVector rowKeys = (BytesColumnVector) batch.cols[keyCol];
        BytesColumnVector partitionPaths = (BytesColumnVector) batch.cols[partitionCol];
        for (int i = 0; i < batch.size; i++) {
          String rowKey = rowKeys.toString(i);
          String partitionPath = partitionPaths.toString(i);
          hoodieKeys.add(new HoodieKey(rowKey, partitionPath));
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from ORC file:" + filePath, e);
    }
    return hoodieKeys;
  }

  @Override
  public List<HoodieKey> fetchRecordKeyPartitionPath(Configuration configuration, Path filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new HoodieIOException("UnsupportedOperation : Disabling meta fields not yet supported for Orc");
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath) {
    Schema avroSchema;
    try {
      Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(configuration));
      avroSchema = AvroOrcUtils.createAvroSchema(reader.getSchema());
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read Avro records from an ORC file:" + filePath, io);
    }
    return readAvroRecords(configuration, filePath, avroSchema);
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath, Schema avroSchema) {
    List<GenericRecord> records = new ArrayList<>();
    try {
      Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(configuration));
      TypeDescription orcSchema = reader.getSchema();
      RecordReader recordReader = reader.rows(new Options(configuration).schema(orcSchema));
      OrcReaderIterator<GenericRecord> iterator = new OrcReaderIterator<>(recordReader, avroSchema, orcSchema);
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        records.add(record);
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to create an ORC reader for ORC file:" + filePath, io);
    }
    return records;
  }

  /**
   * Read the rowKey list matching the given filter, from the given ORC file. If the filter is empty, then this will
   * return all the rowkeys.
   *
   * @param conf configuration to build fs object.
   * @param filePath      The ORC file path.
   * @param filter        record keys filter
   * @return Set Set of row keys matching candidateRecordKeys
   */
  @Override
  public Set<String> filterRowKeys(Configuration conf, Path filePath, Set<String> filter)
      throws HoodieIOException {
    try {
      Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));
      Set<String> filteredRowKeys = new HashSet<>();
      TypeDescription schema = reader.getSchema();
      List<String> fieldNames = schema.getFieldNames();
      VectorizedRowBatch batch = schema.createRowBatch();
      RecordReader recordReader = reader.rows(new Options(conf).schema(schema));

      // column index for the RECORD_KEY_METADATA_FIELD field
      int colIndex = -1;
      for (int i = 0; i < fieldNames.size(); i++) {
        if (fieldNames.get(i).equals(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
          colIndex = i;
          break;
        }
      }
      if (colIndex == -1) {
        throw new HoodieException(String.format("Couldn't find row keys in %s.", filePath));
      }
      while (recordReader.nextBatch(batch)) {
        BytesColumnVector rowKeys = (BytesColumnVector) batch.cols[colIndex];
        for (int i = 0; i < batch.size; i++) {
          String rowKey = rowKeys.toString(i);
          if (filter.isEmpty() || filter.contains(rowKey)) {
            filteredRowKeys.add(rowKey);
          }
        }
      }
      return filteredRowKeys;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read row keys for ORC file:" + filePath, io);
    }
  }

  @Override
  public Map<String, String> readFooter(Configuration conf, boolean required,
                                        Path orcFilePath, String... footerNames) {
    try {
      Reader reader = OrcFile.createReader(orcFilePath, OrcFile.readerOptions(conf));
      Map<String, String> footerVals = new HashMap<>();
      List<UserMetadataItem> metadataItemList = reader.getFileTail().getFooter().getMetadataList();
      Map<String, String> metadata = metadataItemList.stream().collect(Collectors.toMap(
          UserMetadataItem::getName,
          metadataItem -> metadataItem.getValue().toStringUtf8()));
      for (String footerName : footerNames) {
        if (metadata.containsKey(footerName)) {
          footerVals.put(footerName, metadata.get(footerName));
        } else if (required) {
          throw new MetadataNotFoundException(
              "Could not find index in ORC footer. Looked for key " + footerName + " in " + orcFilePath);
        }
      }
      return footerVals;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read footer for ORC file:" + orcFilePath, io);
    }
  }

  @Override
  public Schema readAvroSchema(Configuration conf, Path orcFilePath) {
    try {
      Reader reader = OrcFile.createReader(orcFilePath, OrcFile.readerOptions(conf));
      TypeDescription orcSchema = reader.getSchema();
      return AvroOrcUtils.createAvroSchema(orcSchema);
    } catch (IOException io) {
      throw new HoodieIOException("Unable to get Avro schema for ORC file:" + orcFilePath, io);
    }
  }

  @Override
  public long getRowCount(Configuration conf, Path orcFilePath) {
    try {
      Reader reader = OrcFile.createReader(orcFilePath, OrcFile.readerOptions(conf));
      return reader.getNumberOfRows();
    } catch (IOException io) {
      throw new HoodieIOException("Unable to get row count for ORC file:" + orcFilePath, io);
    }
  }
}

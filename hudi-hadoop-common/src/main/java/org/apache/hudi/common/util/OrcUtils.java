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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.hadoop.OrcReaderIterator;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto.UserMetadataItem;
import org.apache.orc.Reader;
import org.apache.orc.Reader.Options;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.BinaryUtil.toBytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopPath;

/**
 * Utility functions for ORC files.
 */
public class OrcUtils extends FileFormatUtils {

  /**
   * Provides a closable iterator for reading the given ORC file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The ORC file path
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the ORC file
   */
  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath) {
    try {
      Configuration conf = storage.getConf().unwrapCopyAs(Configuration.class);
      conf.addResource(HadoopFSUtils.getFs(filePath.toString(), conf).getConf());
      Reader reader = OrcFile.createReader(convertToHadoopPath(filePath), OrcFile.readerOptions(conf));

      Schema readSchema = HoodieAvroUtils.getRecordKeyPartitionPathSchema();
      TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(readSchema);
      RecordReader recordReader = reader.rows(new Options(conf).schema(orcSchema));
      List<String> fieldNames = orcSchema.getFieldNames();

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
      return new OrcReaderIterator<>(recordReader, readSchema, orcSchema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to open reader from ORC file:" + filePath, e);
    }
  }

  /**
   * Fetch {@link HoodieKey}s from the given ORC file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The ORC file path.
   * @return {@link List} of {@link HoodieKey}s fetched from the ORC file
   */
  @Override
  public List<HoodieKey> fetchHoodieKeys(HoodieStorage storage, StoragePath filePath) {
    try {
      if (!storage.exists(filePath)) {
        return Collections.emptyList();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from ORC file:" + filePath, e);
    }
    List<HoodieKey> hoodieKeys = new ArrayList<>();
    try (ClosableIterator<HoodieKey> iterator = getHoodieKeyIterator(storage, filePath, Option.empty()))  {
      iterator.forEachRemaining(hoodieKeys::add);
    }
    return hoodieKeys;
  }

  @Override
  public List<HoodieKey> fetchHoodieKeys(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new UnsupportedOperationException("Custom key generator is not supported yet");
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new UnsupportedOperationException("Custom key generator is not supported yet");
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath) {
    Schema avroSchema;
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      avroSchema = AvroOrcUtils.createAvroSchema(reader.getSchema());
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read Avro records from an ORC file:" + filePath, io);
    }
    return readAvroRecords(storage, filePath, avroSchema);
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, Schema avroSchema) {
    List<GenericRecord> records = new ArrayList<>();
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      TypeDescription orcSchema = reader.getSchema();
      try (RecordReader recordReader = reader.rows(
          new Options(storage.getConf().unwrapAs(Configuration.class)).schema(orcSchema))) {
        OrcReaderIterator<GenericRecord> iterator = new OrcReaderIterator<>(recordReader, avroSchema, orcSchema);
        while (iterator.hasNext()) {
          GenericRecord record = iterator.next();
          records.add(record);
        }
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
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The ORC file path.
   * @param filter   record keys filter
   * @return Set of row keys matching candidateRecordKeys
   */
  @Override
  public Set<String> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter)
      throws HoodieIOException {
    try (Reader reader = OrcFile.createReader(new Path(filePath.toUri()), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)));) {
      TypeDescription schema = reader.getSchema();
      try (RecordReader recordReader = reader.rows(new Options(storage.getConf().unwrapAs(Configuration.class)).schema(schema))) {
        Set<String> filteredRowKeys = new HashSet<>();
        List<String> fieldNames = schema.getFieldNames();
        VectorizedRowBatch batch = schema.createRowBatch();

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
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read row keys for ORC file:" + filePath, io);
    }
  }

  @Override
  public Map<String, String> readFooter(HoodieStorage storage, boolean required,
                                        StoragePath filePath, String... footerNames) {
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
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
              "Could not find index in ORC footer. Looked for key " + footerName + " in " + filePath);
        }
      }
      return footerVals;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read footer for ORC file:" + filePath, io);
    }
  }

  @Override
  public Schema readAvroSchema(HoodieStorage storage, StoragePath filePath) {
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      if (reader.hasMetadataValue("orc.avro.schema")) {
        ByteBuffer metadataValue = reader.getMetadataValue("orc.avro.schema");
        byte[] bytes = toBytes(metadataValue);
        return new Schema.Parser().parse(new String(bytes));
      } else {
        TypeDescription orcSchema = reader.getSchema();
        return AvroOrcUtils.createAvroSchema(orcSchema);
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to get Avro schema for ORC file:" + filePath, io);
    }
  }

  @Override
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage, StoragePath filePath, List<String> columnList) {
    throw new UnsupportedOperationException(
        "Reading column statistics from metadata is not supported for ORC format yet");
  }

  @Override
  public HoodieFileFormat getFormat() {
    return HoodieFileFormat.ORC;
  }

  @Override
  public long getRowCount(HoodieStorage storage, StoragePath filePath) {
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      return reader.getNumberOfRows();
    } catch (IOException io) {
      throw new HoodieIOException("Unable to get row count for ORC file:" + filePath, io);
    }
  }

  @Override
  public void writeMetaFile(HoodieStorage storage, StoragePath filePath, Properties props) throws IOException {
    // Since we are only interested in saving metadata to the footer, the schema, blocksizes and other
    // parameters are not important.
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(storage.getConf().unwrapAs(Configuration.class))
        .fileSystem((FileSystem) storage.getFileSystem())
        .setSchema(AvroOrcUtils.createOrcSchema(schema));
    try (Writer writer = OrcFile.createWriter(convertToHadoopPath(filePath), writerOptions)) {
      for (String key : props.stringPropertyNames()) {
        writer.addUserMetadata(key, ByteBuffer.wrap(getUTF8Bytes(props.getProperty(key))));
      }
    }
  }

  @Override
  public byte[] serializeRecordsToLogBlock(HoodieStorage storage,
                                           List<HoodieRecord> records,
                                           Schema writerSchema,
                                           Schema readerSchema, String keyFieldName,
                                           Map<String, String> paramsMap) throws IOException {
    throw new UnsupportedOperationException("Hudi log blocks do not support ORC format yet");
  }
}
